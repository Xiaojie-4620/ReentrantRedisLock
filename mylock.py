import asyncio
from typing import Optional, Union
from redis.asyncio import Redis
import redis.asyncio as redis
from redis.exceptions import LockError, LockNotOwnedError
from contextlib import asynccontextmanager
import uuid
import logging

logger = logging.getLogger(__name__)

"""
redis.asyncio.lock 中的 Lock 的 local 默认是线程级隔离。通常多个 asyncio Task 都运行在同一线程，因此会看到相同 token。
应把“可重入所有者”定义为当前 Task
"""

class ReentrantRedisLock:
    lua_reentrant_acquire = None
    lua_reentrant_release = None
    lua_reentrant_extend = None

    
    LUA_REENTRANT_ACQUIRE_SCRIPT = (
    """
    --[[
        0：获取失败；
        1：首次获取；
        2：重入成功。
    ]]
    local key = KEYS[1]; --- 锁的 key
    local token = ARGV[1]; --- 加锁的任务的唯一标识
    local releaseTime = tonumber(ARGV[2]); --- 锁的自动释放时间
    --- 判断锁是否存在
    --- 1. 锁不存在
    if (redis.call('EXISTS', key) == 0) then
        --- lock not exist
        redis.call('HSET', key, token, '1');
        --- set TTL
        redis.call('PEXPIRE', key, releaseTime);
        return 1; --- 返回结果
    end;
    --- 2. 锁存在, 先判断是不是自己的锁
    if (redis.call('HEXISTS', key, token) == 1) then
        --- 是自己的锁，获取锁，重入次数 +1
        redis.call('HINCRBY', key, token, '1');
        --- set lock's TTL
        redis.call('PEXPIRE', key, releaseTime);
        return 2; 
    end;
    return 0 --- 锁不是自己的，直接返回0
    """
    )

    LUA_REENTRANT_RELEASE_SCRIPT = (
    """
    --[[
        -1: Redis 中已经不属于当前持有者；
         0: 完全释放；
        count: 剩余重入次数。
    ]]
    local key = KEYS[1];
    local token = ARGV[1];
    local releaseTime = tonumber(ARGV[2]);

    if (redis.call('HEXISTS', key, token) == 0) then
        return -1; --- 不是我的锁直接返回 空
    end;

    --- 是我的锁
    local count = redis.call('HINCRBY', key, token, -1);
    --- 判断是否重入次数为0，若为0则直接删除该key
    if (count > 0) then
        redis.call('PEXPIRE', key, releaseTime);
        return count -- 大于0, 说明还不能删除这个锁，重置TTL后返回
    else
        -- 没有人用这个锁了，直接删除
        redis.call('DEL', key);
        return 0;
    end;
    """
    )

    LUA_REENTRANT_EXTEND_SCRIPT = (
    """
    local key = KEYS[1];
    local token = ARGV[1];
    local releaseTime = tonumber(ARGV[2]);

    if (redis.call('HEXISTS', key, token) == 0) then
        return 0; --- 不是我的锁，不能续期
    end;

    --- 是我的锁，进行续期操作
    redis.call('PEXPIRE', key, releaseTime);
    return 1; --- 续期成功
        
    """
    )

    def __init__(
        self,
        redis: Redis,
        name: str,
        timeout: float = 10.0,
        blocking: bool = True,
        blocking_timeout: Optional[float] = None,
        sleep: float = 0.1,
        auto_renew: bool = True,
        renew_interval: Optional[float] = None,
        raise_on_release_error: bool = True
    ):
        """
        Create a new ReentrantRedisLock instance named ``name`` using the Redis client
        supplied by ``redis``.


        """
        if not name:
            raise ValueError("name cannot be empty")
        if timeout <= 0:
            raise ValueError("timeout must be greater than 0")
        if sleep <= 0:
            raise ValueError("sleep must be greater than 0")
        if blocking_timeout is not None and blocking_timeout < 0:
            raise ValueError("blocking_timeout cannot be negative")
        self.redis = redis
        self.name = name
        self.timeout = timeout
        self.sleep = sleep
        self.blocking = blocking
        self.blocking_timeout = blocking_timeout
        self.auto_renew = auto_renew
        actual_renew_interval = (
            timeout / 3.0 
            if renew_interval is None 
            else renew_interval
        ) # 看门狗的时间
        if not 0 < actual_renew_interval < timeout:
            raise ValueError(
                "renew_interval must be greater than 0 and less than timeout"
            )
        self.renew_interval = actual_renew_interval
        self.raise_on_release_error = raise_on_release_error

        # 本地协程所有权
        self._owner_task: asyncio.Task | None = None # 判断是不是同一个 asyncio Task 重入
        self._token: str | None = None # 在 Redis 中识别是不是同一个客户端持有者
        self._depth = 0 # 记录本地重入层数
        
        # 看门狗状态
        self._watchdog_task: Optional[asyncio.Task] = None
        self._lock_lost = asyncio.Event()
        self._register_scripts()

    async def locked(self) -> bool:
        """
        Redis 中是否持有该锁，不区分持有者
        """
        return bool(await self.redis.exists(self.name))

    async def owned(self) -> bool:
        """
        当前的 asyncio task 是否任然持有该锁
        """
        current = asyncio.current_task()

        if (current is not self._owner_task or 
            self._token is None or
            self._depth <= 0):
            return False

        return bool(await self.redis.hexists(self.name, self._token))

    @property
    def lost(self) -> bool:
        """ ensure lock is lost by local info"""
        return self._lock_lost.is_set()

    async def wait_until_lost(self) -> None:
        """ wait watchdog ensure the lock is lost"""
        await self._lock_lost.wait()

    def _register_scripts(self) -> None:
        client = self.redis
        self.lua_reentrant_acquire = client.register_script(self.LUA_REENTRANT_ACQUIRE_SCRIPT)            
        self.lua_reentrant_release = client.register_script(self.LUA_REENTRANT_RELEASE_SCRIPT)
        self.lua_reentrant_extend = client.register_script(self.LUA_REENTRANT_EXTEND_SCRIPT)

    async def acquire(
        self,
        *, 
        blocking: Optional[bool] = None,
        blocking_timeout: Optional[float] = None,
    ) -> bool:
        """
        获取 Redis 可重入分布式锁。

        重入规则：
        1. 同一个 ReentrantRedisLock 实例；
        2. 同一个 asyncio Task；
        3. Redis 中仍然保存相同 token。

        首次获取时生成随机 token；同一 Task 重入时复用该 token，
        并在 Redis Hash 中增加重入计数。

        Returns:
            bool: 锁是否获取成功。
        """
        current = asyncio.current_task()
        if current is None:
            raise RuntimeError(
                "ReentrantRedisLock must be used inside an asyncio Task"
            )
        actual_blocking = (self.blocking if blocking is None else blocking)
        actual_blocking_timeout = (
            self.blocking_timeout if blocking_timeout is None else blocking_timeout
        )
        is_reentry = ( current is self._owner_task and self._token is not None and self._depth > 0)
        token = (
                self._token if is_reentry 
                else uuid.uuid4().hex
            )

        ttl = max(1, int(self.timeout * 1000)) # 默认10s, 使用毫秒机制
        loop = asyncio.get_running_loop()

        deadline = (
            loop.time() + actual_blocking_timeout 
            if actual_blocking_timeout is not None
            else None
        )

        # 阻塞模式(非阻塞模式融合到一起)
        while True:
            result = int(await self.lua_reentrant_acquire(
                keys=[self.name],
                args=[token, ttl],
                client=self.redis,
            )) # type:ignore

            if result != 0:
                if is_reentry:
                    if result == 1:
                        # 本地认为是重入，但 Redis 中原锁已经过期。
                        # 获取脚本刚刚重新创建了锁，需要立即撤销。
                        await self.lua_reentrant_release(
                            keys=[self.name],
                            args=[token, ttl],
                            client=self.redis
                        )# type:ignore
                        await self._mark_lost(token)

                        raise LockNotOwnedError(
                            "The lock expired before reentrant acquisition",
                            lock_name=str(self.name),
                        )
                    # result == 2，正常重入
                    self._depth += 1
                    return True
                if result != 1:
                    # 新 UUID 理论上不应该命中旧 token
                    raise LockError(
                        "Unexpected token collision",
                        lock_name=str(self.name),
                    )

                # 首次获取成功
                self._owner_task = current
                self._token = token
                self._depth = 1
                self._lock_lost.clear()
                self._start_watchdog(token) # type:ignore
                return True

            if not actual_blocking:
                return False

            next_try_again = loop.time() + self.sleep
            if deadline is not None and next_try_again > deadline:
                return False
            await asyncio.sleep(self.sleep)

    async def _mark_lost(self, token: str) -> None:
        """
        清理已经失效的本地锁状态
            获取、释放和看门狗都可能发现锁已经过期，应该统一清理状态，避免不同方法分别修改字段。
        """
        if self._token != token:
            return

        self._owner_task = None
        self._token = None
        self._depth = 0
        self._lock_lost.set()

        await self._stop_watchdog()


    async def release(self) -> None:
        """
        释放重入锁机制，调用 Lua 脚本实现原子操作来释放可重入锁
        释放机制：
            如果当前锁的计数大于1 说明处于重入状态，释放时检查 key 和 token 然后将重入计数 -1
            如果当前锁的计数等于1 说明未处于重入状态，检查 key 和 token 直接删除锁
        
        """
        current = asyncio.current_task()
        if current is not self._owner_task or self._token is None:
            raise LockError(
                "Cannot release a lock owned by another task",
                lock_name=str(self.name),
            )

        token = self._token
        ttl = max(1, int(self.timeout * 1000)) # 默认10s, 使用毫秒机制

        remaining = int(
            await self.lua_reentrant_release(
                keys = [self.name], 
                args = [token, ttl], 
                client = self.redis) # type:ignore
        )

        if remaining == -1: 
            await self._mark_lost(token)

            raise LockNotOwnedError(
                "Cannot release a lock that's no longer owned",
                lock_name=str(self.name),
            )

        # 以 Redis 返回的剩余次数为准
        self._depth = remaining

        if remaining == 0:
            self._owner_task = None
            self._token = None
            await self._stop_watchdog()

    def _start_watchdog(self, token: str) -> None:
        """
        获取锁，立马启动看门狗机制
        看门狗是独立 Task，必须捕获确定的 token，不能从“当前 Task 所有权”推导 token。
        """
        if not self.auto_renew:
            return

        if self._watchdog_task is not None and not self._watchdog_task.done():
            return

        self._watchdog_task = asyncio.create_task(
            self._watchdog(token),
            name = f"redis-lock-watchdog:{self.name}"
        )

    async def _watchdog(self, token: str) -> None:
        """
        看门狗机制，后台任务，只要当前锁的还剩 1/3 且锁未释放，就自动续期
        """
        loop = asyncio.get_running_loop()
        lease_deadline = loop.time() + self.timeout
        next_delay = self.renew_interval
        try:
            while True:
                # 先睡眠 锁过期时间的 2/3
                await asyncio.sleep(next_delay)
                # 本地都没token了，说明释放了，退出
                if self._token != token or self._depth <= 0:
                    return
                renew_start_at = loop.time()
                try:
                   renewed =  await self._extend_token(token)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception(
                        "Failed to renew Redis Lock %s",
                        self.name,
                    )
                    remaining = lease_deadline - loop.time()
                    # 已经超过最近一次确认续期后的有效期
                    if remaining < 0:
                        await self._mark_lost(token)
                        return

                    next_delay = min(self.sleep, max(remaining / 2, 0.01))
                    continue

                if not renewed:
                    await self._mark_lost(token)
                    return
                # 从发起续期操作的时间计算，避免忽略网络响应延迟
                lease_deadline = renew_start_at + self.timeout
                next_delay = self.renew_interval

        except asyncio.CancelledError:
            raise

        finally:
            if self._watchdog_task is asyncio.current_task():
                self._watchdog_task = None

    async def _extend_token(self, token: str) -> bool:
        """
        内部看门狗使用的续期方法
        """
        ttl = max(1, int(self.timeout * 1000)) 

        resp = await self.lua_reentrant_extend(
            keys = [self.name],
            args = [token, ttl],
            client = self.redis
        ) # type: ignore

        return bool(resp)

    async def renew(self) -> bool:
        """
        调用 Lua 脚本，原子实现锁的续期
        """
        current = asyncio.current_task()
        if current is not self._owner_task or self._token is None:
            raise LockError(
                "Cannot renew a lock owned by another task",
                lock_name=str(self.name),
            )

        token = self._token
        renewed = await self._extend_token(token)

        if not renewed:
            await self._mark_lost(token)

            raise LockNotOwnedError(
                "Cannot renew a lock that's no longer owned",
                lock_name=str(self.name),
            )

        return True

    async def _stop_watchdog(self) -> None:
        """
        锁释放后，也要终止看门狗的后台任务
        """
        task, self._watchdog_task = self._watchdog_task, None
        current = asyncio.current_task()
        # 关键：不能在 watchog 自己里 await 自己，会死锁
        if task and task is not current and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    # 上下文管理器
    async def __aenter__(self) -> "ReentrantRedisLock":
        acquired = await self.acquire()
        if not acquired:
            raise LockError(
                "Unable to acquire lock within the time specified",
                lock_name=str(self.name),
            )
        return self

    async def __aexit__(self, exc_type, exc, tb):
        # 释放异常会链到原有异常上
        try:
            await self.release()
        except LockError:
            if self.raise_on_release_error:
                raise

            logger.warning(
                "Lock %s was already lost when leaving context",
                self.name,
                exc_info=True,
            )
        return False
