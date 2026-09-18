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
An asynchronous, reentrant Redis lock with automatic lease renewal.

Unlike thread-local ownership, this implementation defines the lock owner as
the current ``asyncio.Task``. This distinction is important because many
asyncio tasks normally run in the same operating-system thread.
"""

class ReentrantRedisLock:
    """A task-reentrant distributed lock backed by Redis.

    Redis stores the lock as a hash whose field is a random owner token and
    whose value is the reentrancy count. Lua scripts make acquire, release,
    and renewal operations atomic. A background watchdog periodically renews
    the key's lease while the owning task is still using the lock.

    Reentrancy is limited to the same ``ReentrantRedisLock`` instance and the
    same ``asyncio.Task``. Another task must compete for the Redis lock even if
    it runs in the same thread.
    """
    
    lua_reentrant_acquire = None
    lua_reentrant_release = None
    lua_reentrant_extend = None

    
    LUA_REENTRANT_ACQUIRE_SCRIPT = (
    """
    --[[
        0: acquisition failed because another owner holds the lock;
        1: the lock was acquired for the first time;
        2: the current owner re-entered the lock.
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
       -1: the token no longer owns the Redis lock;
         0: the lock was fully released;
        count: the remaining reentrancy depth.
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
        """Create a reentrant Redis lock.

        Args:
            redis: An initialized ``redis.asyncio.Redis`` client.
            name: Redis key used to identify the protected resource.
            timeout: Lease duration in seconds.
            blocking: Whether acquisition retries while the lock is occupied.
            blocking_timeout: Maximum blocking time in seconds, or ``None``
                to wait indefinitely.
            sleep: Delay between acquisition attempts, in seconds.
            auto_renew: Whether to start the background watchdog.
            renew_interval: Delay between normal renewal attempts. When
                omitted, one third of ``timeout`` is used.
            raise_on_release_error: Whether a context-manager exit should
                propagate an ownership/release error.
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
        ) # Normal delay between watchdog renewal attempts.
        if not 0 < actual_renew_interval < timeout:
            raise ValueError(
                "renew_interval must be greater than 0 and less than timeout"
            )
        self.renew_interval = actual_renew_interval
        self.raise_on_release_error = raise_on_release_error

        # Local ownership state. Redis remains the authoritative shared state,
        # while these fields determine whether the current task may re-enter.
        self._owner_task: asyncio.Task | None = None
        self._token: str | None = None
        self._depth = 0
        
        # Watchdog state. The event is set after ownership is known to be lost.
        self._watchdog_task: Optional[asyncio.Task] = None
        self._lock_lost = asyncio.Event()
        self._register_scripts()

    async def locked(self) -> bool:
        """
        Return whether the Redis key is currently locked by any owner.
        """
        return bool(await self.redis.exists(self.name))

    async def owned(self) -> bool:
       """Return whether the current task still owns the Redis lock."""
        current = asyncio.current_task()

        if (current is not self._owner_task or 
            self._token is None or
            self._depth <= 0):
            return False

        return bool(await self.redis.hexists(self.name, self._token))

    @property
    def lost(self) -> bool:
        """Return whether this instance has detected loss of ownership."""
        return self._lock_lost.is_set()

    async def wait_until_lost(self) -> None:
        """Wait until the watchdog or an operation detects a lost lock."""
        await self._lock_lost.wait()

    def _register_scripts(self) -> None:
        """Register the atomic Lua operations with the Redis client."""
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
        """Acquire the Redis lock, re-entering it for the owning task.

        A new UUID token is generated for a normal contender. The existing
        token is reused only when the same task re-enters this lock instance.

        Args:
            blocking: Per-call override for blocking behavior.
            blocking_timeout: Per-call maximum wait time in seconds.

        Returns:
            ``True`` when acquired; ``False`` for a non-blocking failure or
            when the blocking deadline is reached.

        Raises:
            LockNotOwnedError: Local state indicates re-entry, but the
                previous Redis lease has already disappeared.
            LockError: Redis reports an unexpected token collision.
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
        # Redis receives lease durations in milliseconds.
        ttl = max(1, int(self.timeout * 1000))
        loop = asyncio.get_running_loop()

        deadline = (
            loop.time() + actual_blocking_timeout 
            if actual_blocking_timeout is not None
            else None
        )

        # The same loop handles blocking and non-blocking acquisition.
        while True:
            result = int(await self.lua_reentrant_acquire(
                keys=[self.name],
                args=[token, ttl],
                client=self.redis,
            )) # type:ignore

            if result != 0:
                if is_reentry:
                    if result == 1:
                        # Local state claimed this was a re-entry, but Redis
                        # created a new lock. The previous lease was therefore
                        # lost. Roll back the newly created lock before raising.
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
                     # Result 2 is a valid re-entry by the same token.
                    self._depth += 1
                    return True
                if result != 1:
                    # A freshly generated UUID should not match an existing
                    # token. Treat such a response as an invariant violation.
                    raise LockError(
                        "Unexpected token collision",
                        lock_name=str(self.name),
                    )

                 # First acquisition: publish the local owner state and start
                # one watchdog for the lifetime of this outermost acquisition.
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
        """Clear local state after ownership has been proven invalid.
        Acquisition, release, and the watchdog can all discover a lost lease.
        Centralizing the cleanup keeps those paths consistent.
        """
        if self._token != token:
            return

        self._owner_task = None
        self._token = None
        self._depth = 0
        self._lock_lost.set()

        await self._stop_watchdog()


    async def release(self) -> None:
       """Release one level of ownership held by the current task.

        The Lua script decrements the Redis reentrancy count. It deletes the
        key only when the count reaches zero. A task other than the recorded
        owner is never allowed to release this lock instance.
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
        """Start one background renewal task for the captured owner token.

        The watchdog is a different asyncio task from the lock owner, so it
        must use the captured token instead of task-ownership checks.
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
        """Renew the lease until release, cancellation, or ownership loss.

        Normal renewals use ``renew_interval``. After a Redis error, retries
        use a shorter delay bounded by the estimated remaining lease time.
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
        """Renew a lease for an explicit token without task-owner checks."""
        ttl = max(1, int(self.timeout * 1000)) 

        resp = await self.lua_reentrant_extend(
            keys = [self.name],
            args = [token, ttl],
            client = self.redis
        ) # type: ignore

        return bool(resp)

    async def renew(self) -> bool:
        """Manually renew the lease from the owning application task."""
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
        """Cancel and await the watchdog without ever awaiting itself."""
        
        task, self._watchdog_task = self._watchdog_task, None
        current = asyncio.current_task()
        # 关键：不能在 watchog 自己里 await 自己，会死锁
        if task and task is not current and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    # Async context-manager support allows nested ``async with lock`` blocks.
    async def __aenter__(self) -> "ReentrantRedisLock":
        acquired = await self.acquire()
        if not acquired:
            raise LockError(
                "Unable to acquire lock within the time specified",
                lock_name=str(self.name),
            )
        return self

    async def __aexit__(self, exc_type, exc, tb):
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
