import asyncio
import pytest_asyncio
import pytest
import redis.asyncio as redis
from redis.exceptions import LockNotOwnedError, LockError
from .mylock import ReentrantRedisLock
import uuid

pytestmark = pytest.mark.asyncio(loop_scope="function")

@pytest_asyncio.fixture
async def redis_client():
    # 使用 fakeredis 模拟异步 Redis 客户端
    client = redis.Redis(
        host="your_redis_server_host", 
        port=6379, 
        password="your_redis_pwd", 
        decode_responses=True)
    yield client
    await client.aclose()


@pytest.mark.asyncio
async def test_basic_acquire_and_release(redis_client):
    """测试基本加锁与释放流程"""
    lock = ReentrantRedisLock(redis_client, name="test_lock_1", timeout=5.0)

    # 1. 初始状态
    assert not await lock.locked()
    assert not await lock.owned()

    # 2. 获取锁
    acquired = await lock.acquire()
    assert acquired is True
    assert await lock.locked() is True
    assert await lock.owned() is True
    assert lock._depth == 1

    # 3. 释放锁
    await lock.release()
    assert await lock.locked() is False
    assert await lock.owned() is False
    assert lock._depth == 0


@pytest.mark.asyncio
async def test_reentrant_lock(redis_client):
    """测试同一个 Task 的可重入性（多次加锁与对应释放）"""
    lock = ReentrantRedisLock(redis_client, name="test_lock_reentrant", timeout=5.0)

    async with lock:
        assert lock._depth == 1
        # 第一次重入
        async with lock:
            assert lock._depth == 2
            # 第二次重入
            async with lock:
                assert lock._depth == 3
            assert lock._depth == 2
        assert lock._depth == 1

    # 完全退出后应该释放
    assert await lock.locked() is False
    assert lock._depth == 0


@pytest.mark.asyncio
async def test_different_lock_instances_contention(redis_client):
    """测试不同协程（Task）之间的互斥性"""
    lock1 = ReentrantRedisLock(redis_client, name="test_lock_contest", timeout=5.0)
    lock2 = ReentrantRedisLock(redis_client, name="test_lock_contest", timeout=5.0)

    # Task 1 获取锁
    acquired1 = await lock1.acquire()
    assert acquired1 is True

    # Task 2 尝试非阻塞获取应失败
    acquired2_non_block = await lock2.acquire(blocking=False)
    assert acquired2_non_block is False

    # Task 2 尝试带短超时阻塞获取，应该超时返回 False
    start_time = asyncio.get_running_loop().time()
    acquired2_timeout = await lock2.acquire(blocking=True, blocking_timeout=0.5)
    end_time = asyncio.get_running_loop().time()
    elapsed = end_time - start_time
    assert acquired2_timeout is False
    assert 0.30 <= elapsed <= 0.70 # 确保确实等待了指定时间

    # Task 1 释放后，Task 2 应该能成功获取
    await lock1.release()
    
    acquired2_success = await lock2.acquire(blocking=False)
    assert acquired2_success is True
    assert await lock2.owned() is True
    await lock2.release()

@pytest.mark.asyncio
async def test_different_tasks_contention(redis_client):
    lock = ReentrantRedisLock(
        redis_client,
        name="test_lock_task_contention",
        timeout=5.0,
        sleep=0.1,
    )

    holder_entered = asyncio.Event()
    allow_holder_exit = asyncio.Event()

    async def holder():
        async with lock:
            holder_entered.set()
            await allow_holder_exit.wait()

    holder_task = asyncio.create_task(holder())

    # 等待 holder Task 确实持有锁
    await holder_entered.wait()

    try:
        start_time = asyncio.get_running_loop().time()

        # 当前 pytest Task 和 holder_task 是两个不同 Task
        acquired = await lock.acquire(
            blocking=True,
            blocking_timeout=0.5,
        )

        elapsed = asyncio.get_running_loop().time() - start_time

        assert acquired is False
        assert 0.30 <= elapsed <= 0.70
        assert await lock.locked() is True

    finally:
        allow_holder_exit.set()
        await holder_task

    assert await lock.locked() is False

    # holder 释放后，当前 Task 应该可以获取
    assert await lock.acquire(blocking=False) is True
    await lock.release()

@pytest.mark.asyncio
async def test_watchdog_auto_renewal(redis_client):
    """测试看门狗自动续期机制"""
    # 设置很短的 timeout 和 renew_interval，便于快速测试
    lock = ReentrantRedisLock(
        redis_client, 
        name="test_lock_watchdog", 
        timeout=2.0, 
        renew_interval=0.5, 
        auto_renew=True
    )

    async with lock:
        # 获取初始 TTL
        initial_ttl = await redis_client.pttl("test_lock_watchdog")
        assert initial_ttl > 0

        # 等待超过一个续期周期，让看门狗触发续期
        await asyncio.sleep(1.2)

        # 检查 TTL 是否被续期（应重新接近 2000ms）
        renewed_ttl = await redis_client.pttl("test_lock_watchdog")
        assert renewed_ttl > 0
        # 验证看门狗确实在后台成功工作了
        assert not lock.lost

    assert await lock.locked() is False


@pytest.mark.asyncio
async def test_illegal_release_by_other_task(redis_client):
    """测试非持有者协程尝试释放锁时抛出异常"""
    lock1 = ReentrantRedisLock(redis_client, name="test_lock_illegal", timeout=5.0)
    lock2 = ReentrantRedisLock(redis_client, name="test_lock_illegal", timeout=5.0)

    await lock1.acquire()

    # 用另一个实例（或直接篡改 owner）模拟非法释放
    # 由于 lock2 没有成功 acquire，其内部 _token 为 None
    with pytest.raises(LockError):
        await lock2.release()

    # 清理
    await lock1.release()

@pytest.mark.asyncio
async def test_watchdog_keeps_lock_beyond_original_ttl(redis_client):
    """验证看门狗能让锁存活超过原始 TTL。"""
    lock_name = f"test_watchdog:{uuid.uuid4().hex}"

    lock = ReentrantRedisLock(
        redis_client,
        name=lock_name,
        timeout=1.0,
        renew_interval=0.2,
        auto_renew=True,
    )

    async with lock:
        assert await lock.locked() is True
        assert await lock.owned() is True

        initial_ttl = await redis_client.pttl(lock_name)
        assert initial_ttl > 0

        # 等待时间明显超过原始的 1 秒 TTL。
        # 如果看门狗没有续期，此时 Redis key 应该已经过期。
        await asyncio.sleep(1.8)

        renewed_ttl = await redis_client.pttl(lock_name)

        assert renewed_ttl > 0
        assert await lock.locked() is True
        assert await lock.owned() is True
        assert lock.lost is False

    # 退出上下文后应彻底释放
    assert await lock.locked() is False
    assert await redis_client.exists(lock_name) == 0
    assert lock._depth == 0

@pytest.mark.asyncio
async def test_illegal_release_by_different_task(redis_client):
    """验证非持有者 Task 不能释放同一个锁实例。"""
    lock_name = f"test_illegal_release:{uuid.uuid4().hex}"

    lock = ReentrantRedisLock(
        redis_client,
        name=lock_name,
        timeout=5.0,
    )

    await lock.acquire()

    try:
        assert await lock.owned() is True
        owner_task = asyncio.current_task()

        async def release_from_other_task():
            # 确认当前确实是另一个 Task
            assert asyncio.current_task() is not owner_task

            # Redis 中存在锁，但当前 Task 不是持有者
            assert await lock.locked() is True
            assert await lock.owned() is False

            with pytest.raises(
                LockError,
                match="Cannot release a lock owned by another task",
            ):
                await lock.release()

        other_task = asyncio.create_task(
            release_from_other_task()
        )
        await other_task

        # 非法释放失败后，原持有者和 Redis 锁应保持不变
        assert await lock.locked() is True
        assert await lock.owned() is True
        assert lock._depth == 1

    finally:
        # 必须由原持有者 Task 释放
        if await lock.owned():
            await lock.release()

    assert await lock.locked() is False
    assert lock._depth == 0