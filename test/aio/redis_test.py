# -*- coding: utf-8 -*-
import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../..")

import asyncio
from pythinkutils.aio.redis.ThinkAioRedisPool import ThinkAioRedisPool
from pythinkutils.common.datetime_utils import *
from pythinkutils.aio.common.aiolog import g_aio_logger
from pythinkutils.aio.redis.AioRedisLock import AioRedisLock
from aioredis import Redis

async def main():
    # conn_pool = await ThinkAioRedisPool.get_default_conn_pool()

    r = Redis(connection_pool=await ThinkAioRedisPool.get_conn_pool_ex())
    try:
        await r.set('fxxxxk', get_current_time_str())
        szRet = await r.get("fxxxxk")
        await g_aio_logger.info(szRet.decode("utf-8"))

        szVal = await AioRedisLock.acquire_with_timeout(r, "lock_test")
        await g_aio_logger.info(szVal)

        await AioRedisLock.release(r, "lock_test", szVal)

    except Exception as ex:
        await g_aio_logger.error(ex)
    finally:
        await r.close()



if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())