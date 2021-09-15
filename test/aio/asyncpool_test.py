# -*- coding: utf-8 -*-
import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../..")

import asyncio
from asyncio_pool import AioPool

from pythinkutils.aio.common.aiolog import g_aio_logger

g_pool = None

async def cor_worker(nNum):
    await g_aio_logger.info("FXXK %d" % (nNum,))
    # await asyncio.sleep(5)

    # while True:
    #     await g_aio_logger.info("FXXK %d" % (nNum, ))
    #     await asyncio.sleep(5)


async def start_pool(nSize):
    global g_pool
    g_pool = AioPool(size=32)

    for i in range(nSize):
        g_pool.spawn_n(cor_worker(i))

    # await g_pool.join()
    # await g_aio_logger.info("pool stoped")

async def test1():
    global g_pool

    await g_aio_logger.info("FXXXXXXXXXXXXK")
    await asyncio.sleep(10)

    for i in range(10):
        g_pool.spawn_n(cor_worker(i))

def main():
    loop = asyncio.get_event_loop()
    asyncio.gather(start_pool(5))
    asyncio.gather(test1())
    # asyncio.gather(test1())
    loop.run_forever()

if __name__ == '__main__':
    main()