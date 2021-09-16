# -*- coding: utf-8 -*-

from pythinkutils.common.log import g_logger

import asyncio
import random
import uuid

from asyncio_pool import AioPool
from aiokafka import AIOKafkaProducer

from pythinkutils.aio.common.aiolog import g_aio_logger
from pythinkutils.config.Config import g_config
from pythinkutils.common.object2json import *
from pythinkutils.common.datetime_utils import *

g_pool = None

'''
{
    "__time": nTimestamp
    , "appId": 1
    , "sdkId": 1
    , "userId": uuid
    , "requestId": sessionId
    , "osType": 1-4 //1: PC, 2: Android, 3: IOS, 4: Others
    , "device": "OPPO/VIVO/HUAWEI/XIAOMI/iPhone"
    , "type": 1-9 1: 
    , "event": ""
    , "description": ""
}
'''

async def send_msg(dictMsg):
    producer = AIOKafkaProducer(loop=asyncio.get_event_loop(), bootstrap_servers=g_config.get("kafka", "host"))
    await producer.start()

    try:
        # Produce message
        await g_aio_logger.info(obj2json(dictMsg))
        await producer.send_and_wait(g_config.get("kafka", "topic"), obj2json(dictMsg).encode())
    except Exception as e:
        await g_aio_logger.error(e)
    finally:
        # Wait for all pending messages to be delivered or expire.
        await producer.stop()

async def fake_event(nThreadId):

    szSessionId = str(uuid.uuid4())

    # 1: PC, 2: Android, 3: IOS, 4: Others
    nOS = random.randint(1, 4)

    szDevice = ""
    if 3 == nOS:
        szDevice = "iPhone"
    elif 2 == nOS:
        szDevice = random.choice(["OPPO", "VIVO", "HUAWEI", "XIAOMI"])
    else:
        pass

    for i in range(9):
        if i >= 1:
            nRand = random.randint(1, 100)

            if nRand > 50:
                pass
            else:
                return

        dictMsg = {
            "timestamp": get_timestamp() * 1000
            , "appId": 1
            , "sdkId": 1
            , "userId": str(uuid.uuid4())
            , "sessionId": szSessionId
            , "osType": nOS
            , "device": szDevice
            , "type": i + 1
            , "event": ""
            , "description": ""
        }

        await send_msg(dictMsg)

async def cor_worker(nNum):
    await g_aio_logger.info("FXXK %d" % (nNum,))

    while True:
        await fake_event(nNum)

async def start_pool(nSize):
    global g_pool
    g_pool = AioPool(size=nSize)

    for i in range(nSize):
        g_pool.spawn_n(cor_worker(i))

    # await g_pool.join()
    # await g_aio_logger.info("pool stoped")

def main():
    loop = asyncio.get_event_loop()
    asyncio.gather(start_pool(64))
    # asyncio.gather(test1())
    loop.run_forever()

if __name__ == '__main__':
    main()