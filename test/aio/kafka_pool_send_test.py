# -*- coding: utf-8 -*-

import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../..")

import asyncio

from aiokafka import AIOKafkaProducer
from pythinkutils.aio.common.aiolog import g_aio_logger
from pythinkutils.config.Config import g_config
from faker import Faker
from pythinkutils.common.datetime_utils import *
from pythinkutils.common.object2json import *

from asyncio_pool import AioPool

from pythinkutils.aio.common.aiolog import g_aio_logger

g_pool = None

async def cor_worker(nIdx):
    producer = AIOKafkaProducer(loop=asyncio.get_event_loop(), bootstrap_servers=g_config.get("kafka", "host"))
    # Get cluster layout and initial topic/partition leadership information
    await producer.start()
    try:
        # Produce message
        nTS = random.randint(946656000, 1609430400)
        szDate = timestamp2str(nTS)
        szDate = "{}.000Z".format(szDate.replace(" ", "T"))

        dictMsg = {"time": szDate, "channel": "#en.wikipedia", "cityName": None,
                   "comment": "(edited with [[User:ProveIt_GT|ProveIt]])", "countryIsoCode": None, "countryName": None,
                   "isAnonymous": False, "isMinor": False, "isNew": False, "isRobot": False, "isUnpatrolled": False,
                   "metroCode": None, "namespace": "Main", "page": "Tom Watson (politician)", "regionIsoCode": None,
                   "regionName": None, "user": "Eva.pascoe", "delta": 182, "added": 182, "deleted": 0}
        await g_aio_logger.info("%d ==> %s" % (nIdx, obj2json(dictMsg)))
        await producer.send_and_wait("druid-test", obj2json(dictMsg).encode())
    except Exception as e:
        await g_aio_logger.error(e)
    finally:
        # Wait for all pending messages to be delivered or expire.
        await producer.stop()

async def start_pool(nSize):
    global g_pool
    g_pool = AioPool(size=512)

    for i in range(nSize):
        g_pool.spawn_n(cor_worker(i))

def main():
    loop = asyncio.get_event_loop()
    asyncio.gather(start_pool(100000))
    # asyncio.gather(test1())
    loop.run_forever()

if __name__ == '__main__':
    main()
