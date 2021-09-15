# -*- coding: UTF-8 -*-

import os
import random
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../..")

import asyncio
from aiokafka import AIOKafkaProducer
from pythinkutils.aio.common.aiolog import g_aio_logger
from pythinkutils.config.Config import g_config
from faker import Faker
from pythinkutils.common.datetime_utils import *
from pythinkutils.common.object2json import *

async def send_one():
    producer = AIOKafkaProducer(loop=asyncio.get_event_loop(), bootstrap_servers=g_config.get("kafka", "host"))
    # Get cluster layout and initial topic/partition leadership information
    await producer.start()
    try:
        # Produce message
        nTS = random.randint(946656000, 1609430400)
        szDate = timestamp2str(nTS)
        szDate = "{}.000Z".format(szDate.replace(" ", "T"))

        dictMsg = {"time":szDate,"channel":"#en.wikipedia","cityName":None,"comment":"(edited with [[User:ProveIt_GT|ProveIt]])","countryIsoCode":None,"countryName":None,"isAnonymous":False,"isMinor":False,"isNew":False,"isRobot":False,"isUnpatrolled":False,"metroCode":None,"namespace":"Main","page":"Tom Watson (politician)","regionIsoCode":None,"regionName":None,"user":"Eva.pascoe","delta":182,"added":182,"deleted":0}
        await g_aio_logger.info(obj2json(dictMsg))
        await producer.send_and_wait("druid-test", obj2json(dictMsg).encode())
    except Exception as e:
        await g_aio_logger.error(e)
    finally:
        # Wait for all pending messages to be delivered or expire.
        await producer.stop()

async def main():
    await g_aio_logger.info("FXXK")
    for i in range(100000):
        await send_one()
    await g_aio_logger.info("DONE")


if __name__ == '__main__':
    asyncio.run(main())