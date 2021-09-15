# -*- coding: utf-8 -*-

import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../..")

import asyncio
from pythinkutils.aio.common.aiolog import g_aio_logger

async def main():
    await g_aio_logger.debug("FXXK")
    await g_aio_logger.info("FXXK")
    await g_aio_logger.info("FXXK1")

if __name__ == '__main__':
    asyncio.run(main())