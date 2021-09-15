# -*- coding: utf-8 -*-
import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../..")

import asyncio

import aiomysql

from pythinkutils.aio.mysql.ThinkAioMysql import ThinkAioMysql
from pythinkutils.aio.common.aiolog import g_aio_logger
from pythinkutils.common.object2json import *

async def insert_test():
    try:
        conn_pool = await ThinkAioMysql.get_conn_pool_ex()
        async with conn_pool.acquire() as conn:
            try:
                async with conn.cursor() as cur:
                    nRet = await cur.execute('''
                        INSERT INTO 
                            t_test(name)
                        VALUES
                            (%s)
                    ''', ("FXXXXK", ))

                    await conn.commit()
                    return nRet
            except Exception as e:
                await g_aio_logger.info(e)
                return 0
            finally:
                conn.close()
    except Exception as ex:
        await g_aio_logger.info(ex)
        return 0


async def query_test():
    try:
        conn_pool = await ThinkAioMysql.get_conn_pool_ex()
        async with conn_pool.acquire() as conn:
            try:
                async with conn.cursor(aiomysql.cursors.DictCursor) as cur:
                    await cur.execute('''
                        SELECT 
                            * 
                        FROM 
                            t_test
                    ''')

                    rows = await cur.fetchall()
                    if len(rows) <= 0:
                        return None

                    return rows
            except Exception as e:
                await g_aio_logger.info(e)
                return None
            finally:
                conn.close()

    except Exception as e:
        await g_aio_logger.info(e)
        return None

async def test():
    await insert_test()

    lstData = await query_test()
    if lstData is None or len(lstData) <= 0:
        await g_aio_logger.info("lstData is empty")
        return

    for dictData in lstData:
        await g_aio_logger.info(obj2json(dictData))

def main():
    loop = asyncio.get_event_loop()
    asyncio.gather(test())
    loop.run_forever()

if __name__ == '__main__':
    main()