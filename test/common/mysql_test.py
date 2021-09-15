# -*- coding: utf-8 -*-
import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../..")

import pymysql

from pythinkutils.mysql.ThinkMysql import ThinkMysql
from pythinkutils.common.log import g_logger
from pythinkutils.common.object2json import *

def insert_test():
    conn = ThinkMysql.get_conn_pool().connection()
    try:
        cur = conn.cursor(pymysql.cursors.DictCursor)

        nRet = cur.execute('''
            INSERT INTO 
                t_test(name)
            VALUES
                (%s)
        ''', ("FXXK"))

        conn.commit()

        return nRet
    except Exception as ex:
        pass
    finally:
        conn.close()

def query_test():
    conn = ThinkMysql.get_conn_pool_ex().connection("")
    c = conn.cursor(pymysql.cursors.DictCursor)

    try:
        c.execute('''
            SELECT
                *
            FROM
                t_test
        ''')

        rows = c.fetchall()

        return rows
    except Exception as e:
        g_logger.error(e)
        return None
    finally:
        conn.close()

def main():
    insert_test()

    lstData = query_test()
    if lstData is None or len(lstData) <= 0:
        g_logger.info("NO DATA")

    for dictData in lstData:
        g_logger.info(obj2json(dictData))

if __name__ == '__main__':
    main()