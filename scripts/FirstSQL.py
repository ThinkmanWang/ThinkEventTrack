# -*- coding: utf-8 -*-

from pydruid.db import connect
from prettytable import PrettyTable

from pythinkutils.common.log import g_logger
from pythinkutils.common.StringUtils import *

g_connDruid = connect(host='172.16.0.2', port=9002, path='/druid/v2/sql/', scheme='http')


def test1():
    global g_connDruid
    curs = g_connDruid.cursor()
    curs.execute('''
        select count(1) as cnt from thinkeventtrack
    ''')

    for row in curs:
        print(row)

def test2():
    global g_connDruid
    curs = g_connDruid.cursor()
    curs.execute('''
        select 
            device
            , type
            , count(1) as cnt 
        from 
            thinkeventtrack 
        WHERE 
            __time >= TIME_PARSE('2021-09-17 00:00:00', 'YYYY-MM-dd HH:mm:ss', '+08:00')
            and __time <= TIME_PARSE('2021-09-17 23:59:59', 'YYYY-MM-dd HH:mm:ss', '+08:00')
        GROUP BY
            device, type
    ''')

    pTable = PrettyTable(["device", "type", "cnt"])
    for row in curs:
        if is_empty_string(row.device):
            pTable.add_row(["Unknow", row.type, row.cnt])
        else:
            pTable.add_row([row.device, row.type, row.cnt])
        # dictRow = {
        #     "device": row.device
        #     , "type": row.type
        #     , "cnt": row.cnt
        # }
        #
        # lstData.append(dictRow)
    print(pTable)

def query_pv_by_date():
    global g_connDruid
    curs = g_connDruid.cursor()
    curs.execute('''
        select 
            TIME_FORMAT(__time, 'YYYY-MM-dd HH', '+08:00') as pv_date
            , count(__time) as msg_cnt
            , count(DISTINCT sessionId) as pv
            , count(DISTINCT userId) as uv
        from 
            thinkeventtrack 
        WHERE 
            __time >= TIME_PARSE('2021-09-01 00:00:00', 'YYYY-MM-dd HH:mm:ss', '+08:00')
            and __time <= TIME_PARSE('2021-09-30 23:59:59', 'YYYY-MM-dd HH:mm:ss', '+08:00')
        GROUP BY
            TIME_FORMAT(__time, 'YYYY-MM-dd HH', '+08:00')
    ''')

    pTable = PrettyTable(["pv_date", "msg_cnt", "pv", "uv"])
    for row in curs:
        pTable.add_row([row.pv_date, row.msg_cnt, row.pv, row.uv])

    print(pTable)

def main():
    test1()
    test2()
    query_pv_by_date()

if __name__ == '__main__':
    main()