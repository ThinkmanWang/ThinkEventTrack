# -*- coding: utf-8 -*-

from pydruid.db import connect
from prettytable import PrettyTable

from pythinkutils.common.log import g_logger
from pythinkutils.common.StringUtils import *

def test1():
    conn = connect(host='172.16.0.2', port=9002, path='/druid/v2/sql/', scheme='http')
    curs = conn.cursor()
    curs.execute('''
        select count(1) from thinkeventtrack
    ''')

    for row in curs:
        print(row)

def test2():
    conn = connect(host='172.16.0.2', port=9002, path='/druid/v2/sql/', scheme='http')
    curs = conn.cursor()
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

def main():
    test1()
    test2()

if __name__ == '__main__':
    main()