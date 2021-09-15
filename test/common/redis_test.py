# -*- coding: utf-8 -*-
import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../..")

import redis
from pythinkutils.redis.ThinkRedis import ThinkRedis
from pythinkutils.redis.RedisLock import *
from pythinkutils.common.datetime_utils import *
from pythinkutils.common.log import g_logger
from pythinkutils.common.StringUtils import *

# if __name__ == '__main__':
#     r = redis.StrictRedis(connection_pool=g_redis_pool)
#     szID = acquire_lock_with_timeout(r, "migu_register_fxxk", acquire_timeout=10, lock_timeout=60)
#     if False == is_empty_string(szID):
#         print szID
#     else:
#         print "FXXK"

def main():
    r = redis.StrictRedis(connection_pool=ThinkRedis.get_conn_pool_ex())
    r.set("FXXK", get_current_time_str())
    byteRet = r.get("FXXK")
    g_logger.info(byteRet)

    if byteRet is not None:
        szRet = str(byteRet, encoding="utf-8")
        g_logger.info(szRet)

    szID = acquire_lock_with_timeout(r, "migu_register_fxxk", acquire_timeout=10, lock_timeout=60)
    if False == is_empty_string(szID):
        g_logger.info(szID)
    else:
        g_logger.info("FXXK")


if __name__ == '__main__':
    main()