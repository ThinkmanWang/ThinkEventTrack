# -*- coding: utf-8 -*-
import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../..")

from pythinkutils.common.log import g_logger

def main():
    g_logger.info("Hello World")
    g_logger.info("Hello %s" % ("FXXK", ))

if __name__ == '__main__':
    main()