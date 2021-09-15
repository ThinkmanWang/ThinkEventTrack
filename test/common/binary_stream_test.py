# -*- coding: utf-8 -*-
import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../..")

from pythinkutils.common.log import g_logger
from pythinkutils.common.BinaryStream import BinaryStream

def main():
    bs = BinaryStream()
    bs.writeInt16(1)
    bs.writeInt32(2)
    bs.writeString("Hello World")

    byteData = bytes(bs.base_stream.getvalue())

    bs = BinaryStream(byteData)
    nVal = bs.readInt16()
    g_logger.info(nVal)

    nVal = bs.readInt32()
    g_logger.info(nVal)

    szVal = bs.readString()
    g_logger.info(szVal)

if __name__ == '__main__':
    main()