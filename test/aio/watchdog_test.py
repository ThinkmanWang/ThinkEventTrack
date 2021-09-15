# -*- coding: utf-8 -*-
import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/../..")

import asyncio

from pythinkutils.aio.watchdog.AIOWatchdog import AIOWatchdog
from pythinkutils.aio.watchdog.AIOWatchdog import AIOEventHandler

class MyEventHandler(AIOEventHandler):
    async def on_any_event(self, event):
        print('Others:', event.src_path)

    async def on_moved(self, event):
        print('Moved:', event.src_path)

    async def on_created(self, event):
        print('Created:', event.src_path)


    async def on_deleted(self, event):
        print('Deleted:', event.src_path)

    async def on_modified(self, event):
        print('Modified:', event.src_path)

async def test():
    handler = MyEventHandler()
    dog = AIOWatchdog(event_handler=handler)
    dog.start()

def main():
    loop = asyncio.get_event_loop()
    asyncio.gather(test())
    loop.run_forever()

if __name__ == '__main__':
    main()
