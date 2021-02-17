import asyncio
import aioredis
import json
from collections import OrderedDict

class Message(object):
    def __init__(self, redisHost, redisPort):
        self.redisHost = redisHost
        self.redisPort = redisPort
        self.workers = dict()
        self.redis = None
        self.channels = []

    def add_worker(self, workName, workerInstance):
        self.workers[workName] = workerInstance

    async def connect_to_redis(self):
        conn = 'redis://' + self.redisHost + ':' + str(self.redisPort)
        print(conn)
        self.redis = await aioredis.create_redis(conn)

    async def send_message(self, channel, message):
        print('About to publish %s to channel %s', message, channel)
        await self.redis.publish(channel, message)

    async def add_channel(self, *channels):
        if len(channels)>0:
            for channel in channels:
                res = await self.redis.subscribe(channel)
                self.channels.append(res[0])
                tsk = asyncio.ensure_future(self.callback_message_comes(res[0]))
                await tsk
    
    async def callback_message_comes(self, channel):
        while (await channel.wait_message()):
            msg = await channel.get_json()
            print("Got Message:", msg)
            worker = self.workers.get(msg['type'], "nothing")
            worker.perform(msg)
