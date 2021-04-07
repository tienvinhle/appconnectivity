import asyncio
from modbus import ModbusDevice
from queueHandler import Message
from threading import Thread
import threading
import functools
import json
from collections import OrderedDict

mbD = []
configPath = "/etc/modbus/deviceConfig.conf"
msg = None

def deviceInit(eventloop, sharedQueue):
	global msg
	def start_loop(device):
		print('Connecting device ... from thread: ', d._ip, threading.current_thread().name)
		device.start(on_read_register)
	
	def on_read_register(regSet):
		for reg in regSet:
			asyncio.run_coroutine_threadsafe(send2Redis(reg), eventloop)

	print('Create devices ...')
	conf = getConfig(configPath)
	msgConf = conf.get("message")
	msg = Message(msgConf["ip"], msgConf["port"])
	for mb in conf.get("modbustcp").values():
		mbD.append(ModbusDevice(mb, sharedQueue, eventloop))		
		print('Device was created: ', mb.get("ip"), mb.get("port"), mb.get("unitID"))
	for d in mbD:
		t = Thread(target=start_loop, args=[d])
		t.daemon = True
		t.start()
	print('About to run loop in mainthread')

def getConfig(configPath):
	with open(configPath, 'r') as f:
			config = json.load(f, object_pairs_hook=OrderedDict)
			return config

async def on_msg_queue(queue):
	while True:
		# Get a "work item" out of the queue.
		data = await queue.get()
		# Notify the queue that the "work item" has been processed.
		queue.task_done()
		#print('Send to Redis value {} in {}'.format(data, threading.current_thread().name))
		dataString = json.dumps(data)
		await msg.send_message('data/inverterA/atmosphere1', dataString)

async def send2Redis(reg):
	await asyncio.sleep(1)
	print('Send to Redis value {} in {}'.format(reg, threading.current_thread().name))

async def main():
	loop = asyncio.get_running_loop()
	queue = asyncio.Queue()
	asyncio.set_event_loop(loop)
	deviceInit(loop, queue)
	await msg.connect_to_redis()
	await loop.create_task(on_msg_queue(queue))

if __name__ == '__main__':
	asyncio.run(main())