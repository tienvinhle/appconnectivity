import asyncio
from modbus import ModbusDevice
import functools

mb1 = None
OFFSET = 0x01

def deviceInit(eventloop):
	global mb1
	print('Create devices ...')
	mb1 = ModbusDevice("115.78.6.251", 5589, OFFSET,eventloop)
	print('Connecting devices ...')
	mb1.connect()

def callRedisClient():
	print('Send to Redis')

async def updateValue(period):
	global mb1
	print('About to update Value')
	while True:
		print('About to read holding register...')
		mb1.read_holding_Reg(100, 2, callRedisClient)
		await asyncio.sleep(1)

async def main():
	loop = asyncio.get_running_loop()
	deviceInit(loop)
	print('About to create task reading holding register')
	loop.create_task(functools.partial(updateValue, 1))

if __name__ == '__main__':
	asyncio.run(main())