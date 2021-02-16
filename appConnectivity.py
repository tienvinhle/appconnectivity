import asyncio
from modbus import ModbusDevice
import functools

mb1 = None
OFFSET = 0x01

def deviceInit(eventloop):
	global mb1
	mb1 = ModbusDevice('192.168.0.106', 8080, OFFSET,eventloop)
	print('devices have been created successfully')

def callRedisClient():
	print('Send to Redis')

async def updateValue(period):
	global mb1
	while True:
		mb1.read_holding_Reg(100, 2, callRedisClient)
		await asyncio.sleep(1)

async def main():
	loop = asyncio.get_running_loop()
	deviceInit(loop)
	loop.create_task(functools.partial(updateValue, 1))

if __name__ == '__main__':
	asyncio.run(main())