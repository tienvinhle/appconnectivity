import asyncio
import functools
from pymodbus.client.asynchronous.tcp import AsyncModbusTCPClient as ModbusClient
from pymodbus.client.asynchronous import schedulers

class ModbusDevice:
	def __init__(self, ip, port, offset, eventloop):
		self._ip = ip
		self._port = port
		self._offset = offset
		print('Start to init Object')
		self._loop, self._conn = ModbusClient(schedulers.ASYNC_IO, port=port, loop=eventloop)
	
	def read_holding_Reg(self, startAddr, noRegister, callback):
		task= self._loop.create_task(functools.partial(self.read_holding_registers, startAddr, noRegister))
		task.add_done_callback(callback)

	async def read_holding_registers(self, startAddr, noRegister):
		for reg in (await self._conn.protocol.read_holding_registers(startAddr, noRegister, self._offset)):
			print('Value= ', reg)