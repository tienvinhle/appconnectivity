import asyncio
import functools
import threading
import time
from pymodbus.client.asynchronous.tcp import AsyncModbusTCPClient as ModbusClient
from pymodbus.client.asynchronous import schedulers
import json

class ModbusDevice:
	def __init__(self, config, sharedQueue, loopMainThread):
		self._ip = config["ip"]
		self._port = config["port"]
		self._unitID = config["unitID"]
		self._eventLoopLocal = None
		self._evetLoopMainThread = loopMainThread
		self._conn = None
		self._queue = sharedQueue
		self._requestQ = []
		#request cycle = 50ms
		self._requestCycle = 100/1000
		self._on_task_finsih_callback = None
		self._taskDict = config["tasks"]
		self._tasks = {}
		self._tryingconnect = False

	def start(self, on_task_finsih_callback):
		self._on_task_finsih_callback = on_task_finsih_callback
		print('Start to connect device ', self._ip)
		self._eventLoopLocal, self._conn = ModbusClient(schedulers.ASYNC_IO, host=self._ip, port=self._port, timeout=30)
		if self._conn.protocol:
			#release tryconnect for next use
			self._tryingconnect = False
			print('Device connected ', self._ip)
		else:
			print('Cannot connect. Try to reconnect ...', self._ip)
			#self._tryingconnect = True
			asyncio.ensure_future(self.tryconnect(), loop=self._conn.loop)
		self.finalizeTask()
		self._conn.loop.run_forever()
	
	def finalizeTask(self):
		def tasksForTaskName(taskSet):
			if taskSet is None:
				return None
			else:
				tempTaskSet = []
				taskCount = len(taskSet)
				tempRec = taskSet[0]
				fpos = []
				for x in range(0, tempRec["numberOfRegisters"]):
					fpos.append(x)
					lastPos = x
				tempRec["tags"] = [{"tagName": tempRec["tagName"], "positions": fpos}]
				tempRec.pop("tagName")
				tempTaskSet.append(tempRec)
				if taskCount > 1:
					for i in range(1, taskCount):
						noRegs = taskSet[i]["numberOfRegisters"]
						if taskSet[i]["startAddress"] == (taskSet[i-1]["startAddress"] + taskSet[i-1]["numberOfRegisters"]):
							pos = []
							for no in range(0, taskSet[i]["numberOfRegisters"]):
								lastPos = lastPos +1
								pos.append(lastPos)
								newNoRegs = tempTaskSet[-1]["numberOfRegisters"] + 1
								tempTaskSet[-1]["numberOfRegisters"] = newNoRegs
							tempTaskSet[-1]["tags"].append({"tagName": taskSet[i]["tagName"], "positions": pos})
						#done the series of tasks
						else:
							nPos = []
							tempRec = taskSet[i]
							for x in range(0, tempRec["numberOfRegisters"]):
								nPos.append(x)
								lastPos = x
							tempRec["tags"] = [{"tagName": tempRec["tagName"], "positions": nPos}]
							tempRec.pop("tagName")
							tempTaskSet.append(tempRec)
				return tempTaskSet

		#shorten the given list
		tempTaskDict = self._taskDict
		for taskName, taskSet in tempTaskDict.items():
			if taskSet is not None:
				self._tasks[taskName] = []
				#group by cycle
				temp = {}
				for task in taskSet:
					cycle = task["cycleInSecond"]
					if cycle not in temp.keys():
						#prepare empty List
						temp[cycle] = []
					temp[cycle].append(task)
				for cycle, tasks in temp.items():
					tasks.sort(key= self.customSortRegister)
					tempTasks = tasksForTaskName(tasks)
					self._tasks[taskName].extend(tempTasks)
					#self.createTask(taskName, cycle, tempTasks)
		#print('Create tasks based on self._tasks:\r\n{}'.format(json.dumps(self._tasks, indent=4)))
		self.createTask(self._tasks)
		asyncio.ensure_future(self.processRequestQueue(), loop=self._conn.loop)
	
	#create tasks based on cycle
	def createTask(self, tasks):
		#print('Create task {} for cycle {} in {}:\r\n{}'.format(taskName, cycle, threading.current_thread().name, json.dumps(tasks, indent=4)))
		for taskName, taskSet in tasks.items():
			for task in taskSet:
				cycle = task["cycleInSecond"]
				asyncio.ensure_future(self.task_cycle(taskName, cycle, task), loop=self._conn.loop)

	async def task_cycle(self, taskName, cycleInSecond, task):
		while True:
			if self._conn.protocol:
				#create all read/write tasks
				#for task in tasks:
					#self._eventLoopLocal.create_task(self.read_registers(task["startAddress"], task["numberOfRegisters"], self._on_task_finsih_callback))
				TTL = cycleInSecond/self._requestCycle
				rec = {"TTL": TTL, "taskName": taskName, "request":task}
				self._requestQ.append(rec)
					#asyncio.ensure_future(self.read_registers(task["startAddress"], task["numberOfRegisters"], self._on_task_finsih_callback), loop=self._conn.loop)
					#print('Create task: ', rec)
				#wait for cycle to pass and create new read/write tasks
				await asyncio.sleep(cycleInSecond)
			else:
				print('Connection lost. No reading is schedule')
				break
		#Stop reading/writing and Try to reconnect
		asyncio.ensure_future(self.tryconnect(), loop=self._conn.loop)

	async def tryconnect(self):
		print('Start tryconnect function')
		#check if there is any tryconnect coroutine is running
		if not self._tryingconnect:
			#Let's me do this. Im working on tryconnect
			self._tryingconnect = True
			#check if connection is stopped
			if self._conn.protocol is None:
				self._conn.reset_delay()
				await asyncio.ensure_future(self._conn._reconnect(), loop=self._conn.loop)
				#release tryconnect for next use
				self._tryingconnect = False
				if self._conn.protocol:
					print('Device connected ', self._ip)
					self.createTask(self._tasks)
				else:
					print('Try to reconnect ...', self._ip)
					asyncio.ensure_future(self.tryconnect(), loop=self._conn.loop)
		else:
			print('There is already some tryconnect coroutine is running. no need to schedule this one')

	def customSortRegister(self, item):
		return item["startAddress"]

	def customSortTask(self, item):
		return item["TTL"]

	async def processRequestQueue(self):
		while True:
			for item in self._requestQ:
				curV = item["TTL"]
				item.update({"TTL": curV -1})
			self._requestQ.sort(key= self.customSortTask)
			#print(self._requestQ)
			if len(self._requestQ) > 0:
				if self._requestQ[0]["TTL"] < 1:
					toDotask = self._requestQ.pop(0)
					#print('About to execute task: ', json.dumps(toDotask, indent=4))
					if toDotask["taskName"] == "read_registers":
						asyncio.ensure_future(self.read_registers(toDotask["taskName"], toDotask["request"]["startAddress"], toDotask["request"]["numberOfRegisters"], self._on_task_finsih_callback), loop=self._conn.loop)
					elif toDotask["taskName"] == "write_registers":
						print('execute write_registers task --> ignore')
			await asyncio.sleep(self._requestCycle)

	async def send_queue(self, data):
		#print('{} about to send {} to MainThread'.format(threading.current_thread().name, data))
		self._queue.put_nowait(data)
		await self._queue.join()

	async def read_registers(self, taskName, startAddress, numberOfRegisters, callback):
		responseSet = []
		if self._conn.protocol:
			result = await self._conn.protocol.read_holding_registers(startAddress, numberOfRegisters, unit=self._unitID)
			#check if out of range
			for reg in result.registers:
				responseSet.append(reg)
			asyncio.run_coroutine_threadsafe(self.send_queue(self.convert2TagName(taskName, startAddress, responseSet)), self._evetLoopMainThread)
		else:
			print('Connection lost during task execution. Cancel this red_registers coroutine')

	def convert2TagName(self, taskName, startAddress, responseSet):
		responseCount= len(responseSet)
		if responseCount>0:
			responseTags = {}
			if taskName =='read_registers':
				tasks = self._tasks["read_registers"]
				for task in tasks:
					if task["startAddress"] == startAddress:
						taskRegCount= task["numberOfRegisters"]
						if taskRegCount != responseCount:
							print('Response length and registers not match. Something went wrong')
						else:
							value = []
							for reg in task["tags"]:
								for pos in reg["positions"]:
									value.append({startAddress+pos :responseSet[pos]})
								responseTags[reg["tagName"]] = value
								#quit for loop once hit the target
								break
			return 	responseTags
		else:
			print('response contains no components!')
			return None							