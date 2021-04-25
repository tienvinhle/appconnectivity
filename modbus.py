import asyncio
import functools
import threading
import time
from pymodbus.client.asynchronous.tcp import AsyncModbusTCPClient as ModbusClient
from pymodbus.client.asynchronous import schedulers
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.constants import Endian
import json
from collections import OrderedDict
import datetime

defaultOrder = Endian.Little
#defaultNoWords = {
#	"bits":1,
#	"int8":1,
#	"uint8":1,
#	"int16":2,
#	"uint16":2,
#	"int32":4,
#	"uint32":4,
#	"int64":8,
#	"uint64":8,
#	"float16":2,
#	"float32":4,
#	"float64":8
#}


class ModbusDevice:
	def __init__(self, thingID, config, sharedQueue, loopMainThread):
		self._thingID = thingID
		self._ip = config["ip"]
		self._port = config["port"]
		self._unitID = config["unitID"]
		self._startingAddress = config["startingAddress"]
		self._wordOrder = self.get_data_format(config["dataFormat"]["wordOrder"])
		self._byteOrder = self.get_data_format(config["dataFormat"]["byteOrder"])
		self._supportedTypes = config["dataFormat"]["supportedTypes"]
		self._scanningCycleInSecond = config["scanningCycleInSecond"]
		self._requestCycle = config["minResponseTimeInMiliSecond"] /1000
		self._cycleTTL = self._scanningCycleInSecond / self._requestCycle
		self._eventLoopLocal = None
		self._evetLoopMainThread = loopMainThread
		self._conn = None
		self._queue = sharedQueue
		self._on_task_finsih_callback = None
		self._taskDict = config["tasks"]
		self._tasks = []
		self._tryingconnect = False
	
	def get_data_format(self, stringType):
		returnedValue = None
		if (stringType== "Endian.Little"):
			returnedValue = Endian.Little
		elif (stringType== "Endian.Big"):
			returnedValue = Endian.Big
		else:
			returnedValue = defaultOrder
		return returnedValue

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
		self.create_task(self._taskDict)
		self._conn.loop.run_forever()

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
					#Start to create tasklist
					#self.createTask(self._tasks)
				else:
					print('Try to reconnect ...', self._ip)
					asyncio.ensure_future(self.tryconnect(), loop=self._conn.loop)
		else:
			print('There is already some tryconnect coroutine is running. no need to schedule this one')

	async def send_queue(self, data):
		self._queue.put_nowait(data)
		await self._queue.join()

	async def read_registers(self, task, taskID):
		responseSet = []
		if self._conn.protocol:
			startAddress = task["offSet"]
			numberOfWords = task["numberOfWords"]
			result = await self._conn.protocol.read_holding_registers(startAddress, numberOfWords, unit=self._unitID)
			#check if out of range
			for reg in result.registers:
				responseSet.append(reg)
			#decode recieved values into tagName and prepare for adding to Queue
			datapointList = self.task_decode(responseSet, task, taskID)
			for datapoint in datapointList:
				asyncio.run_coroutine_threadsafe(self.send_queue(datapoint), self._evetLoopMainThread)
		else:
			print('Connection lost during task execution. Cancel this red_registers coroutine')

#========= Task creation ============

	def create_task(self, taskDictionary):
		#task creation goes in sequence: read_coils, read_registers, write_coils, write_registers
		creation_sequence = ['read_coils', 'read_registers', 'write_coils', 'write_registers']
		for taskType in creation_sequence:
			if (taskDictionary[taskType] is not None):
				self.sort_task_by_offset(taskDictionary[taskType])
				self.register_task(taskDictionary[taskType], taskType)
		#start to execute registered tasks
		asyncio.ensure_future(self.task_executor(self._tasks), loop=self._conn.loop)

	def custom_sort_by_offset(self, item):
		return item["offSet"]

	def sort_task_by_offset(self, taskList):
		#taskList is like : [{"tagName": "power", "unit": "W", "offSet": 116, "dataType":"uint16", "PF":1, "size":1},]
		#					 {"tagName": "powerGenerated", "unit": "kWh", "offSet": 118, "dataType":"uint16", "PF":1, "size":1}]
		#defin sort by offset
		taskList.sort(key = self.custom_sort_by_offset)
		#assign numberofWord to each register
		for reg in taskList:
			#check if the dataType is valid
			#if reg["dataType"] not in defaultNoWords.keys():
			#	print("Not supported data type")
				#if it is invalid then break the for loop
			#	break
			# 1 size = 1 register = 2 words
			reg["numberOfWords"] = reg["size"] * 2

	def register_task(self, taskList, taskType):
		#taskList is like : [{"tagName": "power", "unit": "W", "offSet": 116, "dataType":"uint16",  "PF":1, "size":1, "numberOfWords":2},
		#					{"tagName": "powerGenerated", "unit": "kWh", "offSet": 118, "dataType":"uint16",  "PF":1, "size":1, "numberOfWords":2}]
		taskCount = len(taskList)
		#if number of tasks = 0 then return with no registration
		if taskCount == 0:
			return
		#tempTask to store current conclusion and assign tracking value to taskList's 1st component
		tempTask = {"taskType": taskType, "offSet":taskList[0]["offSet"], "numberOfWords":taskList[0]["numberOfWords"]}
		#insert the tempTask to global taskList at the very bottom of the list. Then check its position
		self._tasks.append(tempTask)
		taskPosition = len(self._tasks) -1
		#update the position and taskID in the task. The 1st task now be like
		#{"tagName": "power", "unit": "W", "offSet": 116, "dataType":"uint16", "numberOfWords":2, "taskID": taskPosition, "dataPosition": 0}
		taskList[0]["taskID"] = taskPosition
		taskList[0]["dataPosition"] = 0
		#update the TTL of the task to be exactly its position so the least the position is, the high its priority is
		self._tasks[taskPosition]["TTL"] = taskPosition
		#start to check if only the number of tasks starting from 2
		if taskCount > 1:
			for i in range(1, taskCount):
				#check if this register is nearby the previous one
				if (taskList[i]["offSet"] == (taskList[i-1]["offSet"] + taskList[i-1]["numberOfWords"])):
					#If yes. Then increase the total number of words that need to be read
					self._tasks[taskPosition]["numberOfWords"] = self._tasks[taskPosition]["numberOfWords"] + taskList[i]["numberOfWords"]
					#update the position and taskID for this task
					taskList[i]["taskID"] = taskPosition
					taskList[i]["dataPosition"] = taskList[i-1]["dataPosition"] + taskList[i-1]["numberOfWords"]
				else:
					#create a new tempTask starting with the current task and insert into the global taskList
					tempTask = {"taskType": taskType, "offSet":taskList[i]["offSet"], "numberOfWords":taskList[i]["numberOfWords"]}
					self._tasks.append(tempTask)
					taskPosition = len(self._tasks) -1
					#update the current task
					taskList[i]["taskID"] = taskPosition
					taskList[i]["dataPosition"] = 0
					self._tasks[taskPosition]["TTL"] = taskPosition

	async def task_executor(self, taskList):
		#taskList is like [{"taskType": "read_registers", "offSet":116, "numberOfWords":4, "TTL":0},
		# 					{"taskType": "read_registers", "offSet":124, "numberOfWords":8, "TTL":1}]
		#make it scan for task forever
		while True:
			if len(taskList) > 0:
				#execute the top priority task which its TTL = 0
				for i in range(0, len(taskList)):
					if (taskList[i]["TTL"] == 0):
						#re-assign the TTL to the based_TTL and plus 1 for later reduction
						taskList[i]["TTL"] = self._cycleTTL +1
						if taskList[i]["taskType"] == "read_registers":
							asyncio.ensure_future(self.read_registers(taskList[i], i), loop=self._conn.loop)
					#as 1 cycle is passed, we reduce all TTL of the taskList
					taskList[i]["TTL"] = taskList[i]["TTL"] -1
			#wait for the next scanning cycle
			await asyncio.sleep(self._requestCycle)

	def task_decode(self, valueList, task, taskID):
		#taskID = 1
		#task = {"taskType": "read_registers", "offSet":116, "numberOfWords":4, "TTL":0}
		dicTask = None
		taskType = task["taskType"]
		dicTaskList = self._taskDict[taskType]
		#dictTaskList is like [{"tagName": "power", "unit": "W", "offSet": 116, "dataType":"uint16",  "PF":1, "size":1, "numberOfWords":2, "taskID": taskPosition, "dataPosition": 0}]
		decodedDatapointList = []
		decodedDatapoint = None
		for taskDict in dicTaskList:
			#look for the register that match with the task
			if (taskDict["taskID"] == taskID):
				#prepare some info
				startingPosition = taskDict["dataPosition"]
				endingPosition = taskDict["dataPosition"] + taskDict["numberOfWords"] + 1
				dataType = taskDict["dataType"]
				rawValue = valueList[startingPosition:endingPosition]
				size = taskDict["size"]
				#decode value
				decodedValue = self.value_decode(rawValue, dataType, size)
				#calculate with Power Factor
				if taskDict["PF"] is not None:
					finallValue = decodedValue * (10**taskDict["PF"])
				else:
					finalValue = decodedValue
				#prepare the frame to send
				dataSend = {"value": finalValue, "unit": taskDict["unit"], "dataType":taskDict["dataType"], "timeStamp": str(datetime.datetime.utcnow())}
				decodedDatapoint = {"thingID": self._thingID, "datapoint": taskDict["tagName"], "dataValue": dataSend}
				decodedDatapointList.append(decodedDatapoint)
		return decodedDatapointList

	def value_decode(self, registers, typeString, size):
		decoder = BinaryPayloadDecoder.fromRegisters(registers, byteorder=self._byteOrder, wordorder=self._wordOrder)
		value = None
		if (typeString in self._supportedTypes):
			if (typeString == "bits"):
				value = decoder.decode_bits()
			elif (typeString == "int8"):
				value = decoder.decode_8bit_int()
			elif (typeString == "uint8"):
				value = decoder.decode_8bit_uint()
			elif (typeString == "int16"):
				value = decoder.decode_16bit_int()
			elif (typeString == "uint16"):
				value = decoder.decode_16bit_uint()
			elif (typeString == "int32"):
				value = decoder.decode_32bit_int()
			elif (typeString == "uint32"):
				value = decoder.decode_32bit_uint()
			elif (typeString == "float16"):
				value = decoder.decode_16bit_float()
			elif (typeString == "float32"):
				value = decoder.decode_32bit_float()
			elif (typeString == "int64"):
				value = decoder.decode_64bit_int()
			elif (typeString == "uint64"):
				value = decoder.decode_64bit_uint()
			elif (typeString == "float64"):
				value = decoder.decode_64bit_float()
			elif (typeString == "string"):
				value = decoder.decode_string(size).decode()
			else:
				value = "Invalid type"
		else:
			value = "Not supported type"
		return value