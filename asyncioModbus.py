import asyncio
import logging
from queueHandler import Message
# ----------------------------------------------------------------------- #
# Import the required asynchronous client
# ----------------------------------------------------------------------- #
from pymodbus.client.asynchronous.tcp import AsyncModbusTCPClient as ModbusClient
from pymodbus.client.asynchronous import schedulers
from threading import Thread
import time
# --------------------------------------------------------------------------- #
# configure the client logging
# --------------------------------------------------------------------------- #

logging.basicConfig()
log = logging.getLogger()
log.setLevel(logging.DEBUG)

# --------------------------------------------------------------------------- #
# specify slave to query
# --------------------------------------------------------------------------- #
# The slave to query is specified in an optional parameter for each
# individual request. This can be done by specifying the `unit` parameter
# which defaults to `0x00`
# --------------------------------------------------------------------------- #


UNIT = 0x00


async def start_async_test(client):
    # ----------------------------------------------------------------------- #
    # specify slave to query
    # ----------------------------------------------------------------------- #
    # The slave to query is specified in an optional parameter for each
    # individual request. This can be done by specifying the `unit` parameter
    # which defaults to `0x00`
    # ----------------------------------------------------------------------- #

    log.debug("Write to a holding register and read back")
    rq = await client.write_register(100, 10, unit=UNIT)
    rr = await client.read_holding_registers(100, 1, unit=UNIT)
    assert(rq.function_code < 0x80)     # test that we are not an error

def startModbus(loop):
    """
    An already running loop is passed to ModbusClient Factory
    :return:
    """
    log.debug("Running Async client with asyncio loop already started")
    log.debug("------------------------------------------------------")

    def done(future):
        log.info("Done Writing and Reading!!!")

    loop, client = ModbusClient(schedulers.ASYNC_IO, host="115.78.6.251", port=5589, loop=loop)
    future = asyncio.run_coroutine_threadsafe(
        start_async_test(client.protocol), loop=loop)
    future.add_done_callback(done)
    while not future.done():
        time.sleep(0.1)
    loop.stop()
    log.debug("--------DONE RUN_WITH_ALREADY_RUNNING_LOOP-------------")
    log.debug("")

if __name__ == '__main__':
    # Run with No loop
    log.debug("Running Async client")
    log.debug("------------------------------------------------------")

    def start_loop(loop):
        """
        Start Loop
        :param loop:
        :return:
        """
        asyncio.set_event_loop(loop)
        loop.run_forever()

    loop = asyncio.new_event_loop()
    t = Thread(target=start_loop, args=[loop])
    t.daemon = True
    # Start the loop
    t.start()
    assert loop.is_running()
    asyncio.set_event_loop(loop)    
    msg = Message('127.0.0.1', 6379)
    await loop.create_task(msg.connect_to_redis())
    print('Connect to Redis successfully')
    startModbus(loop)

    # Run with loop not yet started
    # run_with_not_running_loop()

    # Run with already running loop
    # run_with_already_running_loop()

    log.debug("")