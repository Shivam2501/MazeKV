import asyncio
import sys
from server import Server

inputQueue = asyncio.Queue()

async def inputMessage(loop):
	return await inputQueue.get()

def addToQueue():
	asyncio.ensure_future(inputQueue.put(sys.stdin.readline().strip()))

async def outputMessage(loop):
	return

async def interface(loop):
	server = Server(loop)

	inputSymbol = '>>> '
	loop.add_reader(sys.stdin, addToQueue)

	# asyncio.ensure_future(addToQueue(loop))

	while True:
		print(inputSymbol, end='', flush=True)
		command = await inputMessage(loop)
		#print('GOT: ' + command, flush=True)
		await server.send_data(command)

		if command == 'exit':
			print("Exiting......")
			break
