import asyncio
import sys
from server import Server, ServerRequestHandlers
from msg import InputMessage

inputQueue = asyncio.Queue()

async def inputMessage(loop):
	return await inputQueue.get()

def addToQueue():
	asyncio.ensure_future(inputQueue.put(sys.stdin.readline().strip()))

async def interface(loop):
	server = Server(loop)
	clientRPC = ClientRequestHandlers()

	#inputSymbol = '>>> '
	loop.add_reader(sys.stdin, addToQueue)

	while True:
		#print(inputSymbol, end='', flush=True)
		command = await inputMessage(loop)
		
		if command == 'exit':
			print("Exiting......")
			break

		#call request handler based on message type
		msgObj = InputMessage(command)
		msgObj.findOwner(server)

		await getattr(clientRPC, 'handle_{}'.format(msgObj.type))(msgObj, server)

class ClientRequestHandlers:

	def __init__(self):
		self.serverRPC = ServerRequestHandlers()

	async def handle_SET(self, messageObj, server):
		#check if client is the owner of the key
		print("OWNER: {} HOST: {}".format(messageObj.owner, server.hostNumber))
		if messageObj.owner == server.hostNumber:
			#call server
			await getattr(self.serverRPC, 'handle_{}'.format(messageObj.type))(messageObj, server)
			print("SET OK", flush=True)
		else:
			#send message to owner
			await server.send_data(messageObj)

	async def handle_GET(self, messageObj, server):
		#check if client is the owner of the key
		if messageObj.owner == server.hostNumber:
			#call server
			value = await getattr(self.serverRPC, 'handle_{}'.format(messageObj.type))(messageObj, server)
			if value:
				print('FOUND: ' + value, flush=True)
			else:
				print('NOT FOUND', flush=True)
		else:
			#send message to owner
			await server.send_data(messageObj)

	async def handle_OWNER(self, messageObj, server):
		#return all owners of the key from dictionary
		#find successor
		successor = messageObj.owner + 1
		while True:
			if successor in server.ring.values():
				break
			else:
				successor += 1
				if successor > 10:
					successor = 1
				if successor == messageObj.owner:
					break

		predecessor = messageObj.owner - 1
		while True:
			if predecessor in server.ring.values():
				break
			else:
				predecessor -= 1
				if predecessor <= 0:
					predecessor = 10
				if predecessor == messageObj.owner:
					break

		print('{} {} {}'.format(messageObj.owner, successor, predecessor))

	async def handle_LIST_LOCAL(self, messageObj, server):
		#find all the stored keys and return
		for host in server.storage.values():
			for key in host.keys():
				print(key, flush=True)
		print('END LIST', flush=True)