import socket
import asyncio
import pickle
import struct
from struct import *
from msg import InputMessage

class Server:

	HOST = ''
	PORT = 8888

	hostnames = ["sp17-cs425-g20-01.cs.illinois.edu", "sp17-cs425-g20-02.cs.illinois.edu", "sp17-cs425-g20-03.cs.illinois.edu",
				 "sp17-cs425-g20-04.cs.illinois.edu", "sp17-cs425-g20-05.cs.illinois.edu", "sp17-cs425-g20-06.cs.illinois.edu",
				 "sp17-cs425-g20-07.cs.illinois.edu", "sp17-cs425-g20-08.cs.illinois.edu", "sp17-cs425-g20-09.cs.illinois.edu", 
				 "sp17-cs425-g20-10.cs.illinois.edu"]

	def __init__(self, loop):
		self.loop = loop
		self.connections = {}
		self.storage = {}

		self.ack = asyncio.Event()

		hostname = socket.gethostname().split('-')
		self.hostNumber = int(hostname[3].split('.')[0])
		self.ring = {i:self.hostNumber for i in range(1,11)}

		self.successor = None
		self.predecessor = None

		self.sock = socket.socket()
		self.sock.setblocking(False)
		self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

		self.sock.bind((self.HOST, self.PORT))
		self.sock.listen(len(self.hostnames)-1)

		self.serverRPC = ServerRequestHandlers()

		self.loop.create_task(self.receive_connections())
		self.loop.create_task(self.create_connection())

	def addRing(self, node):
		nodeNumber = int(node.split('-')[3].split('.')[0])
		predecessor = nodeNumber - 1
		while True:
			if predecessor in self.ring.values():
				break
			else:
				predecessor -= 1
				if predecessor <= 0:
					predecessor = 10
				if predecessor == nodeNumber:
					break
		#update ring
		for i in range(predecessor+1, nodeNumber):
			self.ring[i] = nodeNumber

	async def receive_connections(self):
		while True:
			client, addr = await self.loop.sock_accept(self.sock)
			client.setblocking(False)
			self.connections[addr[0]] = client
			print('New Connection: {}'.format(addr[0]))
			self.addRing(socket.gethostbyaddr(addr[0])[0])
			self.loop.create_task(self.receive_data(client, addr[0]))

	async def create_connection(self):
		for host in self.hostnames:
			if host != socket.gethostname() and socket.gethostbyname(host) not in self.connections:
				try:
					s = socket.socket()
					s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
				except socket.error as msg:
					continue
				# Try to establish connection with a host
				try:
					s.connect((socket.gethostbyname(host), self.PORT))
				except socket.error as msg:
				    s.close()
				    continue
				s.setblocking(False)
				self.connections[socket.gethostbyname(host)] = s
				self.addRing(host)
				self.loop.create_task(self.receive_data(s, socket.gethostbyname(host)))

	async def send_data(self, messageObj):
		while True:
			self.ack.clear()
			#send data to the owner of the key
			host = self.hostnames[messageObj.owner]
			msg = pickle.dumps(messageObj)
			await self.loop.sock_sendall(self.connections[socket.gethostbyname(host)], struct.pack('>I', len(msg)) + msg)

			#wait for a response until a timeout and then try again
			await asyncio.wait_for(self.ack.wait(), 2.0)
			#check if the request was successfully completed
			if self.ack.is_set():
				break
			else:
				messageObj.findOwner(self)
		self.ack.clear()

	# async def broadcast(self, message):
	# 	for client in self.connections.values():
	# 		self.loop.sock_sendall(client, message.encode('utf8'))

	async def receive_data(self, client, addr):
		while True:
			data = await self.loop.sock_recv(client, 4)
			if not data:
				break	#connecion closed
			
			lengthBuffer = struct.unpack('>I', data)[0]
			msg = pickle.loads(conn.recv(data))

			#if it is a response to SET and GET
			if msg.type == "ACK" and self.ack.is_set() is False:
				print(msg.key + msg.value, flush=True)
				self.ack.set()

			elif msg.type == "SET":
				await getattr(self.serverRPC, 'handle_{}'.format(msg.type))(msg, self)
				#create an ACK msg with msg = 'SET OK' and send it back
				msgObj = InputMessage('ACK SET OK')
				msg = pickle.dumps(msgObj)
				await self.loop.sock_sendall(client, struct.pack('>I', len(msg)) + msg)

			elif msg.type == "GET":
				value = await getattr(self.serverRPC, 'handle_{}'.format(msg.type))(msg, self)

				#create an ACK msg with msg = 'SET OK' and send it back
				if value:
					msgObj = InputMessage('ACK FOUND: ' + value)
				else:
					msgObj = InputMessage('ACK NOT FOUND')
				msg = pickle.dumps(msgObj)
				await self.loop.sock_sendall(client, struct.pack('>I', len(msg)) + msg)

		client.close()
		del self.connections[addr]
		print('Connection Closed: {}'.format(addr))

class ServerRequestHandlers:

	async def handle_SET(self, messageObj, server):
		#store the key value pair
		if messageObj.owner not in server.storage:
			server.storage[messageObj.owner] = {}
		server.storage[messageObj.owner][messageObj.key] = messageObj.value

		# #if owner send to replicas
		# if messageObj.owner == server.hostNumber:
		# 	if server.successor:

	async def handle_GET(self, messageObj, server):
		#find the key and return
		if messageObj.owner not in server.storage:
			return False
		if messageObj.key not in server.storage[messageObj.owner]:
			return False
		return server.storage[messageObj.owner][messageObj.key]
