import socket
import asyncio
import pickle
import struct
from struct import *
from msg import InputMessage, StabilizeData
import hashlib

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
		self.ring = {i:self.hostNumber for i in range(0,10)}

		self.sock = socket.socket()
		self.sock.setblocking(False)
		self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

		self.sock.bind((self.HOST, self.PORT))
		self.sock.listen(len(self.hostnames)-1)

		self.serverRPC = ServerRequestHandlers()

		self.loop.create_task(self.receive_connections())
		self.loop.create_task(self.create_connection())

	def stabilization(self):

		#send my data to successor and predecessor
		if self.hostNumber in self.storage:
			msgObj = StabilizeData(self.storage[self.hostNumber], self.hostNumber)

			msg = pickle.dumps(msgObj)
			while True:
				successor = self.find_successor(self.hostNumber)
				host = self.hostnames[successor - 1]
				try:
					self.loop.sock_sendall(self.connections[socket.gethostbyname(host)], struct.pack('>I', len(msg)) + msg)
				except OSError as oe:
					pass

			# while True:
			# 	predecessor = self.find_predecessor(self.hostNumber)
			# 	host = self.hostnames[predecessor - 1]
			# 	try:
			# 		self.loop.sock_sendall(self.connections[socket.gethostbyname(host)], struct.pack('>I', len(msg)) + msg)
			# 	except OSError as oe:
			# 		pass

	def find_predecessor(self, node):
		predecessor = node - 1
		while True:
			if predecessor in self.ring.values():
				break
			else:
				predecessor -= 1
				if predecessor <= 0:
					predecessor = 10
				if predecessor == node:
					break
		return predecessor

	def find_successor(self, node):
		successor = node + 1

		while True:
			if successor in self.ring.values():
				break
			else:
				successor += 1
				if successor > 10:
					successor = 1
				if successor == node:
					break

		return successor

	def addRing(self, node, flag):
		nodeNumber = int(node.split('-')[3].split('.')[0])
		successor = self.find_successor(nodeNumber)
		predecessor = self.find_predecessor(nodeNumber)
		#update ring
		if nodeNumber < predecessor:
			for i in range(predecessor, 10):
				self.ring[i] = nodeNumber
			for i in range(0, nodeNumber):
				self.ring[i] = nodeNumber
		else:
			for i in range(predecessor, nodeNumber):
				self.ring[i] = nodeNumber

		if flag is False:
			return

		#balance storage
		transfer_keys = []
		if nodeNumber < predecessor:
			for i in range(predecessor+1, 10):
				transfer_keys.append(i)
			for i in range(0, nodeNumber+1):
				transfer_keys.append(i)
		else:
			for i in range(predecessor+1, nodeNumber+1):
				transfer_keys.append(i)

		msg = {}
		if successor in self.storage:
			for key, value in self.storage[successor].items():
				h = hashlib.md5(key.encode()).hexdigest()
				ind = int(h, base=16) % 10
				if ind+1 in transfer_keys:
					msg[key] = value
		if successor == self.hostNumber: #successor
			# if predecessor in self.storage:
			# 	self.storage[nodeNumber] = self.storage.pop(predecessor)
			#send keys

			if msg:
				msgObj = StabilizeData(msg, nodeNumber)
				new_msg = pickle.dumps(msgObj)

				host = self.hostnames[nodeNumber - 1]
				try:
					self.loop.sock_sendall(self.connections[socket.gethostbyname(host)], struct.pack('>I', len(new_msg)) + new_msg)
					if nodeNumber not in self.storage:
						self.storage[nodeNumber] = {}
					for key, value in msg.items():
						del self.storage[successor][key]	
						self.storage[nodeNumber][key] = value
					#send replicas
					msgObj = StabilizeData(self.storage[successor], successor)
					new_msg = pickle.dumps(msgObj)
					host = self.hostnames[successor - 1]
					self.loop.sock_sendall(self.connections[socket.gethostbyname(host)], struct.pack('>I', len(new_msg)) + new_msg)
					
					msgObj = StabilizeData(self.storage[predecessor], predecessor)
					new_msg = pickle.dumps(msgObj)
					host = self.hostnames[predecessor - 1]
					self.loop.sock_sendall(self.connections[socket.gethostbyname(host)], struct.pack('>I', len(new_msg)) + new_msg)
					
				except OSError as oe:
					pass

			if predecessor in self.storage:
				self.storage.pop(predecessor)

		if predecessor == self.hostNumber: #predecessor
			if successor in self.storage:
				self.storage.pop(successor)
				self.storage[nodeNumber] = msg

	def deleteRing(self, node):
		nodeNumber = int(node.split('-')[3].split('.')[0])
		successor = self.find_successor(nodeNumber)
		predecessor = self.find_predecessor(nodeNumber)

		#update ring
		if nodeNumber < predecessor:
			for i in range(predecessor, 10):
				self.ring[i] = successor
			for i in range(0, nodeNumber):
				self.ring[i] = successor
		else:
			for i in range(predecessor, nodeNumber):
				self.ring[i] = successor

		#balance storage
		if successor == self.hostNumber:
			#put all the data in replica of nodenumber into its own data
			if self.hostNumber not in self.storage:
				self.storage[self.hostNumber] = {}
			if nodeNumber in self.storage:
				self.storage[self.hostNumber] = {**self.storage.pop(self.hostNumber), **self.storage.pop(nodeNumber)}

		if predecessor == self.hostNumber:
			if nodeNumber in self.storage:
				msgObj = StabilizeData(self.storage[nodeNumber], successor)
				new_msg = pickle.dumps(msgObj)

				host = self.hostnames[successor - 1]
				try:
					self.loop.sock_sendall(self.connections[socket.gethostbyname(host)], struct.pack('>I', len(new_msg)) + new_msg)
				except OSError as oe:
					pass
				self.storage[successor] = self.storage.pop(nodeNumber)

	async def receive_connections(self):
		while True:
			client, addr = await self.loop.sock_accept(self.sock)
			client.setblocking(False)
			self.connections[addr[0]] = client
			print('New Connection: {}'.format(addr[0]))
			self.addRing(socket.gethostbyaddr(addr[0])[0], True)
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
				self.addRing(host, False)
				self.loop.create_task(self.receive_data(s, socket.gethostbyname(host)))

	async def send_data(self, messageObj):
		while True:
			self.ack.clear()

			while True:
				#send data to the owner of the key
				host = self.hostnames[messageObj.owner - 1]
				msg = pickle.dumps(messageObj)
				try:
					await self.loop.sock_sendall(self.connections[socket.gethostbyname(host)], struct.pack('>I', len(msg)) + msg)
					break
				except OSError as oe:
					messageObj.findOwner(self)

			#wait for a response until a timeout and then try again
			try:
				await asyncio.wait_for(self.ack.wait(), 3.0)
				#check if the request was successfully completed
				if self.ack.is_set():
					break
				else:
					messageObj.findOwner(self)
			except asyncio.TimeoutError as te:
				messageObj.findOwner(self)
		self.ack.clear()

	async def receive_data(self, client, addr):
		while True:
			data = await self.loop.sock_recv(client, 4)
			if not data:
				break	#connecion closed
			
			lengthBuffer = struct.unpack('>I', data)[0]
			buf = await self.loop.sock_recv(client, lengthBuffer)
			msg = pickle.loads(buf)

			#if it is a response to SET and GET
			if msg.type == "ACK" and self.ack.is_set() is False:
				print("{} {}".format(msg.key, msg.value), flush=True)
				self.ack.set()
			elif msg.type == "SET":
				await getattr(self.serverRPC, 'handle_{}'.format(msg.type))(msg, self)
				if msg.owner == self.hostNumber:
					#create an ACK msg with msg = 'SET OK' and send it back
					msgObj = InputMessage('ACK SET OK')
				else:
					msgObj = InputMessage('REPLICA ACK')
				new_msg = pickle.dumps(msgObj)
				await self.loop.sock_sendall(client, struct.pack('>I', len(new_msg)) + new_msg)

			elif msg.type == "GET":
				value = await getattr(self.serverRPC, 'handle_{}'.format(msg.type))(msg, self)

				#create an ACK msg with msg = 'GET VALUE' and send it back
				if value:
					msgObj = InputMessage('ACK FOUND: ' + value)
				else:
					msgObj = InputMessage('ACK NOT FOUND')
				new_msg = pickle.dumps(msgObj)
				await self.loop.sock_sendall(client, struct.pack('>I', len(new_msg)) + new_msg)
			elif msg.type == "OWNERS":
				value = await getattr(self.serverRPC, 'handle_{}'.format(msg.type))(msg, self)

				#create an ACK msg with msg = 'owners id' and send it back
				if value:
					msgObj = InputMessage('ACK {}'.format(value))
				else:
					msgObj = InputMessage('ACK NOT FOUND')
				new_msg = pickle.dumps(msgObj)
				await self.loop.sock_sendall(client, struct.pack('>I', len(new_msg)) + new_msg)
			elif msg.type == "STABILIZE":
				self.storage[msg.owner] = msg.data
		client.close()
		del self.connections[addr]
		#print("OLD RING: {}".format(self.ring))
		self.deleteRing(socket.gethostbyaddr(addr)[0])
		#print("NEW RING: {}".format(self.ring))
		print('Connection Closed: {}'.format(addr))

class ServerRequestHandlers:

	async def handle_SET(self, messageObj, server):
		#store the key value pair
		if messageObj.owner not in server.storage:
			server.storage[messageObj.owner] = {}
		server.storage[messageObj.owner][messageObj.key] = messageObj.value

		#if owner send to replicas
		if messageObj.owner == server.hostNumber:
			msg = pickle.dumps(messageObj)
			while True:
				successor = server.find_successor(messageObj.owner)
				host = server.hostnames[successor - 1]
				try:
					await server.loop.sock_sendall(server.connections[socket.gethostbyname(host)], struct.pack('>I', len(msg)) + msg)
					break
				except OSError as oe:
					continue

			while True:
				predecessor = server.find_predecessor(messageObj.owner)
				host = server.hostnames[predecessor - 1]
				try:
					await server.loop.sock_sendall(server.connections[socket.gethostbyname(host)], struct.pack('>I', len(msg)) + msg)
					break
				except OSError as oe:
					continue

	async def handle_GET(self, messageObj, server):
		#find the key and return
		if messageObj.owner not in server.storage:
			return False
		if messageObj.key not in server.storage[messageObj.owner]:
			return False
		return server.storage[messageObj.owner][messageObj.key]

	async def handle_OWNERS(self, messageObj, server):
		if messageObj.owner == server.hostNumber:
			#find the key and return successor and predecessor
			if messageObj.owner not in server.storage:
				return False
			if messageObj.key not in server.storage[messageObj.owner]:
				return False
			successor = server.find_successor(messageObj.owner)
			predecessor = server.find_predecessor(messageObj.owner)
			return '{} {} {}'.format(predecessor, messageObj.owner, successor)
		else:
			return False
