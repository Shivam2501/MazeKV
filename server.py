import socket
import asyncio

class Server:

	HOST = ''
	PORT = 8888

	hostnames = ["sp17-cs425-g20-01.cs.illinois.edu", "sp17-cs425-g20-02.cs.illinois.edu", "sp17-cs425-g20-03.cs.illinois.edu",
				 "sp17-cs425-g20-04.cs.illinois.edu", "sp17-cs425-g20-05.cs.illinois.edu", "sp17-cs425-g20-06.cs.illinois.edu",
				 "sp17-cs425-g20-07.cs.illinois.edu", "sp17-cs425-g20-08.cs.illinois.edu"]

	def __init__(self, loop):
		self.loop = loop
		self.connections = {}

		self.sock = socket.socket()
		self.sock.setblocking(False)
		self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

		self.sock.bind((self.HOST, self.PORT))
		self.sock.listen(len(self.hostnames)-1)

		self.loop.create_task(self.receive_connections())
		self.loop.create_task(self.create_connection())

	async def receive_connections(self):
		while True:
			client, addr = await self.loop.sock_accept(self.sock)
			client.setblocking(False)
			self.connections[addr[0]] = client
			print('\nNew Connection: {}'.format(addr[0]))
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
				self.loop.create_task(self.receive_data(s, socket.gethostbyname(host)))

	async def send_data(self, message):
		for client in self.connections.values():
			self.loop.sock_sendall(client, message.encode('utf8'))

	async def receive_data(self, client, addr):
		while True:
			data = await self.loop.sock_recv(client, 1024)
			if not data:
				break	#connecion closed
			print(data)
		client.close()
		del self.connections[addr]
		print('\nConnection Closed: {}'.format(addr))

# if __name__ == "__main__":
# 	loop = asyncio.get_event_loop()
# 	Server(loop)
# 	loop.run_forever()
