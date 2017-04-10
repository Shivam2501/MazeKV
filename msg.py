import hashlib

class InputMessage:

	def __init__(self, message):
		self.message = message

		self.type = message.split(' ')[0]
		if self.type == "SET":
			self.key = message.split(' ')[1]
			self.value = message.split(message.split(' ')[1])[1].strip()
		elif self.type == "GET":
			self.key = message.split(' ')[1]
		elif self.type == "OWNERS":
			self.key = message.split(' ')[1]
		else:
			self.key = self.message

		h = hashlib.md5(self.key.encode()).hexdigest()
		self.hashkey = int(h, base=16) % 10

	def findOwner(self, server):
		print(self.ring.values)
		self.owner = server.ring[self.hashkey]
