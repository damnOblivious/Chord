import sys
import time
import socket
import threading
from variables import *
import helper


class Node(object):
	def __init__(self):
		self.id = 0
		self.dictionary = {}
		self.isInRing = False
		self.successor = [["", 0], 0]
		self.predecessor= [["", 0], 0]
		self.fingerTable = [[["", 0], 0]] * (M + 1)
		self.successorList = [[["", 0], 0]] * (R + 1)

		self.initiateSocket()
		self.intializeId()

	def findSuccessor(self, nodeId):
		this_node = [[self.getIpAddress(),self.getPortNumber()],self.id]

		if nodeId > self.id and nodeId <= self.successor[1]:
			return self.successor
		elif self.id == successor[1] or nodeId == self.id:
			return this_node
		elif self.successor[1] == self.predecessor[1]:
			if self.successor[1] >= self.id:
				if nodeId > self.successor[1] or nodeId < self.id:
					return this_node
			else:
				if (nodeId > self.id and nodeId > self.successor[1]) or (nodeId < self.id and nodeId < self.successor[1]):
					return self.successor
				else:
					return this_node
		else:

			node = closestPrecedingNode(nodeId)
			if node[1] == self.id:
				return self.successor
			else:				
                
				#/* if this node couldn't find closest preciding node for given node id then now ask it's successor to do so */
				if node[1] == -1:
					node = self.successor
				
				ip=node[0][0]
				port=node[0][1]
				
				sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				
				try:
					sock.connect((ip, int(port)))
					msg = str(nodeId)
					sock.send(msg.encode('ascii'))
					ipAndPort = sock.recv(4096).decode('ascii')
					sock.close()
				except socket.error as e:
					print(str(e))

				if l < 0:
					node = [['',-1],-1]
					return node

				# /* set ip,port and hash for this node and return it */
				key = ipAndPort
				hash_code = helper.getHash(ipAndPort)
				ipAndPortPair = helper.getIpAndPort(key)

				return [ipAndPortPair, hash_code]


	def closestPrecedingNode(self, nodeId):
		for i in range(self.M,0,-1):
			if self.fingerTable[i][0][0] == "" or self.fingerTable[i][0][1] == -1 or self.fingerTable[i][1] == -1:
				continue

			if self.fingerTable[i][1] > self.id and self.fingerTable[i][1] < nodeId:
				return self.fingerTable[i]
			else:

				successorId = helper.getSuccessorId(self.fingerTable[i][0][0],self.fingerTable[i][0][1])

				if successorId == -1:
					continue

				if self.fingerTable[i][1] > successorId:
					if (nodeId <= self.fingerTable[i][1] and nodeId <= successorId) or (nodeId >= self.fingerTable[i][1] and nodeId >= self.successorId):
						return self.fingerTable[i]

				elif self.fingerTable[i][1] < successorId and nodeId > self.fingerTable[i][1] and nodeId < successorId:
					return self.fingerTable[i]


				predNode = helper.getPredecessorNode(self.fingerTable[i][0][0],self.fingerTable[i][0][1],"",-1,false)
				predecessorId = predNode[1]

				if predecessorId != -1 and self.fingerTable[i][1] < predecessorId:
					if (nodeId <= self.fingerTable[i][1] and nodeId <= predecessorId) or (nodeId >= self.fingerTable[i][1] and nodeId >= predecessorId):
						return predNode


				if predecessorId != -1 and self.fingerTable[i][1] > predecessorId and nodeId >= predecessorId and nodeId <= self.fingerTable[i][1]:
					return predNode

		node = [["",-1],-1]
		return node

	def fixFingers(self):
		next_node = 1
		mod = 2 ** M
		while next_node <= M:
			if helper.isNodeAlive(self.successor[0][0],self.successor[0][1]) == False:
				return

			newId = self.id + 2**(next_node-1)
			newId = newId % mod
			# pair< pair<string,int> , lli >
			node = self.findSuccessor(newId)
			if node[0][0] == "" or node[1] == -1 or node[0][1] == -1:
				break
			self.fingerTable[next_node] = node
			next_node+=1

	def stabilize(self):
		ownIp = self.getIpAddress()
		ownPort = self.getPortNumber()

		if helper.isNodeAlive(self.successor[0][0],self.successor[0][1]) == False:
			return

		#get predecessor of self.successor
		predNode = helper.getPredecessorNode(self.successor[0][0],self.successor[0][1],ownIp,ownPort,true)

		predecessorHash = predNode[1]

		if predecessorHash == -1 or self.predecessor[1] == -1:
			return

		if predecessorHash > self.id or (predecessorHash > self.id and predecessorHash < self.successor[1]) or (predecessorHash < self.id and predecessorHash < self.successor[1]):
			self.successor = predNode

	def notify(self, node):
		#get id of node and predecessor
		self.predecessorHash = self.predecessor[1]
		self.nodeHash = node[1]

		self.predecessor = node

		# if node's successor is node itself then set it's successor to this node
		if self.successor[1] == self.id:
			self.successor = node

	def checkPredecessor(self):
		if self.predecessor[1] == -1:
			return

		ip = self.predecessor[0][0]
		port = self.predecessor[0][1]

		if helper.isNodeAlive(ip,port) == False:
			#/* if node has same successor and self.predecessor then set node as it's successor itself */
			if self.predecessor[1] == self.successor[1]:

				self.successor[0][0] = self.getIpAddress()
				self.successor[0][1] = self.getPortNumber()
				self.successor[1] = self.id
				self.setSuccessorList(self.successor[0][0],self.successor[0][1],self.id)

			self.predecessor[0][0] = ""
			self.predecessor[0][1] = -1
			self.predecessor[1] = -1

	def checkSuccessor(self):
		if self.successor[1] == self.id:
			return

		ip , port = self.successor[0][0], self.successor[0][1]

		if helper.isNodeAlive(ip,port) == False:
			self.successor = self.successorList[2]
			self.updateSuccessorList()

	def updateSuccessorList(self):
		suc_list = helper.getSuccessorListFromNode(self.successor[0][0],self.successor[0][1])

		if len(suc_list) != R:
			return

		self.successorList[1] = self.successor

		for i in range(2,R+1):
			self.successorList[i][0][0] = suc_list[i-2][0]
			self.successorList[i][0][1] = suc_list[i-2][1]
			self.successorList[i][1] = helper.getHash(suc_list[i-2][0] + ":" + to_string(suc_list[i-2][1]))

	def printKeys(self):
		for item in self.dictionary:
			print (self.dictionary[item][0], self.dictionary[item][1])

	def storeKey(self, key, val):
		self.dictionary[key] = val

	def getAllKeysForSuccessor(self): #vector< pair<lli , string> >
		res=[]
		for item in self.dictionary:
			res.append([self.dictionary[item][0],self.dictionary[item][1]])
		self.dictionary={}
		return res

	def getKeysForPredecessor(self, nodeId):#vector< pair<lli , string> >
		res=[]
		for item in self.dictionary:
			keyId = item[0]

			#if predecessor's id is more than current node's id
			if self.id < nodeId:
				if keyId > id and keyId <= nodeId:
					res.append([keyId, self.dictionary[item][1]])

			#if predecessor's id is less than current node's id
			else:
				if keyId <= nodeId or keyId > self.id:
					res.append([keyId, self.dictionary[item][1]])
		self.dictionary = {}
		return res

	def setSuccessor(self, ip, port, hash_code):
		self.successor[0][0] = ip
		self.successor[0][1] = port
		self.successor[1] = hash_code

	def setSuccessorList(self, ip, port, hash_code):
		for i in range(1, 1 + R):
			self.successorList[i] = [[ip,port],hash_code]

	def setPredecessor(self, ip, port, hash_code):
		self.predecessor[0][0] = ip
		self.predecessor[0][1] = port
		self.predecessor[1] = hash_code

	def setFingerTable(self, ip, port, hash_code):
		for i in range(1, 1 + M):
			self.fingerTable[i] = [[ip,port],hash_code]

	def setId(self, id_code):
		self.id = id_code

	def setInRing(self):
		self.isInRing = True

	def getId(self):
		return self.id

	def getValue(self, key):
		if key in self.dictionary:
			return self.dictionary[key]
		else:
			return ""

	def getFingerTable(self):
		return self.fingerTable

	def getSuccessor(self):
		return self.successor

	def getPredecessor(self):
		return self.predecessor

	def getSuccessorList(self):
		return self.successorList

	def checkInRing(self):
		return self.isInRing

	def initiateSocket(self):
		'''
			create a socket at any ip = host and
			allow OS to bind it to any available port.
			set the length of the queue of the unaccepted requests = 128
			i.e. max as per -> /proc/sys/net/core/somaxconn
		'''
		self.soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		host, port = socket.gethostname(), 0
		try:
			self.soc.bind((host, port))
		except socket.error as e:
			print(str(e))
		self.soc.listen(128)

	def getNodeSock(self):
		return self.soc

	def getIpAddress(self):
		return self.soc.getsockname()[0]

	def getPortNumber(self):
		return self.soc.getsockname()[1]

	def closeSocket(self):
		self.soc.close()

	def intializeId(self):
		ip ,port = self.getIpAddress(), self.getPortNumber()
		key = ip + ":" + str(port)
		hashVal = helper.getHash(key)
		self.setId(hashVal);
