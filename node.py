import sys
import time
import socket
import threading
from variables import *
import helper


class Node(object):
	def __init__(self):
		self.initiateSocket()
		self.fingerTable = [[["", 0], 0]] * (M + 1)
		self.successorList = [[["", 0], 0]] * (R + 1)
		self.isInRing = False
		self.id=0
		self.predecessor= [["", 0], 0]
		self.successor = [["", 0], 0]
		self.dictionary={};



	#skipped   def findSuccessor(self, nodeId):# pair< pair<string,int> , lli >

		#lli nodeId


	#skipped	def closestPrecedingNode(lli nodeId):#pair< pair<string,int> , lli >

	def fixFingers(self):
		next_node = 1;
		mod = 2**M
		while next_node <= M:
			if helper.isNodeAlive(successor[0][0],successor[0][1]) == False:
				return;

			newId = self.id + 2**(next_node-1)
			newId = newId % mod;
			# pair< pair<string,int> , lli >
			node = self.findSuccessor(newId);
			if node[0][0] == "" or node[1] == -1 or node[0][1] == -1:
				break
			self.fingerTable[next_node] = node;
			next_node+=1;

	def stabilize(self):
		ownIp = self.sp.getIpAddress()
		ownPort = self.sp.getPortNumber()

		if helper.isNodeAlive(self.successor[0][0],self.successor[0][1]) == False:
			return

		#get predecessor of self.successor
		#pair< pair<string,int> , lli >
		predNode = helper.getPredecessorNode(self.successor[0][0],self.successor[0][1],ownIp,ownPort,true)

		predecessorHash = predNode[1]

		if predecessorHash == -1 or predecessor[1] == -1:
			return

		if predecessorHash > self.id or (predecessorHash > self.id and predecessorHash < self.successor[1]) or (predecessorHash < self.id and predecessorHash < self.successor[1]):
			self.successor = predNode

	def notify(self, node):
		#get id of node and predecessor
		self.predecessorHash = predecessor[1];
		self.nodeHash = node[1];

		self.predecessor = node;

		# if node's successor is node itself then set it's successor to this node
		if self.successor[1] == self.id:
			self.successor = node;

	def checkPredecessor(self):
		if self.predecessor[1] == -1:
			return;

		ip = self.predecessor[0][0];
		port = self.predecessor[0][1];

		if helper.isNodeAlive(ip,port) == False:
			#/* if node has same successor and self.predecessor then set node as it's successor itself */
			if self.predecessor[1] == self.successor[1]:

				self.successor[0][0] = self.sp.getIpAddress();
				self.successor[0][1] = self.sp.getPortNumber();
				self.successor[1] = self.id;
				self.setSuccessorList(self.successor[0][0],self.successor[0][1],self.id);

			self.predecessor[0][0] = "";
			self.predecessor[0][1] = -1;
			self.predecessor[1] = -1;

	def checkSuccessor(self):
		if successor[1] == id:
			return;

		ip = successor[0][0];
		port = successor[0][1];

		if helper.isNodeAlive(ip,port) == False:
			successor = successorList[2];
			self.updateSuccessorList();

	def updateSuccessorList(self):

		#vector< pair<string,int> >
		suc_list = helper.getSuccessorListFromNode(self.successor[0][0],self.successor[0][1])

		if len(suc_list) != R:
			return

		successorList[1] = self.successor

		for i in range(2,R+1):
			successorList[i][0][0] = suc_list[i-2][0]
			successorList[i][0][1] = suc_list[i-2][1]
			successorList[i][1] = helper.getHash(suc_list[i-2][0] + ":" + to_string(suc_list[i-2][1]))

	def printKeys(self):
		for item in dictionary:
			print (item[0], item[1])

	def storeKey(self, key, val):
		self.dictionary[key] = val

	def getAllKeysForSuccessor(self): #vector< pair<lli , string> >
		# vector< pair<lli , string> > res
		res=[]
		for item in self.dictionary[:]:
			res.append([item[0],item[1]])
			self.dictionary.remove(item)
		return res

	def getKeysForPredecessor(self, nodeId):#vector< pair<lli , string> >
		res=[]
		for item in self.dictionary[:]:
			keyId = item[0]

			#if predecessor's id is more than current node's id
			if self.id < nodeId:
				if keyId > id and keyId <= nodeId:
					res.append([keyId, item[1]])
					self.dictionary.remove(item)

			#if predecessor's id is less than current node's id
			else:
				if keyId <= nodeId or keyId > self.id:
					res.append([keyId, item[1]])
					self.dictionary.remove(item)

		return res

	def setSuccessor(self, ip, port, hash_code):
		#string ip,int port,lli hash
		self.successor[0][0] = ip
		self.successor[0][1] = port
		self.successor[1] = hash_code

	def setSuccessorList(self, ip, port, hash_code):
		#string ip,int port,lli hash
		for i in range(1, 1 + R):
			self.successorList[i] = [[ip,port],hash_code]

	def setPredecessor(self, ip, port, hash_code):
		#string ip,int port,lli hash_code
		self.predecessor[0][0] = ip
		self.predecessor[0][1] = port
		self.predecessor[1] = hash_code

	def setFingerTable(self, ip, port, hash_code):
		for i in range(1,1+M):
			self.fingerTable[i] = [[ip,port],hash_code]

	def setId(self, id_code):
		self.id = id_code

	def setInRing(self):
		self.isInRing = True
	def setStatus(self):
		self.isInRing = True;

	def getId(self): #lli
		return self.id;

	def getValue(self, key): #string
		if key in dictionary:
			return dictionary[key];
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

	def getIpAddress(self):
		return self.soc.getsockname()[0]

	def getPortNumber(self):
		return self.soc.getsockname()[1]

	def closeSocket(self):
		self.soc.close()
