import sys
import time
import socket
import threading
from vars import *

class Node(object):
	def __init__(self):
		self.initiateSocket()
		self.fingerTable = [[["", 0], 0]] * (M + 1)
		self.successorList = [[["", 0], 0]] * (R + 1)
		self.isInRing = False


	#skipped   def findSuccessor(self, nodeId):# pair< pair<string,int> , lli >

		#lli nodeId


	#skipped	def closestPrecedingNode(lli nodeId):#pair< pair<string,int> , lli >

	def fixFingers():
		help_er=HelperFunctions();
		next_node = 1;
		mod = 2**M
		while next_node <= M:
			if help.isNodeAlive(successor[0][0],successor[0][1]) == False:
				return;

			newId = self.id + 2**(next_node-1)
			newId = newId % mod;
			# pair< pair<string,int> , lli >
			node = self.findSuccessor(newId);
			if node[0][0] == "" or node[1] == -1 or node[0][1] == -1:
				break
			self.fingerTable[next_node] = node;
			next_node+=1;

	def stabilize():
		help_er = HelperFunctions()
		ownIp = self.sp.getIpAddress()
		ownPort = self.sp.getPortNumber()

		if help_er.isNodeAlive(self.successor[0][0],self.successor[0][1]) == False:
			return

		#get predecessor of self.successor
		#pair< pair<string,int> , lli >
		predNode = help_er.getPredecessorNode(self.successor[0][0],self.successor[0][1],ownIp,ownPort,true)

		predecessorHash = predNode[1]

		if predecessorHash == -1 or predecessor[1] == -1:
			return

		if predecessorHash > self.id or (predecessorHash > self.id and predecessorHash < self.successor[1]) or (predecessorHash < self.id and predecessorHash < self.successor[1]):
			self.successor = predNode

	def notify(node):
		#get id of node and predecessor
		self.predecessorHash = predecessor[1];
		self.nodeHash = node[1];

		self.predecessor = node;

		# if node's successor is node itself then set it's successor to this node
		if self.successor[1] == self.id:
			self.successor = node;

	def checkPredecessor():
		if self.predecessor[1] == -1:
			return;

		help_er = HelperFunctions()
		ip = self.predecessor[0][0];
		port = self.predecessor[0][1];

		if help_er.isNodeAlive(ip,port) == False:
			#/* if node has same successor and self.predecessor then set node as it's successor itself */
			if self.predecessor[1] == self.successor[1]:

				self.successor[0][0] = self.sp.getIpAddress();
				self.successor[0][1] = self.sp.getPortNumber();
				self.successor[1] = self.id;
				self.setSuccessorList(self.successor[0][0],self.successor[0][1],self.id);

			self.predecessor[0][0] = "";
			self.predecessor[0][1] = -1;
			self.predecessor[1] = -1;

	def checkSuccessor():
		if successor[1] == id:
			return;

		help_er = HelperFunctions()
		ip = successor[0][0];
		port = successor[0][1];

		if help_er.isNodeAlive(ip,port) == False:
			successor = successorList[2];
			self.updateSuccessorList();

	def updateSuccessorList():

		help_er = HelperFunctions()

		#vector< pair<string,int> >
		suc_list = help_er.getSuccessorListFromNode(self.successor[0][0],self.successor[0][1])

		if len(suc_list) != R:
			return

		successorList[1] = self.successor

		for i in range(2,R+1):
			successorList[i][0][0] = suc_list[i-2][0]
			successorList[i][0][1] = suc_list[i-2][1]
			successorList[i][1] = help_er.getHash(suc_list[i-2][0] + ":" + to_string(suc_list[i-2][1]))

	def printKeys():
		for item in dictionary:
			print (item[0], item[1])

	def storeKey(key, val):
		self.dictionary[key] = val

	def getAllKeysForSuccessor(): #vector< pair<lli , string> >
		# vector< pair<lli , string> > res
		res=[]
		for item in self.dictionary[:]:
			res.append([item[0],item[1]])
			self.dictionary.remove(item)

		return res
	def getKeysForPredecessor(nodeId):#vector< pair<lli , string> >
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

	def setSuccessor(ip, port, hash_code):
		#string ip,int port,lli hash
		self.successor[0] = ip
		self.successor[0][1] = port
		self.successor[1] = hash_code
	def setSuccessorList(ip, port, hash_code):
		#string ip,int port,lli hash
		for i in range(1, 1 + R):
			self.successorList[i] = [[ip,port],hash_code]

	def setPredecessor(ip, port, hash_code):
		#string ip,int port,lli hash_code
		self.predecessor[0][0] = ip
		self.predecessor[0][1] = port
		self.predecessor[1] = hash_code

	def setFingerTable(ip, port, hash):
		for i in range(1,1+M):
			self.fingerTable[i] = [[ip,port],hash_code]

	def setId(id_code):
		self.id = id_code

	def setStatus():
		self.isInRing = True

	def getId(): #lli
		return self.id;

	def getValue(key): #string
		if key in dictionary:
			return dictionary[key];
		else:
			return ""

	def getFingerTable(): #vector< pair< pair<string,int> , lli > >
		return self.fingerTable

	def getSuccessor(): # pair< pair<string,int> , lli >
		return self.successor

	def getPredecessor(): # pair< pair<string,int> , lli >
		return self.predecessor

	def getSuccessorList():#vector< pair< pair<string,int> , lli > >
		return self.successorList

	def getStatus(): #bool
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
