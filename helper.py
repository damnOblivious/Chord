import socket
import hashlib
import traceback
from variables import *

'''
	generates SHA1 of the given key and reduces it to M bits
	converts it to integer
	@return hash %= 2^M
'''
def getHash(key):
	sizeOfReqBits = int(M / 4)
	sha_1 = hashlib.sha1(key.encode('ascii'))
	hashHex = sha_1.hexdigest()[:sizeOfReqBits]
	hashOutput = int(hashHex, 16) % (2 ** M)
	if key==':-1':
		return -1
	return hashOutput

'''
	parameters: a string of the form "key1:val1;key2:val2;"
	@returns a vector of (key, value) pairs
'''
def seperateKeysAndValues(keysAndValues):
	res = []
	pairs = keysAndValues.split(";")[:-1]       # [:-1] to avoid last part ''
	for pair in pairs:
		pair = pair.split(":")
		res.append([pair[0], pair[1]])
	return res

'''
	node receives all keys from it's predecessor who is
	leaving the ring and adds to it's dictionary
'''
def storeAllKeys(myNode, keysAndValues):

	keysAndValues = keysAndValues[:-len('storeKeys')]
	
	res = seperateKeysAndValues(keysAndValues)
	for pair in res:
		myNode.storeKey(pair[0],pair[1])

'''
	finds succesor of the given key
	sends pi,port to the contacting node
	@parameters key
	@return void
'''
def sendSuccessor(myNode, hashVal, clientConnection):

	if 'SendSuccessorForThisKeyJoin' in hashVal:
		hashVal = hashVal[:-len('SendSuccessorForThisKeyJoin')]
	if 'SendSuccessorForThisKey' in hashVal:
		hashVal = hashVal[:-len('SendSuccessorForThisKey')]
	

	hashVal = int(hashVal)
	succesorNode = myNode.findSuccessor(hashVal)
	ipAndPort = succesorNode[0][0] + ":" + str(succesorNode[0][1])
	msg = ipAndPort
	socket_reply(msg, clientConnection)

'''
	send msg to the succesor asking for (key,val) pairs that belongs to me
	receives those
	and store in the dictionary
'''
def getKeysFromSuccessor(myNode, ip, port):

	msg = "getKeys:" + str(myNode.getId())
	keysAndValues = socket_send_recv(ip, port, msg, "")
	res = seperateKeysAndValues(keysAndValues)
	for pair in res:
		myNode.storeKey(pair[0],pair[1])


def getRepsFromSuccessor(myNode, ip, port):

	msg = "getReplicas:" + str(myNode.getId())
	keysAndValues = socket_send_recv(ip, port, msg, "")
	res = seperateKeysAndValues(keysAndValues)
	for pair in res:
		myNode.storeRepKey(pair[0],pair[1])


def getRecoveryKeysFromSuccessor(myNode, ip, port):

	msg = "getRecoveryKeys:" + str(myNode.getId())
	keysAndValues = socket_send_recv(ip, port, msg, "")
	res = seperateKeysAndValues(keysAndValues)
	for pair in res:
		myNode.storeKey(pair[0],pair[1])

def sendValToNode(nodeInfo, client, nodeIdString):
	nodeIdString = nodeIdString[:-len("GetThisKey")]
	key = int(nodeIdString)
	val = nodeInfo.getValue(key)
	msg = val
	socket_reply(msg, client)

def sendRepValToNode(nodeInfo, client, nodeIdString):
	nodeIdString = nodeIdString[:-len("GetThisReplica")]
	key = int(nodeIdString)
	val = nodeInfo.checkReplica(key)
	msg = val
	socket_reply(msg, client)
	

def getSuccessorId(ip, port):

	msg = "finger"
	succIdChar = socket_send_recv(ip, port, msg, 0)
	
	return int(succIdChar)

def getPredecessorNode(ip, port, ipClient, portClient, forStabilize):
	msg = ""

	# /* p2 means that just send predecessor of node ip:port , do not call notify */
	# /* p1 means that this is for stabilize so notify node as well */

	if forStabilize == True:
		msg = combineIpAndPort(ipClient,str(portClient))
		msg += "GetPredecessorNotify"

	else:
		msg = "GetPredecessor"

	ipAndPort = socket_send_recv(ip, port, msg, '')
	node = [['',-1],-1]
	if ipAndPort != ':-1' and ipAndPort!='' and ipAndPort!='-1':

		ipAndPortPair = getIpAndPort(ipAndPort)

		node = [ipAndPortPair, getHash(ipAndPort)]

	return node


def getSuccessorListFromNode(ip, port):

	msg = "sendSuccList"
	succList = socket_send_recv(ip, port, msg, [])
	if len(succList) == 0: # no response
		return []

	successorList = seperateSuccessorList(succList)
	return successorList
#doubt
def sendSuccessorList(nodeInfo, client):

	successorList = nodeInfo.getSuccessorList()
	# print(len(successorList))
	successorList = splitSuccessorList(successorList)
	msg = successorList
	socket_reply(msg, client)

def isNodeAlive(ip, port):
	msg = "alive"
	res = socket_send_recv(ip, port, msg, False)
	if res == False:
		return False
	return True


def combineIpAndPort(ip, port):
	ipAndPort = str(ip)+':'+str(port)
	return ipAndPort

def getIpAndPort(key):
	split = key.split(':')
	if len(split)==2:
		ip, port = key.split(':')
		return [ip,int(port)]
	elif len(split)==1:
		if '0' in key:
			return ['',0]
		else:
			return ['',-1]


def seperateSuccessorList(succList):
	addresses = succList.split(';')
	res=[]
	for key in addresses[:-1]:
		res.append(getIpAndPort(key))
	return res

def splitSuccessorList(succList):
	
	res = ""
	for key in succList:
		res = res + key[0][0] + ':' + str(key[0][1]) + ';'
	return res


def sendTest(clientConnection):
	msg="This is test"
	socket_reply(msg, clientConnection)


def getTest(ip, port):
	msg = "sendTest"

	succList = socket_send_recv(ip, port, msg, '')

	if len(succList) == 0: # no response
		return []


# send ack to contacting node that this node is still alive 
def sendAcknowledgement(clientConnection):
	msg="1"
	socket_reply(msg, clientConnection)


# send ip:port of predecessor of current node to contacting node 
def sendPredecessor(nodeInfo, clientConnection):
	
	predecessor = nodeInfo.getPredecessor()
	ip = predecessor[0][0]
	port = str(predecessor[0][1])
	msg=''
	ipAndPort = combineIpAndPort(ip,port)
	msg = ipAndPort
	socket_reply(msg, clientConnection)
	

# send successor id of current node to the contacting node
def sendSuccessorId(nodeInfo, clientConnection):

	succ = nodeInfo.getSuccessor()
	msg = str(succ[1])
	socket_reply(msg, clientConnection)


# send all keys to the newly joined node which belong to it now 
def sendNeccessaryKeys(nodeInfo, clientConnection, nodeIdString):
	nodeId = int(nodeIdString.split(':')[1])

	keysAndValuesVector = nodeInfo.getKeysForPredecessor(nodeId)

	keysAndValues = ""

	# will arrange all keys and val in form of key1:val1;key2:val2;
	for key_val in keysAndValuesVector:
		keysAndValues += str(key_val[0]) + ":" + str(key_val[1]) + ';'

	msg=keysAndValues
	socket_reply(msg, clientConnection)

def sendRecoveryKeys(nodeInfo, clientConnection, nodeIdString):
	nodeId = int(nodeIdString.split(':')[1])

	keysAndValuesVector = nodeInfo.getKeysForRecovery(nodeId)

	keysAndValues = ""

	# will arrange all keys and val in form of key1:val1;key2:val2;
	for key_val in keysAndValuesVector:
		keysAndValues += str(key_val[0]) + ":" + str(key_val[1]) + ';'

	msg=keysAndValues
	socket_reply(msg, clientConnection)


def sendNeccessaryReplicas(nodeInfo, clientConnection, nodeIdString):
	nodeId = int(nodeIdString.split(':')[1])

	keysAndValues = ""

	# will arrange all keys and val in form of key1:val1;key2:val2;
	for key in nodeInfo.dictionary_rep:
		keysAndValues += str(key) + ":" + str(nodeInfo.dictionary_rep[key]) + ';'

	msg=keysAndValues
	socket_reply(msg, clientConnection)
	


# send key to node who requested for it
def sendKeyToNode(node, keyHash, value):

	ip = node[0][0]
	port = int(node[0][1])
	keyAndVal = str(keyHash)+':'+str(value)+"StoreThisKey"
	msg = keyAndVal
	res = socket_send_recv(ip, port, msg, '')


#  will contact a node and get value of a particular key from that node 
def getKeyFromNode(node, keyHash):
	ip = node[0][0];
	port = int(node[0][1])

	keyHash += "GetThisKey";
	msg = keyHash
	res = socket_send_recv(ip, port, msg, '')

	return res

def getKeyFromNodeReplica(node, keyHash):
	ip = node[0][0];
	port = int(node[0][1])

	keyHash += "GetThisReplica";
	msg = keyHash
	res = socket_send_recv(ip, port, msg, '')

	return res

def socket_send_recv(ip, port, msg, no_res):
	if int(port) < 0 or int(port ) > 65535:
		return no_res

	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	try:
		sock.connect((ip, int(port)))
		sock.send(msg.encode('ascii'))
		res = sock.recv(4096).decode('ascii')
		sock.close()
	except socket.error as e:
		# print(str(e))
		print('Node has exited')
		return no_res

	return res

def socket_reply(msg, clientConnection):
	try:
		clientConnection.send(msg.encode('ascii'))
	except socket.error as e:
		print(str(e))

#  will decide if id is in form of key:value or not 
def isKeyValue(key_val):

	if ':' not in key_val:
		return False
	key, val = key_val.split(':')
	for i in key:
		if int(i) <0 or int(i)>9:
			return False

	return True


# key will be in form of key:value , will seperate key and value and return it 
def getKeyAndVal(keyAndVal, message):
	keyAndVal = keyAndVal[:-len(message)]

	key, val = keyAndVal.split(':')
	key = int(key)
	return [key,val]
