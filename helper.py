import socket
import hashlib

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
    keysAndValues = keysAndValues[:-len(storeKeys)]
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
    succesorNode = [['oblivious', 6556], 656565]
    # succesorNode = myNode.findSuccessor(hashVal)
    ipAndPort = succesorNode[0][0] + ":" + str(succesorNode[0][1])
    clientConnection.send(ipAndPort.encode('ascii'))
    clientConnection.close()

'''
    send msg to the succesor asking for (key,val) pairs that belongs to me
    receives those
    and store in the dictionary
'''
def getKeysFromSuccessor(myNode, ip, port):
    print("about to connect to in getKeysFromSuccessor", ip, port)

    newConnection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        newConnection.connect((ip, int(port)))
        msg = "getKeys:" + myNode.getId()
        newConnection.send(msg.encode('ascii'))
        # keysAndValues = newConnection.recv(4096).decode('ascii') #NOTE MODULE TESTING PHASE
        newConnection.send(msg.encode('ascii'))
        newConnection.close()
    except socket.error as e:
        print(str(e))

    keysAndValues = "key1:val1;key2:val2;key3:val3;key4:val4;" #NOTE MODULE TESTING PHASE
    res = seperateKeysAndValues(keysAndValues)
    print(res)
    for pair in res:
        myNode.storeKey(pair[0],pair[1])




def sendValToNode(nodeInfo, client, nodeIdString):
    key = int(nodeIdString)
    val = nodeInfo.getValue(key)

    try:
        client.send(val.encode('ascii'))
        client.close()
    except socket.error as e:
        print(str(e))


def getSuccessorId(ip, port):
    
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        sock.connect((ip, int(port)))
        msg = "finger"
        sock.send(msg.encode('ascii'))
        succIdChar = sock.recv(4096).decode('ascii') 
        sock.close()
    except socket.error as e:
        print(str(e))

    return int(succIdChar)

def getPredecessorNode(ip, port, ipClient, portClient, forStabilize):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    msg = ""

    # /* p2 means that just send predecessor of node ip:port , do not call notify */
    # /* p1 means that this is for stabilize so notify node as well */

    if forStabilize == True:
        msg = combineIpAndPort(ipClient,str(portClient))
        msg += "p1"

    else:
        msg = "p2"


    ipAndPort=''
    try:
        sock.connect((ip, int(port)))
        msg = "finger"
        sock.send(msg.encode('ascii'))
        ipAndPort = sock.recv(4096).decode('ascii') 
        sock.close()
    except socket.error as e:
        print(str(e))

    node=[['',-1],-1]
    
    if ipAndPort == None:# no response
        node=[['',-1],-1]
        return node

    if len(ipAndPort) == 0: # no response
        node=[['',-1],-1]
        return node

    ipAndPortPair = getIpAndPort(ipAndPort)

    node = [ipAndPortPair, getHash(ipAndPort)]

    return node


def getSuccessorListFromNode(ip, port):

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((ip, int(port)))
        msg = "sendSuccList"
        sock.send(msg.encode('ascii'))
        succList = sock.recv(4096).decode('ascii')
        sock.close()
    except socket.error as e:
        print(str(e))

    if succList == None:# no response
        return []

    if len(succList) == 0: # no response
        return []

    successorList = seperateSuccessorList(succList)

    return successorList
#doubt
def sendSuccessorList(nodeInfo, client):

    successorList = nodeInfo.getSuccessorList()
    successorList = splitSuccessorList(successorList)
    try:
        msg = successorList
        client.send(msg.encode('ascii'))
        client.close()
    except socket.error as e:
        print(str(e))

def isNodeAlive(ip, port):

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((ip, int(port)))
        msg = "alive"
        sock.send(msg.encode('ascii'))
        response = sock.recv(4096).decode('ascii')
        sock.close()
    except socket.error as e:
        return False

    if succList == None:# no response
        return False

    if len(succList) == 0: # no response
        return False

    return True


def combineIpAndPort(ip, port):
    ipAndPort = str(ip)+':'+str(port)
    return ipAndPort

def getIpAndPort(key):
    ip, port = key.split(':')
    return [ip,int(port)]


def seperateSuccessorList(succList):
    addresses = succList.split(';')
    res=[]
    for key in addresses:
        res.append(getIpAndPort(key))
    return res

def splitSuccessorList(succList):
    res = ""
    for key in succList:
        res = res + key[0][0] + ':' + int(key[0][1]) + ';'

    return res