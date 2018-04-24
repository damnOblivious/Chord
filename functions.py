import sys
import time
import socket
import threading

import helper

'''
    intializes a node and all it's variables
    spawns 2 threads: one to listenTo other nodes and
    to other to doStabilization
'''
def create(myNode):
    ip ,port, idVal = myNode.getIpAddress(), myNode.getPortNumber(), myNode.getId()
    myNode.setSuccessor(ip, port, idVal)
    myNode.setSuccessorList(ip, port, idVal)
    myNode.setPredecessor("", -1, -1)
    myNode.setFingerTable(ip, port, idVal)
    myNode.setInRing()

    # launch threads,one thread will listen to request from other nodes,one will do stabilization
    second = threading.Thread(target=listenTo, args=(myNode,))
    second.daemon = True
    second.start()

    # fifth = threading.Thread(target=doStabilize, args=(myNode,))
    # fifth.daemon = True
    # fifth.start()

'''
    sends requests a node of the ring to find it's successor
    joins the node to the ring
    @parameters currentNode, address of the node of the ring to be connected
'''
def join(myNode, ip, port):
    '''ip should be socket.gethostname() [in my case oblivious]'''

    # if(helper.isNodeAlive(ip,atoi(port.c_str())) == False:
    #     print("Sorry but no node is active on this ip or port")
    #     return
    myId = myNode.getId()

    newConnection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print("about to connect to", ip, port)
    try:
        newConnection.connect((ip, int(port)))
        newConnection.send(str(myId).encode('ascii'))
        ipAndPort = newConnection.recv(1024).decode('ascii')
        newConnection.close()
    except socket.error as e:
        print(str(e))

    print("Successfully joined the ring")
    print(ipAndPort)

    address = ipAndPort.split(":")
    idVal, port, hashVal = address[0], int(address[1]), helper.getHash(ipAndPort)
    myNode.setSuccessor(ip, port, idVal)
    myNode.setSuccessorList(ip, port, idVal)
    myNode.setPredecessor("", -1, -1)
    myNode.setFingerTable(ip, port, idVal)
    myNode.setInRing()

    helper.getKeysFromSuccessor(myNode , ip, port)

    # launch threads,one thread will listen to request from other nodes,one will do stabilization
    second = threading.Thread(target=listenTo, args=(myNode,))
    second.daemon = True
    second.start()

    # fifth = threading.Thread(target=doStabilize, args=(myNode,))
    # fifth.daemon = True
    # fifth.start()


'''
    a separate thread to listen to all the messages by other nodes.
    on arrival of a msg it -
    creates a new thread to handle that request and continues listen to others
'''
def listenTo(myNode):
    # print("going to listten")
    while(True):
        clientConnection, clientAddress = myNode.soc.accept()
        print("JUST GOT A MSG")
        msg = clientConnection.recv(1024).decode('ascii')
        f = threading.Thread(target=doTask, args=(myNode, clientConnection, clientAddress, msg))
        f.daemon = True
        f.start()

'''
    each thread of listenTo is assigned a task to perform based on the msg
    this function contains all the possible task that can be asked in a msg
'''
def doTask(myNode, clientConnection, clientAddress, msg):
    # predecessor of this node has left the ring and has sent all it's keys to this node(it's successor)
    if msg.find("storeKeys") != -1:
        helper.storeAllKeys(myNode, msg)
        
    elif msg.find("sendTest") != -1:
        helper.sendTest(clientConnection)

    # check if the sent msg is in form of key:val, if yes then store it in current node (for put )
    elif helper.isKeyValue(msg):
        keyAndVal = helper.getKeyAndVal(msg)
        myNode.storeKey(keyAndVal[0], keyAndVal[1])
        clientConnection.send("1".encode('ascii'))
    
    elif msg.find("alive") != -1:
        helper.sendAcknowledgement(client)
    
    # contacting node wants successor list of this node
    elif msg.find("sendSuccList") != -1:
        helper.sendSuccessorList(myNode, client)
    
    # contacting node has just joined the ring and is asking for keys that belongs to it now
    elif msg.find("getKeys") != -1:
        helper.sendNeccessaryKeys(myNode,client,msg)
    
    # contacting node has run get command so send value of key it requires
    elif msg.find("k") != -1:
        helper.sendValToNode(myNode,client,msg)
    
    # contacting node wants the predecessor of this node
    elif msg.find("p") != -1:
        helper.sendPredecessor(myNode,client)
        # p1 in msg means that notify the current node about this contacting node
    
        if msg.find("p1") != -1:
            callNotify(myNode,msg)
    
    # contacting node wants successor Id of this node for help in finger table
    elif msg.find("finger") != -1:
        helper.sendSuccessorId(myNode,client)
    
    # contacting node wants current node to find successor for it
    else:
        helper.sendSuccessor(myNode, int(msg), clientConnection)
    
    clientConnection.close()

'''
    called on a node before leaving
    transfers all the keys to it's succesor
    and closes the socket associated with the node
'''
def leave(myNode):
    successor = myNode.getSuccessor()
    if myNode.getId() == successor[1]:
        return
    keysAndValuesVector = myNode.getAllKeysForSuccessor()
    if len(keysAndValuesVector) == 0:
        return
    keysAndValues = ""
    '''arrange all keys and val in form of key1:val1;key2:val2;'''
    for item in keysAndValuesVector:
        keysAndValues += str(keysAndValuesVector[i][0]) + ":" + keysAndValuesVector[i][1] + ""
    keysAndValues += "storeKeys"

    newConnection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        newConnection.connect((successor[0][0], successor[0][1]))
        newConnection.send(keysAndValues.encode('ascii'))
        newConnection.close()
    except socket.error as e:
        print(str(e))
    myNode.closeSocket()

'''
    displays the information that a node has. Prints
    - the details of socket(ip, port, id)
    - it's successor, predecessor, successor-list and finger-table.
'''
def printState(myNode):
    successor = myNode.getSuccessor()
    predecessor = myNode.getPredecessor()
    fingerTable = myNode.getFingerTable()
    successorList = myNode.getSuccessorList()

    print("Self: ipAddr =", myNode.getIpAddress(), "port =", myNode.getPortNumber(), "id =", myNode.getId())
    print("Successor: ipAddr =", successor[0][0], "port =", successor[0][1], "id =", successor[1])
    print("Predecessor: ipAddr =", predecessor[0][0], "port =", predecessor[0][1], "id =", predecessor[1])

    for index, node in enumerate(fingerTable):
        print("Finger[" + str(index) + "]: ipAddr = ", node[0][0], "port =", node[0][1], "id =", node[1])
    for index, node in enumerate(successorList):
        print("Successor[" + str(index) + "]: ipAddr = ", node[0][0], "port =", node[0][1], "id =", node[1])

'''
    displays the list of commands and their usages
'''
def showHelp():
    print("1) create : will create a DHT ring")
    print("2) join <ip> <port> : will join ring by connecting to main node having ip and port")
    print("3) printstate : will print successor, predecessor, fingerTable and Successor list")
    print("4) print : will print all keys and values present in that node")
    print("5) port : will display port number on which node is listening")
    print("6) port <number> : will change port number to mentioned number if that port is free")
    print("7) put <key> <value> : will put key and value to the node it belongs to")
    print("8) get <key> : will get value of mentioned key")




def put(key, value, nodeInfo):
    if key == "" or value == "":
        print("Key or value field empty")
        return
    else:

        keyHash = help.getHash(key)
        print("Key is ", key, " and hash : ", keyHash)

        node = nodeInfo.findSuccessor(keyHash)

        help.sendKeyToNode(node,keyHash,value)

        print("key entered successfully")

def get(key, nodeInfo):

    if key == "":
        print("Key field empty")
        return 
    else:
        
        keyHash = help.getHash(key)

        node = nodeInfo.findSuccessor(keyHash)

        val = help.getKeyFromNode(node,str(keyHash))

        if val == "":
            print("Key Not found")

        else:
            print("Found ",key," : ",val)



def printState(nodeInfo):
    ip = nodeInfo.getIpAddress()
    my_id = nodeInfo.getId()
    port = nodeInfo.getPortNumber()
    fingerTable = nodeInfo.getFingerTable()
    print("Self ", ip, " ", port, " "<<my_id)
    succ = nodeInfo.getSuccessor()
    pre = nodeInfo.getPredecessor()
    succList = nodeInfo.getSuccessorList()
    print("Succ ", succ[0][0], " ", succ[0][1], " ", succ[1])<<endl

    print("Pred ",pre[0][0]," ",pre[0][1]," ",pre[1])
    for i in range(1,M+1):
        ip = fingerTable[i][0][0]
        port = fingerTable[i][0][1]
        this_id = fingerTable[i][1]
        print("Finger[",i,"] ",this_id," ",ip," ",port)
    
    for i in range(1,R+1):
        ip = succList[i][0][0]
        port = succList[i][0][1]
        this_id = succList[i][1]
        print("Successor[",i,"] ",this_id," ",ip," ",port)


def  doStabilize(nodeInfo):

    while True:

        nodeInfo.checkPredecessor()

        nodeInfo.checkSuccessor()

        nodeInfo.stabilize()

        nodeInfo.updateSuccessorList()

        nodeInfo.fixFingers()

        time.sleep(3)
   


def callNotify(nodeInfo, ipAndPort):

    ipAndPort = ipAndPort[2:]
    #.pop_back()
    #ipAndPort.pop_back()

    ipAndPortPair = help.getIpAndPort(ipAndPort)
    ip = ipAndPortPair[0]
    port = ipAndPortPair[1]
    hash_code = help.getHash(ipAndPort)

    node = [[ip,port],hash_code]

    nodeInfo.notify(node)
