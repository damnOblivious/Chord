import sys
import time
import socket
import threading
import functions
import helper
from node import Node

def isBlank(st):
    return not (st and st.strip())

newNode = Node()
print("Running at " + str(newNode.getIpAddress())+' '+ str(newNode.getPortNumber()))

endLoop = False
command = ''

while not endLoop:
    while(isBlank(command)):
        command = input(">>> " )
    arguments = command.split()

    if len(arguments) == 1:
        if arguments[0] == "create":
            if newNode.checkInRing():
                print("The node is already in a ring, so can't create a new ring with it")
            else:
                print("creating")
                first = threading.Thread(target=functions.create, args=(newNode,))
                first.daemon = True
                first.start()
                print("Done!")
        elif arguments[0] == "printstate":
            if not newNode.checkInRing():
                print("The current node is not associated to any ring")
            else:
                functions.printState(newNode)
        elif arguments[0] == "printkeys":
            if not newNode.checkInRing():
                print("The current node is not associated to any ring")
            else:

                newNode.printKeys()
                print("Replicated Keys:")
                newNode.printRepKeys()

        elif arguments[0] == "leave":
            functions.leave(newNode)
            endLoop = True
        elif arguments[0] == "port":
            print(str(newNode.getPortNumber()))
        elif arguments[0] == "help":
            functions.showHelp()
        elif arguments[0] == "test":
            functions.showHelp()
        else:
            print("Invalid Command")

    elif len(arguments) == 2:
        if arguments[0] == "get":
            if newNode.checkInRing() == False:
                print("Sorry this node is not in the ring")
            else:
                functions.get(arguments[1], newNode)

    elif len(arguments) == 3:
        if arguments[0] == "join":
            if newNode.checkInRing():
                print("The node is already in a ring, so can't add it to a new ring\n")
            else:
                try:
                    temp = int(arguments[2])
                except ValueError:
                    print("Please enter an integer")
                functions.join(newNode, arguments[1], arguments[2])
        elif arguments[0] == "test":
            try:
                temp = int(arguments[2])
            except ValueError:
                print("Please enter an integer")
            helper.getTest(arguments[1], arguments[2])

        elif arguments[0] == "put":
            if newNode.checkInRing() == False:
                print("Sorry this node is not in the ring")
            else:
                functions.put(str(arguments[1]),str(arguments[2]), newNode)
        else:
            print("Invalid Command")


    command = ''
