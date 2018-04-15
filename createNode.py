import sys
import time
import socket
import threading
import functions
from node import Node

def isBlank(st):
    return not (st and st.strip())

newNode = Node();
print("New node created, listening at port number " + str(newNode.getPortNumber()))
print("Type help to know more")

th = threading.Thread(target=functions.create, args=(newNode,))
th.daemon = True
th.start()

command = ''
while True:
    while(isBlank(command)):
        command = input(">>> " )
    arguments = command.split()

    if len(arguments) == 1:
        if arguments[0] == "create":
            if newNode.checkInRing():
                print("The node is already in a ring, so can't create a new ring with it\n")
            else:
                print("----------------------------->creating")
                th = threading.Thread(target=functions.create, args=(newNode,))
                th.daemon = True
                th.start()


    command = ''
