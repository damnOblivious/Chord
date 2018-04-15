import sys
import time
import socket
import threading

import helper


def create(newNode):
    print("im in create funciton")

    ip ,port = newNode.getIpAddress(), newNode.getPortNumber()
    key = ip + ":" + str(port)
    hashVal = helper.getHash(key)
    newNode.setId(hashVal);
    newNode.setSuccessor(ip, port, hashVal);
    newNode.setSuccessorList(ip, port, hashVal);
    newNode.setPredecessor("", -1, -1);
    newNode.setFingerTable(ip, port, hashVal);
    newNode.setStatus();

    # /* launch threads,one thread will listen to request from other nodes,one will do stabilization */
    # thread second(listenTo,ref(nodeInfo));
    # second.detach();
    #
    # thread fifth(doStabilize,ref(nodeInfo));
    # fifth.detach();
