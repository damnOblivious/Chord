import hashlib

from variables import *

def getHash(key):
    # hashOutput = ""
    sizeOfReqBits = int(M / 4)

    sha_1 = hashlib.sha1(key.encode('ascii'))
    hashHex = sha_1.hexdigest()[:sizeOfReqBits]
    hashOutput = int(hashHex, 16) % (2 ** M)
    return hashOutput
