import logging
import threading
from typing import Tuple, Any

debugging = False

# Use this function for debugging
def debug(format, *args):
    if debugging:
        logging.info(format % args)

# Put or Append
class PutAppendArgs:
    # Add definitions here if needed
    def __init__(self, key, value):
        self.key = key
        self.value = value

class PutAppendReply:
    # Add definitions here if needed
    def __init__(self, value):
        self.value = value

class GetArgs:
    # Add definitions here if needed
    def __init__(self, key):
        self.key = key

class GetReply:
    # Add definitions here if needed
    def __init__(self, value):
        self.value = value

class KVServer:
    def __init__(self, cfg):
        self.mu = threading.Lock()
        self.cfg = cfg
        self.dict_kv = {}

        # Your definitions here.

    def Get(self, args: GetArgs):

        # Your code here.
        self.mu.acquire()
        try:
            value = self.dict_kv.get(args.key, "")
        finally:
            self.mu.release()

        return GetReply(value)

    def Put(self, args: PutAppendArgs):

        # Your code here.
        self.mu.acquire()
        try:
            self.dict_kv[args.key] = args.value
        finally:
            self.mu.release()

        return PutAppendReply("")

    def Append(self, args: PutAppendArgs):

        # Your code here.
        self.mu.acquire()
        try:
            temp_val = self.dict_kv.get(args.key, "")
            self.dict_kv[args.key] = temp_val + args.value
        finally:
            self.mu.release()

        return PutAppendReply(temp_val)