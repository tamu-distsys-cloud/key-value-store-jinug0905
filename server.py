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
    def __init__(self, key, value, client_idx, request_idx):
        self.key = key
        self.value = value

        self.c_idx = client_idx
        self.r_idx = request_idx

class PutAppendReply:
    # Add definitions here if needed
    def __init__(self, value):
        self.value = value

class GetArgs:
    # Add definitions here if needed
    def __init__(self, key, client_idx, request_idx):
        self.key = key

        self.c_idx = client_idx
        self.r_idx = request_idx

class GetReply:
    # Add definitions here if needed
    def __init__(self, value):
        self.value = value

class KVServer:
    def __init__(self, cfg):
        self.mu = threading.Lock()
        self.cfg = cfg
        self.dict_kv = {}

        self.requests = {}
        self.replies = {}
        # Your definitions here.

    def Get(self, args: GetArgs):

        # Your code here.
        self.mu.acquire()
        try:
            req = self.requests.get(args.c_idx, -1)

            if args.r_idx <= req: # Already requested
                return self.replies[args.c_idx]
            
            value = self.dict_kv.get(args.key, "")
            reply = GetReply(value)

            self.requests[args.c_idx] = args.r_idx
            self.replies[args.c_idx] = reply
        finally:
            self.mu.release()

        return GetReply(value)

    def Put(self, args: PutAppendArgs):

        # Your code here.
        self.mu.acquire()
        try:
            req = self.requests.get(args.c_idx, -1)

            if args.r_idx < req: # Already requested
                return self.replies[args.c_idx]
            
            self.dict_kv[args.key] = args.value
            reply = PutAppendReply("")

            self.requests[args.c_idx] = args.r_idx
            self.replies[args.c_idx] = reply

        finally:
            self.mu.release()

        return reply

    def Append(self, args: PutAppendArgs):

        # Your code here.
        self.mu.acquire()
        try:
            req = self.requests.get(args.c_idx, -1)

            if args.r_idx <= req: # Already requested
                return self.replies[args.c_idx]
            
            temp_val = self.dict_kv.get(args.key, "")
            self.dict_kv[args.key] = temp_val + args.value
            reply = PutAppendReply(temp_val)

            self.requests[args.c_idx] = args.r_idx
            self.replies[args.c_idx] = reply

        finally:
            self.mu.release()

        return reply