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

        self.sid = 0
        for s in cfg.kvservers:
            if s is not None:
                self.sid += 1
        # Your definitions here.

    def shard_of(self, key: str) -> int:
        if key.isdigit():
            shard = int(key) % self.cfg.nservers
        else:
            shard = sum(map(ord, key)) % self.cfg.nservers
        return shard
    
    ### Chatgpt Used ###
    # I used chatgpt to help me quickly write out the code to check for key owndership
    # of each shard (for Get, Put, Append functions). 
    # I also use it to write out the replication code in put and append functions

    def PutReplica(self, args: PutAppendArgs):
        self.mu.acquire()
        try:
            self.dict_kv[args.key] = args.value
        finally:
            self.mu.release()

    def AppendReplica(self, args: PutAppendArgs):
        self.mu.acquire()
        try:
            self.dict_kv[args.key] = self.dict_kv.get(args.key, "") + args.value
        finally:
            self.mu.release()

    def Get(self, args: GetArgs):
        shard = self.shard_of(args.key)
        sid = self.sid
        cfg = self.cfg

        # ownership
        d = (sid - shard) % cfg.nservers
        if d == 0:
            owns_key = True
        elif 0 < d < cfg.nreplicas:
            owns_key = True
        else:
            owns_key = False
        # Reject
        if not owns_key:
            return None

        # if not primary
        if d != 0:
            return cfg.kvservers[shard].Get(args)
        
        self.mu.acquire()
        try:
            req = self.requests.get(args.c_idx, -1)

            if args.r_idx <= req: # old requests
                return self.replies[args.c_idx]
            
            value = self.dict_kv.get(args.key, "")
            reply = GetReply(value)

            self.requests[args.c_idx] = args.r_idx
            self.replies[args.c_idx] = reply
        finally:
            self.mu.release()

        return reply

    def Put(self, args: PutAppendArgs):
        shard = self.shard_of(args.key)
        sid = self.sid
        cfg = self.cfg

        # ownership
        d = (sid - shard) % cfg.nservers
        if d == 0:
            is_primary = True
            owns_key = True
        elif 0 < d < cfg.nreplicas:
            is_primary = False
            owns_key = True
        else:
            is_primary = False
            owns_key = False

        # Reject
        if not owns_key:
            return None

        # if not primary
        if not is_primary:
            return cfg.kvservers[shard].Put(args)

        self.mu.acquire()
        try:
            req = self.requests.get(args.c_idx, -1)

            if args.r_idx < req: # old requests
                return self.replies[args.c_idx]
            
            self.dict_kv[args.key] = args.value
            reply = PutAppendReply("")

            self.requests[args.c_idx] = args.r_idx
            self.replies[args.c_idx] = reply
        
        finally:
            self.mu.release()

        # replication
        for i in range(1, self.cfg.nreplicas):
            sid = (self.shard_of(args.key) + i) % self.cfg.nservers
            follower = self.cfg.kvservers[sid]
            if follower is not None:
                follower.PutReplica(args)

        return reply

    def Append(self, args: PutAppendArgs):
        shard = self.shard_of(args.key)
        sid = self.sid
        cfg = self.cfg

        # ownership
        d = (sid - shard) % cfg.nservers
        if d == 0:
            is_primary = True
            owns_key = True
        elif 0 < d < cfg.nreplicas:
            is_primary = False
            owns_key = True
        else:
            is_primary = False
            owns_key = False

         # Reject
        if not owns_key:
            return None

        # if not primary
        if not is_primary:
            return cfg.kvservers[shard].Append(args)

        self.mu.acquire()
        try:
            req = self.requests.get(args.c_idx, -1)

            if args.r_idx <= req:  # old requests
                return self.replies[args.c_idx]
            
            temp_val = self.dict_kv.get(args.key, "")
            self.dict_kv[args.key] = temp_val + args.value
            reply = PutAppendReply(temp_val)

            self.requests[args.c_idx] = args.r_idx
            self.replies[args.c_idx] = reply

        finally:
            self.mu.release()

        # replicate
        for i in range(1, self.cfg.nreplicas):
            sid = (self.shard_of(args.key) + i) % self.cfg.nservers
            follower = self.cfg.kvservers[sid]
            if follower is not None:
                follower.PutReplica(args)

        return reply