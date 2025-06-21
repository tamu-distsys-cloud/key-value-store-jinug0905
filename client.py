import random
import threading
from typing import Any, List

from labrpc.labrpc import ClientEnd
from server import GetArgs, GetReply, PutAppendArgs, PutAppendReply

def nrand() -> int:
    return random.getrandbits(62)

### Chatgpt Used ###
# I used chatgpt to come up with structure of shrad function. There was issue of input key
# not being digit. Didn't know how to check or convert. So I used chatgpt.

def shard(key: str, nshards: int) -> int:
    if key.isdigit():
        shard = int(key) % nshards
    else:
        shard = sum(map(ord, key)) % nshards

    return shard

class Clerk:
    def __init__(self, servers: List[ClientEnd], cfg):
        self.servers = servers
        self.cfg = cfg

        # Your definitions here.
        self.c_idx = nrand() # client index
        self.r_idx = 0       # 0 indexed, request index
        self.next   = 0      # start offset for each replica group

    # Fetch the current value for a key.
    # Returns "" if the key does not exist.
    # Keeps trying forever in the face of all other errors.
    #
    # You can send an RPC with code like this:
    # reply = self.server[i].call("KVServer.Get", args)
    # assuming that you are connecting to the i-th server.
    #
    # The types of args and reply (including whether they are pointers)
    # must match the declared types of the RPC handler function's
    # arguments in server.py.

    def get(self, key: str) -> str:
        args = GetArgs(key, self.c_idx, self.r_idx)
        self.r_idx += 1
        gid  = shard(key, self.cfg.nservers)

        while True:
            group = []

            for i in range(self.cfg.nreplicas):
                server_id = (gid + i) % self.cfg.nservers
                group.append(server_id)

            for off in range(self.cfg.nreplicas):
                server_id = group[(self.next + off) % self.cfg.nreplicas]

                try:
                    rep = self.servers[server_id].call("KVServer.Get", args)
                    if rep is not None:
                        self.next = (self.next + 1) % self.cfg.nreplicas
                        return rep.value          # ""
                except TimeoutError:
                    pass
          
            self.next = (self.next + 1) % self.cfg.nreplicas

        return ""

    # Shared by Put and Append.
    #
    # You can send an RPC with code like this:
    # reply = self.servers[i].call("KVServer."+op, args)
    # assuming that you are connecting to the i-th server.
    #
    # The types of args and reply (including whether they are pointers)
    # must match the declared types of the RPC handler function's
    # arguments in server.py.

    def put_append(self, key: str, value: str, op: str) -> str:
        args = PutAppendArgs(key, value, self.c_idx, self.r_idx)
        self.r_idx += 1
        gid  = shard(key, self.cfg.nservers)

        while True:
            group = []
            for i in range(self.cfg.nreplicas):
                server_index = (gid + i) % self.cfg.nservers
                group.append(server_index)

            for off in range(self.cfg.nreplicas):
                server_id = group[(self.next + off) % self.cfg.nreplicas]

                try:
                    rep = self.servers[server_id].call(f"KVServer.{op}", args)
                    if rep is not None:
                        self.next = (self.next + 1) % self.cfg.nreplicas
                        return rep.value          # ""
                except TimeoutError:
                    pass
            self.next = (self.next + 1) % self.cfg.nreplicas

        return ""

    def put(self, key: str, value: str):
        self.put_append(key, value, "Put")

    # Append value to key's value and return that value
    def append(self, key: str, value: str) -> str:
        return self.put_append(key, value, "Append")
