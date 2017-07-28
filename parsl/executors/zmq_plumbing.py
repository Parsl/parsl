import sys
import time
import zmq

context = zmq.Context()

class ZmQueue():

    def __init__ (self, incoming, outgoing):

        self.incoming = context.socket(zmq.PULL)
        self.incoming.connect(incoming)

        self.outgoing = context.socket(zmq.PUSH)
        self.outgoing.connect(outgoing)

    def get(self):
        print("Waiting on ", self.incoming)
        x = self.incoming.recv()
        print("Received message ", x)
        return x

    def put(self, msg):
        print("Posting on ", self.outgoing)
        print("Sending message" , msg)
        return self.outgoing.send(msg)

