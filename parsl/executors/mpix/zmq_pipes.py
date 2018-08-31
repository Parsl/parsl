#!/usr/bin/env python3

import zmq
import uuid
import time


class JobsQOutgoing(object):

    def __init__(self, task_q):

        self.task_q = task_q
        self.context = zmq.Context()

        self.zmq_socket = self.context.socket(zmq.PUSH)
        self.zmq_socket.bind(task_q)

    def put(self, message):
        self.zmq_socket.send_pyobj(message)


class JobsQIncoming(object):

    def __init__(self, task_url, server_id=None):

        self.server_id = server_id if server_id else uuid.uuid4()
        self.task_url = task_url

        print("I am server #%s" % (self.server_id))
        self.context = zmq.Context()

        #  Task Q
        self.task_q = self.context.socket(zmq.PULL)
        self.task_q.connect(self.task_url)

    def get(self, block=False, timeout=None):
        work = self.task_q.recv_pyobj(flags=zmq.NOBLOCK)
        return work


class ResultsQIncoming(object):

    def __init__(self, results_q):
        self.results_q = results_q

        self.context = zmq.Context()
        self.results_receiver = self.context.socket(zmq.PULL)
        self.results_receiver.bind(self.results_q)

    def get(self, block=True, timeout=None):
        result = self.results_receiver.recv_pyobj(flags=zmq.NOBLOCK)
        return result


class ResultsQOutgoing(object):

    def __init__(self, results_url, server_id=None):

        self.server_id = server_id if server_id else uuid.uuid4()
        self.results_url = results_url

        print("I am server #%s" % (self.server_id))
        self.context = zmq.Context()

        # Results Q
        self.results_q = self.context.socket(zmq.PUSH)
        self.results_q.connect(self.results_url)

    def put(self, result):
        self.results_q.send_pyobj(result)


if __name__ == "__main__":

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--type", default="client", help="client/server")
    args = parser.parse_args()

    jobQ = "tcp://127.0.0.1:5557"
    resultQ = "tcp://127.0.0.1:5558"

    if args.type == "client":
        print("Client")
        jobs_q = JobsQOutgoing(jobQ)
        results_q = ResultsQIncoming(resultQ)
        count = 0
        while True:
            jobs_q.put({'message': 'hello {}'.format(count)})
            print(results_q.get())
            count += 1

    else:
        print("Server")
        jobs_q = JobsQIncoming(jobQ)
        results_q = ResultsQOutgoing(resultQ)

        while True:
            message = jobs_q.get()
            print("Server received : ", message)
            message['server_id'] = "foo"
            print("Server responding with : ", message)
            results_q.put(message)
            print("Sent reply")
            time.sleep(0.2)
