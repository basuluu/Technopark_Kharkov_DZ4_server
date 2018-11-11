import argparse
import socket
import heapq
import time
import pickle
import uuid

class TaskQueueServer:
    def __init__(self, ip, port, path, timeout):
        self.ip = ip
        self.port = port
        self.path = path
        self.timeout = timeout
        self.storage = Storage()

    def run(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind((self.ip, self.port))
        self.sock.listen(5)
        while True:
            try:
                conn, addr = self.sock.accept()
                self.parse(conn)
                conn.close()
            except KeyboardInterrupt:
                self.sock.close()
                break

    def gen_uniq_id(self):
        return uuid.uuid1()

    def add_action(self, data, conn):
        uniq_id = str(self.gen_uniq_id())
        self.storage.add_task(data, uniq_id)
        conn.send(bytes(uniq_id, 'utf-8'))

    def get_action(self, data, conn):
        task = self.storage.get_task(data, self.timeout)
        if task:
            conn.send(bytes("{} {} {}".format(task[1], task[2], task[3]), 'utf-8'))
        else:
            conn.send(b'NONE')

    def ack_action(self, data, conn):
        if self.storage.ack_task(data):
            conn.send(b'YES')
        else:
            conn.send(b'NO')

    def in_action(self, data, conn):
        if self.storage.in_heap(data):
            conn.send(b'YES')
        else:
            conn.send(b'NO')  

    def save(self, conn):
        self.storage.save(self.path)
        conn.send(b'OK')
    
    def work_with_req(self, data, conn):
        if data[0] == "ADD":
            if not int(data[2]) > 1000000 and len(data[3]) == int(data[2]):
                self.add_action(data, conn)
            else:
                conn.send(b'ERROR')
        elif data[0] == "GET":
            self.get_action(data, conn)
        elif data[0] == "ACK":
            self.ack_action(data, conn)
        elif data[0] == "IN":
            self.in_action(data, conn)
        elif data[0] == "SAVE":
            self.save(conn)
        else:
            conn.send(b'ERROR')
        return

    def parse(self, conn):
        conn.setblocking(0)
        tmp = b''
        while 1:
            try:
                tmp += conn.recv(128)
            except BlockingIOError:
                break
        data = tmp.decode("utf-8")
        data = data.split()
        self.storage.update()
        self.work_with_req(data, conn)

def parse_args():
    parser = argparse.ArgumentParser(description='This is a simple task queue server with custom protocol')
    parser.add_argument(
        '-p',
        action="store",
        dest="port",
        type=int,
        default=5555,
        help='Server port')
    parser.add_argument(
        '-i',
        action="store",
        dest="ip",
        type=str,
        default='0.0.0.0',
        help='Server ip adress')
    parser.add_argument(
        '-c',
        action="store",
        dest="path",
        type=str,
        default='./',
        help='Server checkpoints dir')
    parser.add_argument(
        '-t',
        action="store",
        dest="timeout",
        type=int,
        default=4,
        help='Task maximum GET timeout in seconds')
    return parser.parse_args()


class Storage():
    def __init__(self):
        self.heap, self.buff_heap = self.load('heap')
        
    def load(self, name):
        heap = ({}, {})
        try:
            with open((self.path + name), 'rb') as f:
                heap = pickle.load(f)
            f.close()
        except IOError:
            raise IOError
        except PermissionError:
            raise PermissionEror
        finally:
            return heap         

    def gen_time(self):
        return time.time()

    def add_task(self, data, uniq_id):
        if data[1] not in self.heap:
            self.heap[data[1]] = []
        heapq.heappush(self.heap[data[1]], [self.gen_time(), uniq_id, data[2], data[3]])
        
    def get_task(self, data, timeout):
        if data[1] not in self.heap or len(self.heap[data[1]]) == 0:
            return False
        else:
            task = heapq.heappop(self.heap[data[1]])
            task.append(self.gen_time() + float(timeout))
            if data[1] not in self.buff_heap.keys():
                self.buff_heap[data[1]] = []
            heapq.heappush(self.buff_heap[data[1]], task)
            return task

    def search_task(self, data, heap):
        if data[1] in heap.keys():
            for task in heap.get(data[1]):
                if data[2] in task:
                    return task
        return False

    def ack_task(self, data):
        task = self.search_task(data, self.buff_heap)
        if task:
            self.buff_heap[data[1]].remove(task)
            return True
        return False

    def in_heap(self, data):
        if self.search_task(data, self.heap) or self.search_task(data, self.buff_heap):
            return True
        return False

    def update(self):
        for que, tasks in self.buff_heap.items():
            if tasks != None:
                for task in tasks:
                    if time.time() > float(task[4]):
                        heapq.heappush(self.heap[que], task[:-1])
                        self.buff_heap[que].remove(task)
                    else:
                        break

    def save(self, path):
        with open((path + 'heap'), 'wb') as f:
            pickle.dump((self.heap, self.buff_heap), f)
        f.close()


if __name__ == '__main__':
    args = parse_args()
    server = TaskQueueServer(**args.__dict__)
    server.run()
