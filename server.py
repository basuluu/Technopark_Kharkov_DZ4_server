import argparse
import socket
import heapq
import time
import pickle

class TaskQueueServer:
    def __init__(self, ip, port, path, timeout):
        self.ip = ip
        self.port = port
        self.path = path
        self.timeout = timeout
        self.heap, self.buff_heap = self.load('heap'), self.load('heap_buff')

    def load(self, name):
        try:
            with open((self.path + name), 'rb') as f:
                heap = pickle.load(f)
            f.close()
        except:
            heap = {}
        finally:
            return heap

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
        return time.time()

    def add_action(self, data, conn):
        if data[1] not in self.heap:
            self.heap[data[1]] = []
        uniq_id = str(self.gen_uniq_id())
        heapq.heappush(self.heap[data[1]], [uniq_id, data[2], data[3]])
        conn.send(bytes(uniq_id, 'utf-8'))

    def get_action(self, data, conn):
        if data[1] not in self.heap or len(self.heap[data[1]]) == 0:
            conn.send(b'NONE')
            return
        else:
            que = heapq.heappop(self.heap[data[1]])
            que.append(time.time() + float(self.timeout))
            if data[1] not in self.buff_heap.keys():
                self.buff_heap[data[1]] = []
            heapq.heappush(self.buff_heap[data[1]], que)
            conn.send(bytes("{} {} {}".format(que[0], que[1], que[2]), 'utf-8'))

    def search_task(self, data, heap):
        if data[1] in heap.keys():
            for task in heap.get(data[1]):
                if data[2] in task:
                    return task
        return False

    def ack_action(self, data, conn):
        task = self.search_task(data, self.buff_heap)
        if task:
            self.buff_heap[data[1]].remove(task)
            conn.send(b'YES')
        else:
            conn.send(b'NO')

    def in_action(self, data, conn):
        if self.search_task(data, self.heap) or self.search_task(data, self.buff_heap):
            conn.send(b'YES')
        else:
            conn.send(b'NO')  

    def check(self, buff_heap):
        for que, tasks in buff_heap.items():
            if tasks != None:
                for task in tasks:
                    if time.time() > float(task[3]):
                        heapq.heappush(self.heap[que], task[:-1])
                        self.buff_heap[que].remove(task)

    def save(self, conn):
        with open((self.path + 'heap'), 'wb') as f:
            pickle.dump(self.heap, f)
        f.close()
        with open((self.path + 'heap_buff'), 'wb') as f:
            pickle.dump(self.buff_heap, f)
        f.close()
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
        self.check(self.buff_heap)
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

if __name__ == '__main__':
    args = parse_args()
    server = TaskQueueServer(**args.__dict__)
    server.run()
