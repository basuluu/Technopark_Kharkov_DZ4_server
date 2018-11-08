from unittest import TestCase

import time
import socket

import subprocess

from server import TaskQueueServer


class ServerBaseTest(TestCase):
    def setUp(self):
        self.server = subprocess.Popen(['python3', 'server.py'])
        # даем серверу время на запуск
        time.sleep(0.5)

    def tearDown(self):
        self.server.terminate()
        self.server.wait()

    def send(self, command):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(('127.0.0.1', 5555))
        s.send(command)
        data = s.recv(1000000)
        s.close()
        return data

    def test_a_save(self):
        self.setUp()
        task_id = self.send(b'ADD 6 2 12')
        self.assertEqual(b'YES', self.send(b'IN 6 ' + task_id))
        self.assertEqual(b'OK', self.send(b'SAVE'))
        self.tearDown()

        self.setUp()
        self.assertEqual(b'YES', self.send(b'IN 6 ' + task_id))
        self.send(b'GET 6')
        self.assertEqual(b'YES', self.send(b'ACK 6 ' + task_id))
        self.assertEqual(b'NONE', self.send(b'GET 6'))
        self.tearDown()

    def test_a_save_one_more(self):
        self.setUp()
        task_id = self.send(b'ADD 7 2 12')
        self.assertEqual(b'YES', self.send(b'IN 7 ' + task_id))
        self.assertEqual(b'OK', self.send(b'SAVE'))
        self.tearDown()

        self.setUp()
        self.send(b'GET 7')
        self.assertEqual(b'YES', self.send(b'ACK 7 ' + task_id))
        self.assertEqual(b'OK', self.send(b'SAVE'))
        self.tearDown()
 
        self.setUp()
        self.assertEqual(b'NONE', self.send(b'GET 7'))
        self.tearDown()

    def test_a_timeout(self):
        task_id = self.send(b'ADD 5 3 123')
        task = self.send(b'GET 5')
        self.assertEqual(b'NONE', self.send(b'GET 5'))
        time.sleep(5)
        self.assertEqual(task, self.send(b'GET 5'))
        self.assertEqual(b'YES', self.send(b'ACK 5 ' + task_id))

    def test_base_scenario(self):
        task_id = self.send(b'ADD 1 5 12345')
        self.assertEqual(b'YES', self.send(b'IN 1 ' + task_id))

        self.assertEqual(task_id + b' 5 12345', self.send(b'GET 1'))
        self.assertEqual(b'YES', self.send(b'IN 1 ' + task_id))
        self.assertEqual(b'YES', self.send(b'ACK 1 ' + task_id))
        self.assertEqual(b'NO', self.send(b'ACK 1 ' + task_id))
        self.assertEqual(b'NO', self.send(b'IN 1 ' + task_id))

    def test_two_tasks(self):
        first_task_id = self.send(b'ADD 1 5 12345')
        second_task_id = self.send(b'ADD 1 5 12345')
        self.assertEqual(b'YES', self.send(b'IN 1 ' + first_task_id))
        self.assertEqual(b'YES', self.send(b'IN 1 ' + second_task_id))

        self.assertEqual(first_task_id + b' 5 12345', self.send(b'GET 1'))
        self.assertEqual(b'YES', self.send(b'IN 1 ' + first_task_id))
        self.assertEqual(b'YES', self.send(b'IN 1 ' + second_task_id))
        self.assertEqual(second_task_id + b' 5 12345', self.send(b'GET 1'))

        self.assertEqual(b'YES', self.send(b'ACK 1 ' + second_task_id))
        self.assertEqual(b'NO', self.send(b'ACK 1 ' + second_task_id))

    def test_long_input(self):
        data = '12345' * 1000
        data = '{} {}'.format(len(data), data)
        data = data.encode('utf')
        task_id = self.send(b'ADD 1 ' + data)
        self.assertEqual(b'YES', self.send(b'IN 1 ' + task_id))
        self.assertEqual(task_id + b' ' + data, self.send(b'GET 1'))

    def test_wrong_command(self):
        self.assertEqual(b'ERROR', self.send(b'ADDD 1 5 12345'))

    def test_wrong_data(self):
        self.assertEqual(b'ERROR', self.send(b'ADD 1 4 12345'))
  
        
if __name__ == '__main__':
    unittest.main()