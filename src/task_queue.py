from helper import *
from membership import *
from message import *
from logger import *
import threading

'''
TaskQueue is a class that represents a queue of tasks.
Tasks include: put, get, delete, ls, and store
'''


class TaskQueue:
    def __init__(self):
        self.queue = []
        self.lock = threading.Lock()

    def add(self, sender, _task, *args):
        task = {}
        task['sender'] = sender
        task['type'] = _task
        task['args'] = args
        with self.lock:
            self.queue.append(task)

    def insert(self, sender, _task, *args):
        task = {}
        task['sender'] = sender
        task['type'] = _task
        task['args'] = args
        with self.lock:
            if len(self.queue) > 0:
                self.queue.insert(1, task)
            else:
                self.queue.append(task)

    def front(self):
        with self.lock:
            if len(self.queue) > 0:
                return self.queue[0]
            else:
                return None

    def pop(self):
        with self.lock:
            if len(self.queue) > 0:
                return self.queue.pop(0)
            else:
                return None

    def size(self):
        return len(self.queue)

    def isEmpty(self):
        return self.size() == 0

    def clear(self):
        with self.lock:
            self.queue.clear()

    def __str__(self):
        return str(self.queue)
