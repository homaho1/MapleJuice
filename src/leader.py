import socket
import logging
from task_queue import TaskQueue
from message import MessageType, MessageStatus, MessageFlag, packAndSendMsg, unpackAndReceiveMsg
from collections import defaultdict
import threading

class Leader:
    def __init__(self):
        self.isActive = False
        self.taskQueue = TaskQueue()
        self.fileToAddress = defaultdict(set)
        self.filesToRecovery = set()
        self.cv = threading.Condition()
        self.activeCV = threading.Condition()

    def getFileStorage(self, filename, numberOfServers):
        count = 0
        for c in filename:
            count += ord(c)
        return count % numberOfServers

    def listFiles(self):
        return self.fileToAddress

    def addTask(self, *args):
        self.taskQueue.add(args[0], args[1], args[2:])
        with self.cv:
            self.cv.notify()

    def popTask(self):
        return self.taskQueue.pop()

    def frontTask(self):
        return self.taskQueue.front()
    
    def size(self):
        return self.taskQueue.size()

    def changeQueue(self, *args):
        tp = args[0]
        args = args[1:]
        if tp == 'push':
            self.addTask(args)
        elif tp == 'pop':
            self.popTask()

    def notify(self, servers, *args):
        for addr, port, _ in servers:
            with socket.create_connection((addr, port)) as s:
                packAndSendMsg(s, MessageType.CHANGE_QUEUE,
                               args, MessageStatus.SUCCESS, MessageFlag.SYN)
                msgType, _, msgStatus, _, msgFlag = unpackAndReceiveMsg(s)
                if msgType == MessageType.CHANGE_QUEUE and msgStatus == MessageStatus.SUCCESS and msgFlag == MessageFlag.ACK:
                    logging.info(f'Notify changed queue to {addr}')

    def updateLeaderData(self, servers):
        for addr, port, _ in servers:
            with socket.create_connection((addr, port)) as s:
                data = {'queue': self.taskQueue.queue,
                        'fileToAddress': self.fileToAddress,
                        'filesToRecovery': self.filesToRecovery}
                packAndSendMsg(s, MessageType.UPDATE_LEADER_DATA,
                               data, MessageStatus.SUCCESS, MessageFlag.SYN)
                msgType, _, msgStatus, _, msgFlag = unpackAndReceiveMsg(s)
                if msgType == MessageType.UPDATE_LEADER_DATA and msgStatus == MessageStatus.SUCCESS and msgFlag == MessageFlag.ACK:
                    logging.info('Successfully sent replicas')

    def update(self, data):
        self.taskQueue.queue = data['queue'].copy()
        self.fileToAddress = data['fileToAddress']
        self.filesToRecovery = set(data['filesToRecovery'])
        logging.info('Successfully updated replicas')

    def insertTask(self, *args):
        self.taskQueue.insert(args[0], args[1], args[2:])
        with self.cv:
            self.cv.notify()
