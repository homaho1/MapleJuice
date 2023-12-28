from enum import Enum
import pickle
import logging
from config import QUERY_SIZE
import time
import traceback
import socket
import struct
import uuid
import os


class MessageType(Enum):
    GET_MODE = 1
    CHANGE_MODE = 2
    GREP = 3
    GOSSIP = 4
    GOSSIP_S = 5
    ELECTION = 6
    CHANGE_LEADER = 7
    GET_LEADER = 8
    SEND_OPS = 9
    SEND_RELICAS = 10
    CHANGE_QUEUE = 11
    UPDATE_LEADER_DATA = 12
    PUT_FILE = 13
    PUT_FILE_REQUEST = 14
    PUT_FILE_COMPLETE = 15
    PUT_FILE_RESPONSE = 16
    GET_FILE = 17
    GET_FILE_REQUEST = 18
    GET_FILE_RESPONSE = 19
    GET_FILE_COMPLETE = 20
    DELETE_FILE_REQUEST = 21
    DELETE_FILE = 22
    LIST_FILE = 23
    STORE_FILE = 24
    MAPLE = 25
    MAPLE_COMPLETE = 26
    JUICE = 27
    JUICE_COMPLETE = 28

class MessageStatus(Enum):
    SUCCESS = 1
    FAIL = 2

class MessageFlag(Enum):
    SYN = 1
    ACK = 2

class Message:
    def __init__(self, type: MessageType, content, status: MessageStatus, flag: MessageFlag = None):
        self.type = type
        self.content = content
        self.status = status
        self.flag = flag


def packMsg(msgType, msgContent, status, flag):
    '''
    Packs a message into a Message object.
    @param msgType: the type of the message
    @param msgContent: the content of the message
    @param status: the status of the message
    @return: the packed message
    '''
    msg = Message(msgType, msgContent, status, flag)
    msg = pickle.dumps(msg)
    return msg

def packFile(fileBlock):
    '''
    Packs a file block into a FileBlock object.
    @param fileBlock: the file block
    @return: the packed file block
    '''
    fileBlock = pickle.dumps(fileBlock)
    return fileBlock

def sendMsg(conn, msg, udp=False, addr=None):
    '''
    Sends a message to a socket connection.
    @param conn: the socket connection
    @param msg: the message to be sent
    @param udp: whether the message is sent via UDP
    @param addr: the address of the receiver
    '''
    if udp:
        if addr is None:
            raise Exception("UDP message must have address")
        conn.sendto(msg, addr)
    else:
        conn.sendall(msg)


def packAndSendMsg(conn, msgType, msgContent, status, flag=None, udp=False, addr=None):
    '''
    Packs a message and sends it to a socket connection.
    @param conn: the socket connection
    @param msgType: the type of the message
    @param msgContent: the content of the message
    @param status: the status of the message
    @param udp: whether the message is sent via UDP
    @param addr: the address of the receiver
    '''
    try:
        msg = packMsg(msgType, msgContent, status, flag)
        if not udp:
            size = len(msg)
            sendMsg(conn, struct.pack('>I', size), udp, addr)
        sendMsg(conn, msg, udp, addr)
    except socket.error as e:
        logging.error(e)
        logging.info(f"here?, {conn}")


def packAndSendMsgToAll(conn, msgType, msgContent, status, addr_list=None):
    '''
    Packs a message and sends it to a list of socket connections.
    @param conn: the socket connection
    @param msgType: the type of the message
    @param msgContent: the content of the message
    @param status: the status of the message
    @param addr_list: the list of addresses of the receivers
    '''
    try:
        msg = packMsg(msgType, msgContent, status, None)
        for addr in addr_list:
            sendMsg(conn, msg, True, addr)
    except socket.error as e:
        print(e)


def unpackMsg(msg):
    '''
    Unpacks a message from a Message object.
    @param msg: the message to be unpacked
    @return: the unpacked message
    '''
    if msg is None:
        return None, None, None, None
    msg = pickle.loads(msg)
    return msg


def receiveMsg(conn, udp=False, size=QUERY_SIZE):
    '''
    Receives a message from a socket connection.
    @param conn: the socket connection
    @param udp: whether the message is received via UDP
    @return: the received message
    '''
    if udp:
        msg, addr = conn.recvfrom(size)
        return msg, addr
    else:
        if size < QUERY_SIZE:
            msg = conn.recv(size)
        else:
            # t1 = time.time()
            tmpFileName = str(uuid.uuid4())
            tmpFile = open(tmpFileName, "wb")
            while size > 0:
                msg = conn.recv(size) # or size < QUERY_SIZE?
                size -= len(msg)
                tmpFile.write(msg)
            tmpFile.close()
            with open(tmpFileName, "rb") as f:
                msg = f.read()
            os.remove(tmpFileName)
            # logging.info(f"Received time {time.time() - t1}")
        return msg, None

def sendFileInfo(conn, fileName, fileSize, msgType):
    msg = packMsg(msgType, (fileName, struct.pack('>I', fileSize)), MessageStatus.SUCCESS, MessageFlag.SYN)
    sendMsg(conn, struct.pack('>I', len(msg)))
    sendMsg(conn, msg)
    logging.info(f"Sent {fileName} with size {fileSize} bytes")


def sendFile(conn, fileContent):
    sendMsg(conn, fileContent)
    logging.info(f"Sent {len(fileContent)} bytes")


def receiveFile(conn, fileSize):
    data = b''
    t1 = time.time()
    logging.info(f"trying to receive {fileSize} bytes in receiveFile")
    tmpFileName = str(uuid.uuid4())
    tmpFile = open(tmpFileName, "wb")
    while fileSize > 0:
        
        # logging.info(f"Receiving start")
        data, _ = receiveMsg(conn, False, fileSize if fileSize < QUERY_SIZE else QUERY_SIZE)
        # logging.info(f"Received  {len(data)} bytes")
        fileSize -= len(data)
        # logging.info(f"remaining {fileSize} len {len(data)}")
        tmpFile.write(data)
    tmpFile.close()

    with open(tmpFileName, "rb") as f:
        data = f.read()
    logging.info(f'pickled {len(data)}')
    os.remove(tmpFileName)
    data = pickle.loads(data)
    logging.info(f"Received time {time.time() - t1}")
    return data


def unpackAndReceiveMsg(conn, udp=False):
    '''
    Unpacks a message and receives it from a socket connection.
    @param conn: the socket connection
    @param udp: whether the message is received via UDP
    @return: the unpacked message
    '''
    try:
        if not udp:
            size = struct.unpack('>I', receiveMsg(conn, udp, 4)[0])[0]
            logging.info(size)
        else:
            size = QUERY_SIZE

        msg, addr = receiveMsg(conn, udp, size)
        msg = unpackMsg(msg)
        if not msg:
            logging.info('ERROR: no message received in unpackAndReceiveMsg')
        return msg, addr
    except Exception as e:
        logging.info(f'ERROR: socket error in unpackAndReceiveMsg: {e}, {traceback.format_exc()}')
        return None, None
    
def sendAndReceiveMsg(conn, msgType, msgContent, status, flag=None, udp=False, addr=None):
    packAndSendMsg(conn, msgType, msgContent, status, flag, udp, addr)
    return unpackAndReceiveMsg(conn, udp)

def validate(msg, msgType, msgFlag):
    '''
    Validates a message.
    @param msg: the message to be validated
    @param msgType: the type of the message
    @param msgStatus: the status of the message
    @param msgFlag: the flag of the message
    @return: whether the message is valid
    '''
    if msg is None or msg.type != msgType or msg.flag != msgFlag or msg.status != MessageStatus.SUCCESS:
        error_message = (
            f"Message validation failed:\n"
            f"Expected:\n"
            f"  type: {msgType}\n"
            f"  flag: {msgFlag}\n"
            f"  status: {MessageStatus.SUCCESS}\n"
            f"Got:\n"
            f"  type: {msg.type if msg else 'None'}\n"
            f"  flag: {msg.flag if msg else 'None'}\n"
            f"  status: {msg.status if msg else 'None'}"
        )
        logging.error(error_message)
        raise Exception("message validation failed")
    logging.debug(f'message with type {msg.type}, status {msg.status} successfully received')