import socket
import logging
import os
import shutil
import pickle
import subprocess
from message import MessageType, MessageStatus, MessageFlag, packAndSendMsg, unpackAndReceiveMsg, validate, sendAndReceiveMsg
from helper import fetchIdinHostName

class tcpServer:
    def __init__(self, server, host, port) -> None:
        self.tcpServer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcpServer.bind((host, port))
        self.tcpServer.listen()

        # TODO: maybe change to event-driven to reduce coupling
        self.server = server # server object

    def handle_change_mode(self, conn, addr, msgType, msgContent):
        '''
        Handle change mode message
        @param conn: socket connection
        @param addr: address of the sender
        @param msgType: message type
        @param msgContent: message content
        '''
        try:
            self.server.changeMode(msgContent)
            logging.debug(
                f'Mode changed at {fetchIdinHostName(self.server.id[0])}')
        except Exception as msg:
            logging.error(msg)
            status = MessageStatus.FAIL
        else:
            status = MessageStatus.SUCCESS
        packAndSendMsg(conn, MessageType.CHANGE_MODE, '', status, MessageFlag.ACK)

    def handle_get_mode(self, conn, addr, msgType, msgContent):
        '''
        Handle get mode message
        @param conn: socket connection
        @param addr: address of the sender
        @param msgType: message type
        @param msgContent: message content
        '''
        packAndSendMsg(conn, MessageType.GET_MODE,
                       self.server.mode, MessageStatus.SUCCESS, MessageFlag.ACK)
        
    def handle_get_leader(self, conn, addr, msgType, msgContent):
        '''
        Handle get leader message
        @param conn: socket connection
        @param addr: address of the sender
        @param msgType: message type
        @param msgContent: message content
        '''
        try:
            logging.info(f'Get leader at {self.server.id} from {addr}')
            packAndSendMsg(conn, MessageType.GET_LEADER,
                        self.server.membershipList.getLeader(), MessageStatus.SUCCESS, MessageFlag.ACK)
        except Exception as msg:
            logging.error(f"Error in {msg}")
            status = MessageStatus.FAIL


    def handle_grep(self, conn, addr, msgType, msgContent):
        '''
        Handle grep message
        @param conn: socket connection
        @param addr: address of the sender
        @param msgType: message type
        @param msgContent: message content
        '''
        with self.server.list_lock:
            self.server.grep(conn, msgContent)
        
    def handle_election(self, conn, addr, msgType, msgContent):
        '''
        Handle election message
        @param conn: socket connection
        @param addr: address of the sender
        @param msgType: message type
        @param msgContent: message content
        '''
        if self.server.membershipList.getLeader() == self.server.id:
            self.electionTimes = 0
            self.isElecting = False
            packAndSendMsg(conn, MessageType.ELECTION,
                            '', MessageStatus.SUCCESS)
            logging.info(
                f'{self.server.id} receive election message, already leader')
        else:
            self.electionTimes += 1
            logging.info(
                f'{self.server.id} is elected as leader, receive times {self.electionTimes}')
            packAndSendMsg(conn, MessageType.ELECTION,
                            '', MessageStatus.SUCCESS)
            if self.electionTimes > self.server.membershipList.quorum:
                self.server.setLeader(self.server.id)
                self.server.sendChangeLeaderMessage(self.server.id)
                self.isElecting = False
                logging.info(
                    f'{self.server.id} receive {self.electionTimes} times > {self.server.membershipList.quorum}, become leader')
                self.electionTimes = 0

    def handle_change_leader(self, conn, addr, msgType, msgContent):
        '''
        Handle change leader message
        @param conn: socket connection
        @param addr: address of the sender
        @param msgType: message type
        @param msgContent: message content
        '''
        self.electionTime = 0
        self.isElecting = False
        self.server.membershipList.setLeader(msgContent)
        logging.info(f'Change leader to {msgContent} at {self.server.id}')

    def handle_send_ops(self, conn, addr, msgType, msgContent):
        '''
        Handle send operations message
        @param conn: socket connection
        @param addr: address of the sender
        @param msgType: message type
        @param msgContent: message content
        '''
        try:
            self.server.leader.addTask(
                msgContent[0], msgContent[1], msgContent[2])
            self.server.leader.notify(self.server.replicas, 'push',
                            msgContent[0], msgContent[1], msgContent[2])
        except Exception as e:
            logging.error(e)
            packAndSendMsg(conn, MessageType.SEND_OPS,
                              '', MessageStatus.FAIL, MessageFlag.ACK)
        else:
            packAndSendMsg(conn, MessageType.SEND_OPS,
                              '', MessageStatus.SUCCESS, MessageFlag.ACK)
    
    def handle_change_queue(self, conn, addr, msgType, msgContent):
        '''
        Handle change queue message
        @param conn: socket connection
        @param addr: address of the sender
        @param msgType: message type
        @param msgContent: message content
        '''
        self.server.leader.changeQueue(msgContent)
        packAndSendMsg(conn, MessageType.CHANGE_QUEUE,
                       '', MessageStatus.SUCCESS, MessageFlag.ACK)
        
    def handle_update_leader_data(self, conn, addr, msgType, msgContent):
        '''
        Handle update leader data message
        @param conn: socket connection
        @param addr: address of the sender
        @param msgType: message type
        @param msgContent: message content
        '''
        logging.info(f'Update leader data {msgContent}')
        self.server.leader.update(msgContent)
        packAndSendMsg(conn, MessageType.UPDATE_LEADER_DATA,
                       '', MessageStatus.SUCCESS, MessageFlag.ACK)
        
    def handle_send_replicas(self, conn, addr, msgType, msgContent):
        '''
        Handle send replicas message
        @param conn: socket connection
        @param addr: address of the sender
        @param msgType: message type
        @param msgContent: replicas
        '''
        logging.debug(f'Receive replicas {msgContent} from {conn}')
        self.server.replicas = msgContent
        packAndSendMsg(conn, MessageType.SEND_RELICAS, '',
                       MessageStatus.SUCCESS, MessageFlag.ACK)
        
    def handle_put_file(self, conn, addr, msgType, msgContent):
        '''
        Handle file info message
        @param conn: socket connection
        @param addr: address of the sender
        @param msgType: message type
        @param filename: filename
        @param fileSize: file size
        '''
        logging.info(f"enter file info {conn}")
        filename = msgContent

        # receive file content
        msg, _ = sendAndReceiveMsg(conn, MessageType.PUT_FILE,
                                '', MessageStatus.SUCCESS, MessageFlag.ACK)
        content = msg.content
        content = pickle.loads(content)

        logging.info(f"Received {len(content)} bytes")
        with open(f'sdfs/{filename}', 'wb') as f:
            f.write(content)
        logging.info(f'Put file successfully')

        # send ack to notify file received
        packAndSendMsg(conn, MessageType.PUT_FILE, '', MessageStatus.SUCCESS, MessageFlag.SYN)

    def handle_put_file_request(self, conn, addr, msgType, msgContent):
        '''
        Handle put file request message
        @param conn: socket connection
        @param addr: address of the sender
        @param msgType: message type
        '''
        logging.debug(f'Receive put file request with data {msgContent}')
        packAndSendMsg(conn, MessageType.PUT_FILE_REQUEST, '',
                       MessageStatus.SUCCESS, MessageFlag.ACK)
        addrs, localFileName, sdfsFileName = msgContent
        try:
            uploadFile = open(localFileName, 'rb')
            content = uploadFile.read()
            uploadFile.close()
        except Exception as e:
            logging.error(f'Cannot read file {localFileName}: {e}')
            return
        logging.debug(f'sucessfully read file {localFileName}')
        for addr in addrs:
            if addr == self.server.id:
                logging.debug(f"Put file to itself {addr}")
                if localFileName != f'sdfs/{sdfsFileName}':
                    shutil.copyfile(localFileName, f'sdfs/{sdfsFileName}')                       
                    logging.debug(f'Put file {sdfsFileName} successfully at {addr}')
                
                # send ack to leader
                msg, _ = sendAndReceiveMsg(conn, MessageType.PUT_FILE_COMPLETE,
                            (self.server.id, addr, sdfsFileName), MessageStatus.SUCCESS, MessageFlag.SYN)
                validate(msg, MessageType.PUT_FILE_COMPLETE, MessageFlag.ACK)
            else:
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.connect((addr[0], addr[1]))
                    
                    packed_content = pickle.dumps(content)
                    logging.info(f"filesize {len(packed_content)}")
                    
                    # send file name to receiver
                    msg, _ = sendAndReceiveMsg(s, MessageType.PUT_FILE,
                                sdfsFileName, MessageStatus.SUCCESS, MessageFlag.SYN)
                    validate(msg, MessageType.PUT_FILE, MessageFlag.ACK)

                    # send content
                    msg, _ = sendAndReceiveMsg(s, MessageType.PUT_FILE,
                                packed_content, MessageStatus.SUCCESS, MessageFlag.SYN)
                    validate(msg, MessageType.PUT_FILE, MessageFlag.SYN)

                    # send ack to leader
                    msg, _ = sendAndReceiveMsg(conn, MessageType.PUT_FILE_COMPLETE,
                                (self.server.id, addr, sdfsFileName), MessageStatus.SUCCESS, MessageFlag.SYN)
                    validate(msg, MessageType.PUT_FILE_COMPLETE, MessageFlag.ACK)

                    s.close()
                except Exception as e:
                    logging.error(f'Cannot connect to {addr} in put file request: {e}')
                    continue

    def handle_get_file(self, conn, addr, msgType, msgContent):
        '''
        Handle get file message
        @param conn: socket connection
        @param addr: address of the sender
        @param msgType: message type
        '''
        try:
            packAndSendMsg(conn, MessageType.GET_FILE, '', MessageStatus.SUCCESS, MessageFlag.ACK)
            sdfsFileName = msgContent
            with open(f'sdfs/{sdfsFileName}', 'rb') as f:
                content = f.read()
            logging.debug(f'Read file {sdfsFileName} successfully at local')
            content = pickle.dumps(content)

            msg, _ = sendAndReceiveMsg(conn, MessageType.GET_FILE,
                            content, MessageStatus.SUCCESS, MessageFlag.SYN)
            
            msg, _ = unpackAndReceiveMsg(conn)
            validate(msg, MessageType.GET_FILE, MessageFlag.SYN)
            packAndSendMsg(conn, MessageType.GET_FILE, '', MessageStatus.SUCCESS, MessageFlag.ACK)
        except Exception as e:
            logging.error(f'Cannot get file {sdfsFileName}: {e}')


    def handle_get_file_request(self, conn, addr, msgType, msgContent):
        '''
        Handle get file request message
        @param conn: socket connection
        @param addr: address of the sender
        @param msgType: message type
        '''
        logging.debug(f'Receive get file request on sdfs/{msgContent[0]}, stores at {msgContent[2]}')
        packAndSendMsg(conn, MessageType.GET_FILE_REQUEST, '', MessageStatus.SUCCESS, MessageFlag.ACK)

        localFileName, sdfsFileName, addresses = msgContent
        if addresses == None:
            logging.error(f'No file found')
            return 
        if self.server.id in addresses:
            logging.debug(f"Get file to itself")
            shutil.copyfile(f'sdfs/{sdfsFileName}', localFileName)
            msg, _ = sendAndReceiveMsg(conn, MessageType.GET_FILE_COMPLETE,
                        (self.server.id, sdfsFileName) , MessageStatus.SUCCESS, MessageFlag.SYN)
            validate(msg, MessageType.GET_FILE_COMPLETE, MessageFlag.ACK)
            return
        for addr in addresses:
            try:
                logging.debug(f'Try to connect to {addr}')
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((addr[0], addr[1]))
                
                # send get file request with file name
                msg, _ = sendAndReceiveMsg(s, MessageType.GET_FILE,
                                sdfsFileName, MessageStatus.SUCCESS, MessageFlag.SYN)
                validate(msg, MessageType.GET_FILE, MessageFlag.ACK)

                # get content
                msg, _ = unpackAndReceiveMsg(s)
                validate(msg, MessageType.GET_FILE, MessageFlag.SYN)
                packAndSendMsg(s, MessageType.GET_FILE, '', MessageStatus.SUCCESS, MessageFlag.ACK)

                # write file in local
                content = msg.content
                content = pickle.loads(content)
                logging.debug(f"Received {len(content)} bytes")
                with open(f'{localFileName}', 'wb+') as f:
                    f.write(content)
                logging.debug(f'Wrote file successfully')
                
                # send ack to notify file received
                msg, _ = sendAndReceiveMsg(s, MessageType.GET_FILE,
                                '', MessageStatus.SUCCESS, MessageFlag.SYN)
                validate(msg, MessageType.GET_FILE, MessageFlag.ACK)
                s.close()

                # send ack to leader
                msg, _ = sendAndReceiveMsg(conn, MessageType.GET_FILE_COMPLETE,
                                (self.server.id, sdfsFileName) , MessageStatus.SUCCESS, MessageFlag.SYN)
                validate(msg, MessageType.GET_FILE_COMPLETE, MessageFlag.ACK)
                break
            except Exception as e:
                logging.error(f'Cannot connect to {addr} in get file request: {e}')
                continue
        else:
            logging.error(f'Cannot get file {sdfsFileName} from {addresses}')

    def handle_delete_file_request(self, conn, addr, msgType, msgContent):
        '''
        Handle delete file request message
        @param conn: socket connection
        @param addr: address of the sender
        @param msgType: message type
        '''
        try:
            logging.debug(f'Receive delete file request with data {msgContent}')
            packAndSendMsg(conn, MessageType.DELETE_FILE_REQUEST, '',
                            MessageStatus.SUCCESS, MessageFlag.ACK)
            file = msgContent
            if os.path.exists(f'sdfs/{file}'):
                os.remove(f'sdfs/{file}')
                logging.debug(f'Delete file {file} successfully on local')
            else:
                logging.debug(f'File {file} not found')

            msg, _ = sendAndReceiveMsg(conn, MessageType.DELETE_FILE,
                            '', MessageStatus.SUCCESS, MessageFlag.SYN)
            validate(msg, MessageType.DELETE_FILE, MessageFlag.ACK)
        except Exception as e:
            logging.error(f'Cannot delete file {file}: {e}')

    def handle_list_file(self, conn, addr, msgType, msgContent):
        '''
        Handle list file message
        @param conn: socket connection
        @param addr: address of the sender
        @param msgType: message type
        '''
        try:
            serverId = msgContent[0]
            filename = msgContent[1]
            logging.info(f'ls {filename} = {serverId}, len = {len(serverId) if serverId else 0}')
        except Exception as e:
            logging.error(f'Cannot list file {filename}')
            packAndSendMsg(conn, MessageType.LIST_FILE, '',
                        MessageStatus.FAIL, MessageFlag.ACK)
        else:
            packAndSendMsg(conn, MessageType.LIST_FILE, '',
                        MessageStatus.SUCCESS, MessageFlag.ACK)

    def handle_store_file(self, conn, addr, msgType, msgContent):
        '''
        Handle store file message
        @param conn: socket connection
        @param addr: address of the sender
        @param msgType: message type
        '''
        try:
            files = msgContent
            logging.info(f'Store = {files}')
        except Exception as e:
            logging.error(f'Cannot store file {files}')
            packAndSendMsg(conn, MessageType.STORE_FILE, '',
                        MessageStatus.FAIL, MessageFlag.ACK)
        else:
            packAndSendMsg(conn, MessageType.STORE_FILE, '',
                        MessageStatus.SUCCESS, MessageFlag.ACK)
            
    def handle_maple(self, conn, addr, msgType, msgContent):
        '''
        Handle maple message
        @param conn: socket connection
        @param addr: address of the sender
        @param msgType: message type
        '''
        try:
            logging.info(f'Maple run')
            executeFile, args, content = msgContent
            packAndSendMsg(conn, MessageType.MAPLE, '',
                        MessageStatus.SUCCESS, MessageFlag.ACK)
            
            if not os.path.exists(f'local'):
                os.makedirs(f'local')

            with open(f'local/input.txt', 'w') as f:
                f.write("\n".join(content))
            logging.info(args)
            subprocess.run(['python3', executeFile, args, "local/input.txt", "local/output.txt"])
            with open('local/output.txt', 'r') as f:
                content = f.read()

            # os.remove(f'sdfs/{fileName}')
            # os.remove('input.txt')
            # os.remove('output.txt')

            msg, _ = sendAndReceiveMsg(conn, MessageType.MAPLE_COMPLETE,
                            content, MessageStatus.SUCCESS, MessageFlag.SYN)
            validate(msg, MessageType.MAPLE_COMPLETE, MessageFlag.ACK)
            logging.info(f'Maple successfully')
        except Exception as e:
            logging.error(f'Cannot maple: {e}')

    def handle_juice(self, conn, addr, msgType, msgContent):
        try:
            logging.info(f'Juice using {msgContent}')
            length = msgContent
            packAndSendMsg(conn, MessageType.JUICE, '',
                        MessageStatus.SUCCESS, MessageFlag.ACK)
            logging.info(f"Receive file length {length}")
            for i in range(length):
                msg, _ = unpackAndReceiveMsg(conn)
                validate(msg, MessageType.JUICE, MessageFlag.SYN)
                packAndSendMsg(conn, MessageType.JUICE, '',
                            MessageStatus.SUCCESS, MessageFlag.ACK)
                executeFile = msg.content[0]
                fileContent = msg.content[1]

                with open(f'local/input2.txt', 'w') as f:
                    f.write(fileContent)
            
                subprocess.run(['python3', executeFile, "local/input2.txt", "local/output2.txt"])
                with open('local/output2.txt', 'r') as f:
                    content = f.read()
                
                msg, _ = sendAndReceiveMsg(conn, MessageType.JUICE, content, MessageStatus.SUCCESS, MessageFlag.SYN)
                validate(msg, MessageType.JUICE, MessageFlag.ACK)

            # os.remove(f'sdfs/{fileName}')
            # os.remove('output.txt')

            # Send complete message
            msg, _ = sendAndReceiveMsg(conn, MessageType.JUICE_COMPLETE,
                            '', MessageStatus.SUCCESS, MessageFlag.SYN)
            validate(msg, MessageType.JUICE_COMPLETE, MessageFlag.ACK)
            logging.info(f'Juice successfully')
        except Exception as e:
            logging.error(f'Cannot juice: {e}')

    def serve_forever(self) -> None:
        '''
        Create a TCP server to receive get/change mode message
        '''
        self.electionTimes = 0
        handlers = {
            MessageType.CHANGE_MODE: self.handle_change_mode,
            MessageType.GET_MODE: self.handle_get_mode,
            MessageType.GET_LEADER: self.handle_get_leader,
            MessageType.GREP: self.handle_grep,
            MessageType.ELECTION: self.handle_election,
            MessageType.CHANGE_LEADER: self.handle_change_leader,
            MessageType.SEND_OPS: self.handle_send_ops,
            MessageType.CHANGE_QUEUE: self.handle_change_queue,
            MessageType.UPDATE_LEADER_DATA: self.handle_update_leader_data,
            MessageType.SEND_RELICAS: self.handle_send_replicas,
            MessageType.PUT_FILE: self.handle_put_file,
            MessageType.GET_FILE: self.handle_get_file,
            MessageType.GET_FILE_REQUEST: self.handle_get_file_request,
            MessageType.PUT_FILE_REQUEST: self.handle_put_file_request,
            MessageType.DELETE_FILE_REQUEST: self.handle_delete_file_request,
            MessageType.LIST_FILE: self.handle_list_file,
            MessageType.STORE_FILE: self.handle_store_file,
            MessageType.MAPLE: self.handle_maple,
            MessageType.JUICE: self.handle_juice,
        }
        while True:
            conn, address = self.tcpServer.accept()
            msg, _ = unpackAndReceiveMsg(conn)

            if msg.flag != MessageFlag.SYN:
                logging.error(f"Invalid message flag: {msg.type}")
                continue
            #logging.info(f"{msg.type} {msg.content}, {msg.status}, {msg.flag}, {address}")
            handler = handlers.get(msg.type)
            if handler:
                handler(conn, address, msg.type, msg.content)
            else:
                logging.error("Invalid message type")

            conn.close()
