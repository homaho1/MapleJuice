import socket
import subprocess
import time
import threading
import os
import sys
import argparse
import logging
import inspect
import random
import pickle
from random import choices
from leader import Leader
from logger import Logger
from tcp_server import tcpServer
from membership import MembershipList
from message import (
    MessageType,
    MessageStatus,
    MessageFlag,
    packAndSendMsg,
    unpackAndReceiveMsg,
    sendAndReceiveMsg,
    validate,
    packAndSendMsgToAll,
)
from helper import handle_exceptions, locked, fetchIdinHostName
from config import (
    PORT,
    T_HB,
    T_GOSSIP,
    T_TICK,
    B,
    INTRODUCER,
    HOST,
    SERVERS,
    INTRODUCER,
    DOCKER
)


class Server:
    def __init__(self, host: str, port: str) -> None:
        """
        Initialize the server
        """
        self.host = host
        self.port = port
        self.local_time = time.time()
        self.id = (self.host, self.port, self.local_time)

        self.udpServer = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udpServer.bind((self.host, self.port))

        self.tcpServer = tcpServer(self, self.host, self.port)

        self.leader = Leader()
        self.replicas = []

        self.introducer = (INTRODUCER[0], INTRODUCER[1])
        logging.info(f"[CONNECT TO HOST {self.host}]")

        self.membershipList = MembershipList(self.id, self.local_time)
        self.list_lock = threading.Lock()
        self.leader_lock = threading.Lock()
        self.logger = Logger("logs/membership.log")
        self.grepLogger = Logger("logs/grep.log")

        if self.host == INTRODUCER[0] and self.port == INTRODUCER[1]:
            self.changeMode("gossip")
        else:
            self.changeMode(self.getModefromIntroducer())

        # assume leader will not fail
        if self.host == INTRODUCER[0] and self.port == INTRODUCER[1]:
            self.setLeader(self.id)
        else:
            self.sendToServers()
            self.setLeader(self.getLeaderfromServers())

        self.isSetup = True
        self.isElecting = False

    def setLeader(self, leaderId):
        self.membershipList.setLeader(leaderId)
        if leaderId == self.id:
            with self.leader.activeCV:
                self.leader.isActive = True
                self.leader.activeCV.notify()

    @locked
    def _printMembershipList(self):
        """
        Print the membership list
        """
        self.membershipList.print()
        logging.info(f"f2a: {self.leader.fileToAddress}")

    def _leaveGroup(self):
        """
        Leave the group
        """
        logging.info("Leave the group")
        os._exit(0)

    def _changeModeAndSendMessage(self, mode):
        """
        Change the mode and send change mode message to other servers
        @param mode: new mode
        @param message: message to print
        """
        if self.mode != mode:
            logging.info(f"Switch to {mode}")
            self.changeMode(mode)
            self.sendChangeModeMessage(mode)

    @locked
    def _printSuspectedList(self):
        """
        Print the suspected list
        """
        self.membershipList.printSuspected()

    @locked
    def _merge_membership_list(self, membershipList, addr):
        self.membershipList.merge(membershipList, time.time(), addr, self.logger)

    def _waitAndDoFunc(self, func, timeout, *args, **kwargs):
        """
        Wait for timeout seconds and then call func
        @param func: function to call
        @param timeout: time to wait
        @param args: arguments for func
        @param kwargs: keyword arguments for func
        """
        while True:
            time.sleep(timeout)
            func(*args, **kwargs)

    @handle_exceptions
    def getModefromIntroducer(self):
        """
        Get the mode from the introducer using TCP
        """
        logging.info("Start getting mode from introducer...")
        try:
            with socket.create_connection(self.introducer) as s:
                packAndSendMsg(
                    s, MessageType.GET_MODE, "", MessageStatus.SUCCESS, MessageFlag.SYN
                )
                msg, _ = unpackAndReceiveMsg(s)
                if (
                    msg.type == MessageType.GET_MODE
                    and msg.status == MessageStatus.SUCCESS
                    and msg.flag == MessageFlag.ACK
                ):
                    logging.debug(f"System current mode: {msg.content}")
                else:
                    logging.debug(f"Cannot get mode from introducer")
                    raise Exception("Cannot get mode from introducer")
                return msg.content
        except Exception as e:
            logging.debug(f"Cannot connect to introducer: {e}")

    @handle_exceptions
    def getLeaderfromServers(self):
        """
        Get the mode from the introducer using TCP
        """
        logging.info("Start searching for leader...")
        for addr, port in SERVERS:
            logging.debug(f"Try to connect to {addr}")
            if addr == self.id[0] and port == self.id[1]:
                continue
            try:
                with socket.create_connection((addr, port)) as s:
                    s.settimeout(1)
                    packAndSendMsg(
                        s,
                        MessageType.GET_LEADER,
                        "",
                        MessageStatus.SUCCESS,
                        MessageFlag.SYN,
                    )
                    msg, _ = unpackAndReceiveMsg(s)
                    if (
                        msg.type == MessageType.GET_LEADER
                        and msg.status == MessageStatus.SUCCESS
                        and msg.flag == MessageFlag.ACK
                    ):
                        logging.debug(f"System current leader: {msg.content}")
                        return msg.content
                    else:
                        logging.debug(f"Cannot get leader from {addr}")
                        raise Exception("Cannot get leader from introducer")
            except Exception as e:
                logging.debug(f"Cannot connect to {addr}: {e}")

        logging.info(f"No leader found, set leader {self.id} to self")
        return self.id

    @handle_exceptions
    def sendToServers(self):
        """
        Send membershiplist to all servers
        """
        logging.info("Start sending membership list to all servers")
        msgType = MessageType.GOSSIP if self.mode == "gossip" else MessageType.GOSSIP_S
        for addr, port in SERVERS:
            logging.debug(f"Send initial membership list to {addr}")
            if addr == self.id[0] and port == self.id[1]:
                continue
            packAndSendMsg(
                self.udpServer,
                msgType,
                self.membershipList,
                MessageStatus.SUCCESS,
                udp=True,
                addr=(addr, port),
            )

    @handle_exceptions
    @locked
    def gossip(self, k):
        """
        Gossip with k other servers using UDP
        @param k: number of servers to gossip with
        """
        keys = list(self.membershipList.members.keys())
        keys.remove(self.id)
        if len(keys) == 0:
            return
        ids = choices(keys, k=k)
        ids = set([(id[0], id[1]) for id in ids])

        msgType = MessageType.GOSSIP if self.mode == "gossip" else MessageType.GOSSIP_S
        packAndSendMsgToAll(
            self.udpServer, msgType, self.membershipList, MessageStatus.SUCCESS, ids
        )

    @handle_exceptions
    def sendGossip(self):
        """
        Send heartbeat to k other servers every T_gossip seconds
        """
        self._waitAndDoFunc(self.gossip, T_GOSSIP, B)

    def updateReplicas(self):
        """
        Update the replicas of the server.
        """
        # if the server is the leader, it shouldn't be in the replicas
        if self.id in self.replicas:
            self.replicas.remove(self.id)

        aliveMembers = [id for id in self.membershipList.members.keys()]
        aliveMembers.remove(self.id)
        aliveMembers = set(aliveMembers)

        if len(aliveMembers.intersection(set(self.replicas))) == 3:
            return

        replicas_to_remove = []
        for replica in self.replicas:
            if replica not in aliveMembers:
                replicas_to_remove.append(replica)

        for replica in replicas_to_remove:
            self.replicas.remove(replica)

        if (
            len(aliveMembers.intersection(set(self.replicas))) < 3
            and len(aliveMembers) >= 3
        ):
            possibleReplicas = aliveMembers.difference(set(self.replicas))
            # sort possibleReplicas by id, which can be fetched by fetchIdinHostName
            possibleReplicas = sorted(
                possibleReplicas, key=lambda x: fetchIdinHostName(x[0])
            )
            newReplicas = []
            while len(self.replicas) < 3 and possibleReplicas:
                self.replicas.append(possibleReplicas.pop())
                newReplicas.append(self.replicas[-1])
            self.leader.updateLeaderData(newReplicas)
            logging.debug(f"Update replicas to {self.replicas}")

            # send replicas to other servers
            count = 0
            for addr, port, _ in self.membershipList.members.keys():
                if addr == self.id[0] and port == self.id[1]:
                    continue
                with socket.create_connection((addr, port)) as s:
                    packAndSendMsg(
                        s,
                        MessageType.SEND_RELICAS,
                        self.replicas,
                        MessageStatus.SUCCESS,
                        MessageFlag.SYN,
                    )
                    msg, _ = unpackAndReceiveMsg(s)
                    if (
                        msg.type == MessageType.SEND_RELICAS
                        and msg.status == MessageStatus.SUCCESS
                        and msg.flag == MessageFlag.ACK
                    ):
                        count += 1
            logging.debug(f"Send replicas to {count} servers successfully")

    def updateMetaData(self):
        """
        Update the meta data of the server
        """
        for fileName, addressSet in self.leader.fileToAddress.items():
            # the server that stores the file is dead, select a new server to store the file
            toBeRemoved = set()
            for addr in addressSet:
                if addr not in self.membershipList.members:
                    toBeRemoved.add(addr)
            logging.debug(f"original: {addressSet}") if toBeRemoved else None
            logging.debug(f"toBeRemoved: {toBeRemoved}") if toBeRemoved else None
            addressSet.difference_update(toBeRemoved)
            logging.debug(f"updated: {addressSet}") if toBeRemoved else None

            if len(addressSet) < 4 and fileName not in self.leader.filesToRecovery:
                logging.debug(f"Address set left {addressSet}")
                requestServers = list(addressSet)
                self.leader.insertTask(
                    requestServers, "put_recovery", (f"sdfs/{fileName}", fileName)
                )
                # self.leader.notify(requestServers, 'push', requestServers, 'put_recovery', (f'sdfs/{fileName}', fileName))
                self.leader.filesToRecovery.add(fileName)
                logging.debug(f"Updated TaskQueue {self.leader.taskQueue}")

    @handle_exceptions
    def leaderThread(self):
        @locked
        def helper(self, logger):
            if self.leader.isActive and not self.leader_lock.locked():
                self.leader_lock.acquire()
                # self.updateReplicas()
                self.updateMetaData()
                self.leader_lock.release()

        self._waitAndDoFunc(helper, T_TICK, self, self.logger)

    @handle_exceptions
    def tick(self):
        """
        Send heartbeat to k other servers every T_tick seconds
        """

        @locked
        def helper(self, logger):
            self.membershipList.updateStatus(time.time(), logger)
            if (
                not self.isSetup
                and not self.isElecting
                and not self.membershipList.getLeader()
            ):
                if self.leaderElection():
                    self.isElecting = True

        self._waitAndDoFunc(helper, T_TICK, self, self.logger)

    @handle_exceptions
    def hbUpdate(self):
        """
        Update the heartbeat of the server every T_hb seconds
        """

        @locked
        def helper(self, id):
            self.membershipList.updateHb(id, time.time())

        self._waitAndDoFunc(helper, T_HB, self, self.id)

    @handle_exceptions
    @locked
    def _sendGrepRequest(self, data):
        """
        Send grep to other servers
        @param data: grep command
        """
        ids = [
            (id[0], id[1])
            for id, member in self.membershipList.members.items()
            if member.status[0] == "alive"
        ]

        for addr, port in ids:
            try:
                with socket.create_connection((addr, port)) as s:
                    msg, _ = sendAndReceiveMsg(
                        s,
                        MessageType.GREP,
                        data,
                        MessageStatus.SUCCESS,
                        MessageFlag.SYN,
                    )
                    validate(msg, MessageType.GREP, MessageFlag.ACK)

                    self.grepLogger.log(f"===================={id}====================")
                    self.grepLogger.log(f"{msg.content}")
            except:
                logging.debug(f"Cannot connect to {addr}")

    @handle_exceptions
    def grep(self, conn, args):
        """
        Grep the log files
        @param data: grep command
        @param addr: address of the sender
        """
        ret = subprocess.run(
            " ".join(["grep", *args, "logs/membership.log"]),
            shell=True,
            stdout=subprocess.PIPE,
            universal_newlines=True,
        )

        packAndSendMsg(
            conn, MessageType.GREP, ret.stdout, MessageStatus.SUCCESS, MessageFlag.ACK
        )

    @handle_exceptions
    def receive(self):
        """
        Receive gossip from other servers
        """
        while True:
            msg, addr = unpackAndReceiveMsg(self.udpServer, udp=True)
            # addr is a tuple of (ip, port), we need to get the hostname to match the id in membership list
            addr = (socket.gethostbyaddr(addr[0])[0], addr[1])

            if msg.type == MessageType.GOSSIP or msg.type == MessageType.GOSSIP_S:
                self._merge_membership_list(msg.content, addr)
                self.isSetup = False

    @locked
    def changeMode(self, mode):
        """
        Change the mode of the server
        @param mode: new mode
        """
        self.membershipList.changeMode(mode)
        self.mode = mode
        logging.info(f"Current mode: {self.mode}")

    @handle_exceptions
    @locked
    def sendChangeLeaderMessage(self, leaderId):
        """
        Send change leader message to other servers
        """
        ids = list(self.membershipList.members.keys())
        ids.remove(self.id)
        for memberId, memberPort, _ in ids:
            with socket.create_connection((memberId, memberPort)) as s:
                packAndSendMsg(
                    s,
                    MessageType.CHANGE_LEADER,
                    leaderId,
                    MessageStatus.SUCCESS,
                    MessageFlag.SYN,
                )
        logging.info(
            f"Successfully change leader to {self.id} for {len(ids)+1} servers "
        )

    @handle_exceptions
    @locked
    def sendChangeModeMessage(self, mode):
        """
        Change the mode of the servers
        @param mode: new mode
        """
        ids = list(self.membershipList.members.keys())
        ids.remove(self.id)
        count = 1
        for memberId, memberPort, _ in ids:
            try:
                with socket.create_connection((memberId, memberPort)) as s:
                    msg, _ = sendAndReceiveMsg(
                        s,
                        MessageType.CHANGE_MODE,
                        mode,
                        MessageStatus.SUCCESS,
                        MessageFlag.SYN,
                    )
                    validate(msg, MessageType.CHANGE_MODE, MessageFlag.ACK)
                    count += 1
            except Exception as e:
                logging.error(f"Cannot change {memberId}'s mode: {e}")

        logging.info(f"Successfully change mode to {mode} for {count} servers")

    def leaderElection(self):
        possibleLeaders = sorted(self.replicas, key=lambda x: fetchIdinHostName(x[0]))
        highestServerAddr, highestServerPort, _ = possibleLeaders[-1]
        # Current server has the highest id
        if highestServerAddr == self.id[0] and highestServerPort == self.id[1]:
            pass
        # Elect to the highest id server
        else:
            logging.info(f"Try to elect {highestServerAddr} at {self.id}")
            try:
                with socket.create_connection(
                    (highestServerAddr, highestServerPort)
                ) as s:
                    packAndSendMsg(s, MessageType.ELECTION, "", MessageStatus.SUCCESS)
                    msg, addr = unpackAndReceiveMsg(s)
            except socket.gaierror:
                return False
        return True

    @locked
    def getFileStorageServers(self, sdfsFileName, num):
        """
        Get the file storage servers
        @param sdfsFileName: sdfs file name
        @param num: number of servers to get
        """
        addrs = set()
        # if the file is stored on the server
        if self.leader.fileToAddress.get(sdfsFileName):
            addrs = self.leader.fileToAddress.get(sdfsFileName)

        potentialServers = []
        for id, member in self.membershipList.members.items():
            if member.status[0] == "alive" and id not in addrs and id != self.id:
                potentialServers.append(id)

        potentialServers = sorted(
            potentialServers, key=lambda x: fetchIdinHostName(x[0])
        )

        hashValue = self.leader.getFileStorage(sdfsFileName, len(potentialServers))
        logging.debug(f"addrs before while {addrs}")

        i = 0
        while True:
            if len(addrs) == num:
                break
            addrs.add(potentialServers[(hashValue + i) % len(potentialServers)])
            i += 1
        return addrs

    def putFileRecoveyExecute(self, localFileName, sdfsFileName, sender):
        for _sender in sender:
            if (
                _sender in self.membershipList.members
                and self.membershipList.members[_sender].status[0] != "alive"
            ):
                continue
            if self.putFileExecute(localFileName, sdfsFileName, _sender):
                logging.info(f"Put file {sdfsFileName} successfully")
                logging.info(
                    f"self.leader.filesToRecovery {self.leader.filesToRecovery}"
                )
                break
        self.leader.filesToRecovery.remove(sdfsFileName)

    def putFileExecute(self, localFileName, sdfsFileName, sender):
        """
        Put file to the server
        @param localFileName: local file name
        @param sdfsFileName: sdfs file name
        """
        logging.info(f"Start to put file {localFileName} to {sdfsFileName}")

        addrs = self.getFileStorageServers(sdfsFileName, 4)
        logging.info(f"Put file {localFileName} to {addrs} from {sender}")
        count = 0
        self.leader_lock.acquire()
        try:
            with socket.create_connection((sender[0], sender[1])) as s:
                msg, _ = sendAndReceiveMsg(
                    s,
                    MessageType.PUT_FILE_REQUEST,
                    (addrs, localFileName, sdfsFileName),
                    MessageStatus.SUCCESS,
                    MessageFlag.SYN,
                )
                validate(msg, MessageType.PUT_FILE_REQUEST, MessageFlag.ACK)

                for _ in range(4):
                    msg, _ = unpackAndReceiveMsg(s)
                    validate(msg, MessageType.PUT_FILE_COMPLETE, MessageFlag.SYN)
                    self.leader.fileToAddress[msg.content[2]].add(msg.content[1])
                    count += 1
                    packAndSendMsg(
                        s,
                        MessageType.PUT_FILE_COMPLETE,
                        "",
                        MessageStatus.SUCCESS,
                        MessageFlag.ACK,
                    )
        except Exception as e:
            logging.error(f"Cannot connect to {sender} or {addrs}: {e}")
            self.leader_lock.release()
            return False
        logging.info(f"Put file {localFileName} to {addrs} from {sender} successfully")
        self.leader_lock.release()
        return count == 4

    def getFileExecute(self, sdfsFileName, localFileName, sender):
        """
        Get file from the server
        @param localFileName: local file name
        @param sdfsFileName: sdfs file name
        """
        addresses = self.leader.fileToAddress.get(sdfsFileName)
        try:
            with socket.create_connection((sender[0], sender[1])) as s:
                msg, _ = sendAndReceiveMsg(
                    s,
                    MessageType.GET_FILE_REQUEST,
                    (localFileName, sdfsFileName, addresses),
                    MessageStatus.SUCCESS,
                    MessageFlag.SYN,
                )
                validate(msg, MessageType.GET_FILE_REQUEST, MessageFlag.ACK)
                msg, _ = unpackAndReceiveMsg(s)
                validate(msg, MessageType.GET_FILE_COMPLETE, MessageFlag.SYN)
                packAndSendMsg(
                    s,
                    MessageType.GET_FILE_COMPLETE,
                    "",
                    MessageStatus.SUCCESS,
                    MessageFlag.ACK,
                )
                logging.info(f"Get file {localFileName} at {sdfsFileName} successfully")
        except Exception as e:
            logging.error(f"Cannot connect to {sender} in get file request: {e}")

    def multireadFileExecuteThread(
        self, vm, sdfsFileName, localFileName, addresses, sempahore
    ):
        vmIp, vmPort = SERVERS[vm - 1]
        sempahore.acquire()
        logging.info(f"Start thread {vm} to get file {sdfsFileName}")
        try:
            with socket.create_connection((vmIp, vmPort)) as s:
                msg, _ = sendAndReceiveMsg(
                    s,
                    MessageType.GET_FILE_REQUEST,
                    (localFileName, sdfsFileName, addresses),
                    MessageStatus.SUCCESS,
                    MessageFlag.SYN,
                )
                validate(msg, MessageType.GET_FILE_REQUEST, MessageFlag.ACK)

                msg, _ = unpackAndReceiveMsg(s)
                validate(msg, MessageType.GET_FILE_COMPLETE, MessageFlag.SYN)
                packAndSendMsg(
                    s,
                    MessageType.GET_FILE_COMPLETE,
                    "",
                    MessageStatus.SUCCESS,
                    MessageFlag.ACK,
                )
        except Exception as e:
            logging.info(f"Cannot connect to {vmIp} {vmPort} in multiread file request")

        sempahore.release()
        logging.debug(f"semphore count {sempahore._value}")

    def multireadFileExecute(self, sdfsFileName, localFileName, vms, sender):
        addresses = self.leader.fileToAddress.get(sdfsFileName)
        logging.debug(f"Start multiread file which resides in {addresses}")
        sempahore = threading.Semaphore(2)
        threads = []
        for vm in vms:
            threads.append(
                threading.Thread(
                    target=self.multireadFileExecuteThread,
                    args=(int(vm), sdfsFileName, localFileName, addresses, sempahore),
                )
            )
            threads[-1].start()

        for thread in threads:
            thread.join()
        logging.debug(f"Finish multiread file {sdfsFileName}")

    def deleteFileExecute(self, sdfsFileName, sender):
        """
        Delete file from the server
        @param sdfsFileName: sdfs file name
        """
        addresses = self.leader.fileToAddress[sdfsFileName]
        errorServers = []
        for addr in addresses:
            try:
                with socket.create_connection((addr[0], addr[1])) as s:
                    msg, _ = sendAndReceiveMsg(
                        s,
                        MessageType.DELETE_FILE_REQUEST,
                        sdfsFileName,
                        MessageStatus.SUCCESS,
                        MessageFlag.SYN,
                    )
                    validate(msg, MessageType.DELETE_FILE_REQUEST, MessageFlag.ACK)
                    msg, _ = unpackAndReceiveMsg(s)
                    validate(msg, MessageType.DELETE_FILE, MessageFlag.SYN)
                    packAndSendMsg(
                        s,
                        MessageType.DELETE_FILE,
                        "",
                        MessageStatus.SUCCESS,
                        MessageFlag.ACK,
                    )
            except Exception as e:
                logging.error(f"Cannot connect to {addr} in delete file request: {e}")
                errorServers.append(addr)
        if len(errorServers) == 0:
            self.leader.fileToAddress.pop(sdfsFileName)
            logging.debug(f"Delete file {sdfsFileName} successfully")
        else:
            logging.error(f"Cannot delete file {sdfsFileName} at {errorServers}")

    def listFileExecute(self, sdfsFileName, sender):
        """
        List file from the server
        @param sdfsFileName: sdfs file name
        """
        try:
            with socket.create_connection((sender[0], sender[1])) as s:
                content = self.leader.fileToAddress.get(sdfsFileName)
                msg, _ = sendAndReceiveMsg(
                    s,
                    MessageType.LIST_FILE,
                    (content, sdfsFileName),
                    MessageStatus.SUCCESS,
                    MessageFlag.SYN,
                )
                validate(msg, MessageType.LIST_FILE, MessageFlag.ACK)
        except Exception as e:
            logging.error(f"List {sdfsFileName} error: {e}")

    def storeFileExeuction(self, sender):
        """
        Store file from the server
        @param sdfsFileName: sdfs file name
        """
        try:
            with socket.create_connection((sender[0], sender[1])) as s:
                store = []
                for fileName, addressSet in self.leader.fileToAddress.items():
                    if sender in addressSet:
                        store.append(fileName)

                msg, _ = sendAndReceiveMsg(
                    s,
                    MessageType.STORE_FILE,
                    "",
                    MessageStatus.SUCCESS,
                    MessageFlag.SYN,
                )
                validate(msg, MessageType.STORE_FILE, MessageFlag.ACK)
        except Exception as e:
            logging.error(f"Store file request error: {e}")

    def mapleThread(self, i, partition, addr, maple_exe, args, fileName, lock):
        """
        Maple the files
        @param maple_exe: maple executable
        @param partition: partition
        @param addr: address
        @param args: arguments for maple
        @param fileName: file name
        """
        logging.info(f"Start maple thread {i} {maple_exe} {addr} {args} {fileName}")
        try:
            with socket.create_connection((addr[0], addr[1])) as s:
                msg, _ = sendAndReceiveMsg(
                    s,
                    MessageType.MAPLE,
                    (maple_exe, args, partition),
                    MessageStatus.SUCCESS,
                    MessageFlag.SYN,
                )
                validate(msg, MessageType.MAPLE, MessageFlag.ACK)

                # receive file
                msg, _ = unpackAndReceiveMsg(s)
                validate(msg, MessageType.MAPLE_COMPLETE, MessageFlag.SYN)
                packAndSendMsg(
                    s,
                    MessageType.MAPLE_COMPLETE,
                    "",
                    MessageStatus.SUCCESS,
                    MessageFlag.ACK,
                )

                # write file in local
                content = msg.content.strip()
                content = content.split("\n")
                lock.acquire()
                for line in content:
                    res = line.split("\t")
                    if len(res) != 2:
                        continue
                    key, value = res
                    if key not in self.key_file:
                        self.key_file[key] = []
                    self.key_file[key].append(value)
                lock.release()
                self.mapleThreadMaps[i] = True
        except Exception as e:
            logging.error(f"Maple request error: {e}")
            self.mapleThreadMaps[i] = False

    def mapleExecute(
        self,
        maple_exe,
        num_maples,
        sdfs_intermediate_filename_prefix,
        sdfs_src_directory,
        args,
        sender,
    ):
        """
        Maple the files
        @param maple_exe: maple executable
        @param num_maples: number of maples
        @param sdfs_intermediate_filename_prefix: sdfs intermediate file name prefix
        @param sdfs_src_directory: sdfs source directory
        """
        logging.info(
            f"Start maple {maple_exe} {num_maples} {sdfs_intermediate_filename_prefix} {sdfs_src_directory} {args}"
        )
        try:
            # filetoaddress
            fileName = os.listdir(f"sdfs/{sdfs_src_directory}")[0]
            logging.debug(f"maple fileName {fileName}")
            with open(f"sdfs/{sdfs_src_directory}/{fileName}", "r") as f:
                content = f.read()

            num_lines = len(content.split("\n"))
            logging.debug(f"num_lines {num_lines}")

            # partition
            content = content.split("\n")
            num_maples = int(num_maples)
            partition_size = num_lines // num_maples
            logging.debug(f"partition_size {partition_size}")
            partitions = []
            if partition_size != 0:
                for i in range(num_maples):
                    partitions.append(
                        content[i * partition_size : (i + 1) * partition_size]
                    )
                partitions[-1].extend(content[num_maples * partition_size :])
            else:
                partitions.append(content)
            # logging.debug(f'partitions {partitions}')
            
            candidates = []
            for id, member in self.membershipList.members.items():
                if member.status[0] == "alive":
                    candidates.append(id)
            candidates.remove(self.id)
            
            self.key_file = {}

            while len(partitions) > 0:
                logging.info(f"Number of candidates {len(candidates)}")
                logging.info(f"Number of partitions {len(partitions)}")
                self.mapleThreadMaps = {}
                lock = threading.Lock()
                threads = []
                iterations = min(len(partitions), len(candidates))
                for i in range(iterations):
                    logging.debug(f"partition {i} {partitions[i]}")
                    addr = candidates[i]
                    t = threading.Thread(
                        target=self.mapleThread,
                        args=(i, partitions[i], addr, maple_exe, args, fileName, lock),
                    )
                    t.start()
                    threads.append(t)
                
                # wait for all threads to finish
                for t in threads:
                    t.join()
                
                partitionFinished = []
                candidatesRemove = []
                for i, success in self.mapleThreadMaps.items():
                    if not success:
                        candidatesRemove.append(candidates[i])
                    else:
                        partitionFinished.append(partitions[i])
                
                for candidate in candidatesRemove:
                    candidates.remove(candidate)
                
                for partition in partitionFinished:
                    partitions.remove(partition)


            for key, values in self.key_file.items():
                logging.debug(f"{key} {values}")

                with open(f"{sdfs_intermediate_filename_prefix}_{key}", "a+") as f:
                    for value in values:
                        f.write(f"{key}\t{value}\n")

                # TODO: can send to leader itself?
                self.putFileExecute(
                    f"{sdfs_intermediate_filename_prefix}_{key}",
                    f"{sdfs_intermediate_filename_prefix}_{key}",
                    self.id,
                )

        except Exception as e:
            logging.error(f"Maple request error: {e}")

    def juiceThread(self, i, files, addr, juice_exe, args, f, lock):
        """
        Handle juice
        @param files: files
        @param fileName: file name
        @param addr: address
        @param juice_exe: juice executable
        @param args: arguments for juice
        @param f: file
        """
        logging.info(f"Start juice thread {files} {addr} {juice_exe} {args} {f}")
        try:
            with socket.create_connection((addr[0], addr[1])) as s:
                # Send message length
                msg, _ = sendAndReceiveMsg(
                    s,
                    MessageType.JUICE,
                    len(files),
                    MessageStatus.SUCCESS,
                    MessageFlag.SYN,
                )
                validate(msg, MessageType.JUICE, MessageFlag.ACK)
                contents = []

                # files = [string, string]
                for file in files:
                    # Send juice arguments and wait for ack
                    msg, _ = sendAndReceiveMsg(
                        s,
                        MessageType.JUICE,
                        (juice_exe, file),
                        MessageStatus.SUCCESS,
                        MessageFlag.SYN,
                    )
                    validate(msg, MessageType.JUICE, MessageFlag.ACK)

                    # Wait for juice response and send ack
                    msg, _ = unpackAndReceiveMsg(s)
                    validate(msg, MessageType.JUICE, MessageFlag.SYN)
                    packAndSendMsg(
                        s, MessageType.JUICE, "", MessageStatus.SUCCESS, MessageFlag.ACK
                    )

                    content = msg.content
                    content = content.split("\n")
                    contents.extend(content)

                # Wait for juice complete and send ack
                msg, _ = unpackAndReceiveMsg(s)
                validate(msg, MessageType.JUICE_COMPLETE, MessageFlag.SYN)
                packAndSendMsg(
                    s,
                    MessageType.JUICE_COMPLETE,
                    "",
                    MessageStatus.SUCCESS,
                    MessageFlag.ACK,
                )
                lock.acquire()
                logging.info(contents)
                with open(f, "a+") as f:
                    for content in contents:
                        if not content:
                            continue
                        f.write(content + "\n")
                self.juiceThreadMaps[i] = True
                lock.release()

        except Exception as e:
            logging.error(f"Juice request error: {e}")
            self.juiceThreadMaps[i] = False

    def juiceExecute(self, juice_exe, num_juices, sdfs_intermediate_filename_prefix, sdfs_dest_filename, delete_input, args, sender):
        """
        Juice the files
        @param juice_exe: juice executable
        @param num_juices: number of juices
        @param sdfs_intermediate_filename_prefix: sdfs intermediate file name prefix
        @param sdfs_dest_filename: sdfs destination file name
        @param delete_input: delete input or not
        @param args: arguments for juice
        """
        logging.info(
            f"Start juice {juice_exe} {num_juices} {sdfs_intermediate_filename_prefix} {sdfs_dest_filename} {delete_input} {args}"
        )
        try:
            # get files
            same_prefix_files = []
            allFile = self.leader.fileToAddress.keys()
            for fileName in allFile:
                if fileName.startswith(sdfs_intermediate_filename_prefix):
                    same_prefix_files.append(fileName)

            # partition files to num_juices
            num_files = len(same_prefix_files)
            num_juices = int(num_juices)
            partition_size = num_files // num_juices
            partitions_by_fileName = []
            if partition_size != 0:
                for i in range(num_juices):
                    partitions_by_fileName.append(
                        same_prefix_files[i * partition_size : (i + 1) * partition_size]
                    )
                partitions_by_fileName[-1].extend(
                    same_prefix_files[num_juices * partition_size :]
                )
            else:
                partitions_by_fileName.append(same_prefix_files)
            logging.debug(f"partitions {partitions_by_fileName}")
            
            contents = []
            for parition in partitions_by_fileName:
                contents.append([])
                for fileName in parition:
                    with open(f"{fileName}", "r") as f:
                        contents[-1].append(f.read())

            candidates = []
            for id, member in self.membershipList.members.items():
                if member.status[0] == "alive":
                    candidates.append(id)
            candidates.remove(self.id)
            
            # send partitions to servers
            
            while len(contents) > 0:
                logging.info(f"Number of candidates {len(candidates)}")
                logging.info(f"Number of contents {len(contents)}")
                self.juiceThreadMaps = {}
                lock = threading.Lock()
                threads = []
                iterations = min(len(contents), len(candidates))
                for i in range(iterations):
                    addr = candidates[i]
                    t = threading.Thread(
                        target=self.juiceThread,
                        args=(i, contents[i], addr, juice_exe, args, sdfs_dest_filename, lock),
                    )
                    t.start()
                    threads.append(t)
                
                # wait for all threads to finish
                for t in threads:
                    t.join()

                contentsFinished = []
                candidatesRemove = []
                for i, success in self.juiceThreadMaps.items():
                    if not success:
                        candidatesRemove.append(candidates[i])
                    else:
                        contentsFinished.append(contents[i])
                
                for candidate in candidatesRemove:
                    candidates.remove(candidate)

                for content in contentsFinished:
                    contents.remove(content)
                    
            logging.info("Finish juice threads")
            if int(delete_input):
                for fileName in same_prefix_files:
                    self.deleteFileExecute(fileName, self.id)
                    os.remove(fileName)
            # f.close()

        except Exception as e:
            logging.error(f"Juice request error: {e}")

    def filterExecute(self, commandLineArgs, sender):
        logging.info(f"Start filter {commandLineArgs} {sender}")
        # commandLineArgs = SELECT ALL FROM Traffic_Signal_Intersections.geojson where "Video|Radio"
        try:
            dataSetName = commandLineArgs[3]
            regex = commandLineArgs[5]
            fileName = commandLineArgs[-1]
            logging.info(f"Start filter {dataSetName} {regex}")
            nonce = random.randint(0, 100000)
            self.mapleExecute('filterMaple.py', 3, f'intermediate_filter_{nonce}', dataSetName, regex, sender)
            self.juiceExecute('filterJuice.py', 3, f'intermediate_filter_{nonce}', fileName, 0, '', sender)
        except Exception as e:
            logging.error(f"Filter request error: {e}")

    def joinExecute(self, commandLineArgs, sender):
        """
        Join the group
        """
        logging.info(f"Start join at {self.id}")
        # join SELECT ALL FROM D1 D2 WHERE "D1.name=D2.ID out_join.txt"
        try:
            nonce = random.randint(0, 100000)
            dataset1 = commandLineArgs[3][:-1]
            dataset2 = commandLineArgs[4]
            args = commandLineArgs[6]
            schema1, schema2 = args.split("=")
            fileName = commandLineArgs[-1]
            
            self.mapleExecute(
                "joinMaple.py",
                3,
                f"intermediate_join_{nonce}",
                dataset1,
                schema1,
                sender,
            )

            self.mapleExecute(
                "joinMaple.py",
                3,
                f"intermediate_join_{nonce}",
                dataset2,
                schema2,
                sender,
            )

            self.juiceExecute(
                "joinJuice.py",
                3,
                f"intermediate_join_{nonce}",
                fileName,
                0,
                args,
                sender,
            )
        except Exception as e:
            logging.error(f"Join request error: {e}")

        logging.info(f"Finish join at {self.id}")

    def execution(self):
        """
        Execute the task in the task queue
        """
        task_map = {
            "put": self.putFileExecute,
            "get": self.getFileExecute,
            "delete": self.deleteFileExecute,
            "ls": self.listFileExecute,
            "store": self.storeFileExeuction,
            "put_recovery": self.putFileRecoveyExecute,
            "multiread": self.multireadFileExecute,
            "maple": self.mapleExecute,
            "juice": self.juiceExecute,
            "filter": self.filterExecute,
            "join": self.joinExecute,
        }
        while True:
            with self.leader.activeCV:
                self.leader.activeCV.wait_for(lambda: self.leader.isActive)

            with self.leader.cv:
                # no task in the queue
                self.leader.cv.wait_for(lambda: self.leader.size() > 0)

                task = self.leader.frontTask()
                logging.info(f"Execute task {task} at {time.time()}")
                sender = task["sender"]
                taskType = task["type"]
                args = task["args"]
                args = args[0][0]

                task_func = task_map.get(taskType)
                if taskType == "filter":
                    self.filterExecute(args, sender)
                elif taskType == "join":
                    self.joinExecute(args, sender)
                else:
                    # get the arguments for the task
                    argsName = inspect.getfullargspec(task_func).args
                    logging.info(
                        f"Execute task {taskType} {args} at {time.time()}, {task_func}, {argsName}"
                    )
                    argsName.remove("self")
                    argsName.remove("sender")

                    # use zip to convert the args to a dict
                    if len(argsName) == 0:
                        kargs = {}
                    else:
                        kargs = dict(zip(argsName, args))

                    # special case for multiread
                    if taskType == "multiread":
                        kargs["vms"] = args[2:]

                    if task_func:
                        task_func(**kargs, sender=sender)
                    else:
                        logging.error(f"Invalid task type {taskType}")

                self.leader.popTask()
                self.leader.notify(self.replicas, "pop")

    def startThreads(self) -> None:
        """
        Start the server threads
        """
        threads = [
            threading.Thread(
                target=self.tcpServer.serve_forever,
                daemon=True,
                name="tcpServer_thread",
            ),
            threading.Thread(target=self.sendGossip, daemon=True, name="gossip_thread"),
            threading.Thread(
                target=self.hbUpdate, daemon=True, name="heartbeat_thread"
            ),
            threading.Thread(target=self.tick, daemon=True, name="tick_thread"),
            threading.Thread(target=self.receive, daemon=True, name="receiver_thread"),
            threading.Thread(
                target=self.execution, daemon=True, name="execution_thread"
            ),
            threading.Thread(
                target=self.leaderThread, daemon=True, name="leader_thread"
            ),
        ]
        for thread in threads:
            thread.start()

    def sendOperation(self, operation, args=None):
        """
        Send operation to other servers
        @param operation: operation type
        @param args: arguments for the operation
        """
        leaderId = self.membershipList.getLeader()
        with socket.create_connection((leaderId[0], leaderId[1])) as s:
            logging.debug(f"Send operation: {operation} args: {args} to {leaderId}")
            packAndSendMsg(
                s,
                MessageType.SEND_OPS,
                (self.id, operation, args),
                MessageStatus.SUCCESS,
                MessageFlag.SYN,
            )
            msg, _ = unpackAndReceiveMsg(s)
            if (
                msg.type == MessageType.SEND_OPS
                and msg.status == MessageStatus.SUCCESS
                and msg.flag == MessageFlag.ACK
            ):
                logging.debug(f"Operation {operation} submit successfully")
            else:
                logging.error(f"Operation {operation} submit failed")

    def handleUserInput(self):
        """
        Handle user input in the main thread
        """
        if DOCKER:
            sys.stdin = open("pipe", "r")

        actions = {
            "list_mem": lambda: self._printMembershipList(),
            "list_self": lambda: logging.info(self.id),
            "print_leader": lambda: logging.info(self.membershipList.getLeader()),
            "leave": lambda: self._leaveGroup(),
            "enable_suspicion": lambda: self._changeModeAndSendMessage("gossip_s"),
            "disable_suspicion": lambda: self._changeModeAndSendMessage("gossip"),
            "display_suspected": lambda: self._printSuspectedList(),
            "grep": lambda args: self._sendGrepRequest(args),
            "put": lambda args: self.sendOperation("put", args),
            "get": lambda args: self.sendOperation("get", args),
            "delete": lambda args: self.sendOperation("delete", args),
            "ls": lambda args: self.sendOperation("ls", args),
            "store": lambda: self.sendOperation("store"),
            "multiread": lambda args: self.sendOperation("multiread", args),
            "maple": lambda args: self.sendOperation("maple", args),
            "juice": lambda args: self.sendOperation("juice", args),
            "filter": lambda args: self.sendOperation("filter", args),
            "join": lambda args: self.sendOperation("join", args),
        }
        while True:
            try:
                # docker exec -it server$1 bash -c "cd .. && echo $2 > pipe"
                command = input().split(" ")
                # for i in range(len(command)):
                #     command[i] = command[i].replace("_", " ")
                logging.debug(f"Command received: {command}")
                action = actions.get(
                    command[0], lambda: logging.error("Invalid command")
                )
                if len(command) > 1:
                    args = command[1:]
                    action(args)
                else:
                    action()
            except EOFError:
                # receive EOF, reopen the pipe
                if DOCKER:
                    sys.stdin = open("pipe", "r")
                pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action="store_true", default=False)
    args = parser.parse_args()
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    s = Server(HOST, PORT)
    s.startThreads()
    s.handleUserInput()
