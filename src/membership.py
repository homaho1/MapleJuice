from config import T_FAIL, T_CLEANUP, T_SUSPECT
from helper import fetchIdinHostName
import logging


class Membership:
    '''
    A class to represent a member in the membership list
    '''

    def __init__(self, hb, status, id, local_time) -> None:
        '''
        init the member
        @param hb: heartbeat
        @param status: status of the member
        @param id: id of the member
        @param local_time: local time of the member
        '''
        self.hb = hb
        self.status = status
        self.id = id
        self.local_time = local_time

    def __repr__(self) -> str:
        return f'Membership(hb={self.hb}, status={self.status}, id={self.id}, local_time={self.local_time})'


class MembershipList:
    '''
    A class to represent the membership list
    '''

    def __init__(self, ID, local_time) -> None:
        '''
        init the membership list
        @param ID: ID of the server
        '''
        self.members = {}
        self.leaderID = None
        self.serverID = ID
        self.replicas = []
        self.mode = 'gossip'
        print(f'init membership list with server ID {self.serverID}')
        self.members[self.serverID] = Membership(
            hb=0, status=('alive', 0), id=self.serverID, local_time=local_time)
        self.quorum = len(self.members) // 2 + 1

    def size(self):
        return len(self.members)

    def setLeader(self, leaderID):
        '''
        Set the leader of the system to be leaderID
        '''
        self.leaderID = leaderID

    def getLeader(self):
        return self.leaderID

    def getHighestId(self):
        highest = self.serverID
        for id, _ in self.members.items():
            if int(fetchIdinHostName(id[0])) > int(fetchIdinHostName(highest[0])):
                highest = id
        return highest

    def changeMode(self, mode) -> None:
        '''
        Change the mode of the server
        @param mode: new mode of the server
        '''
        logging.debug(
            f'change mode from {self.mode} to {mode} printing at membership.py')
        '''
        Remove all failed or suspected members from the membership list
        '''
        memberToRemove = []
        for id, member in self.members.items():
            if member.status[0] == 'failed' or member.status[0] == 'suspected':
                memberToRemove.append(id)
        logging.debug(f'Remove {len(memberToRemove)} processes')
        for id in memberToRemove:
            del self.members[id]
        self.mode = mode

    def add(self, id, local_time) -> None:
        '''
        Create a new member
        @param id: id of the member
        @param local_time: local time of the member
        '''
        self.members[id] = Membership(
            hb=0, status=('alive', 0), id=id, local_time=local_time)

    def merge_gossip(self, new_member_list, local_time, send_addr, logger) -> None:
        '''
        Merge new_members with self.members
        @param new_member_list: new membership list
        @param local_time: local time of the member
        @param send_addr: address of the sender
        @param logger: logger
        '''
        sender_IP, sender_port = send_addr
        for id, newMember in new_member_list.members.items():
            if id not in self.members:
                if newMember.status[0] == 'alive':
                    self.add(id, local_time)
                    # logger.log(
                    #     f'{local_time}: Add {fetchIdinHostName(id[0])} to membership list')
                    # logging.info(
                    #     f'{local_time}: Add {fetchIdinHostName(id[0])} to membership list')
            else:
                if newMember.hb > self.members[id].hb and (self.members[id].status[0] != 'failed' or (sender_IP == self.members[id].id[0] and sender_port == self.members[id].id[1])):
                    if self.members[id].status[0] != newMember.status[0]:
                        logger.log(
                            f'{local_time}: {fetchIdinHostName(id[0])} {self.members[id].status[0]} -> {newMember.status[0]} from merge_gossip')
                        logging.info(
                            f'{local_time}: {fetchIdinHostName(id[0])} {self.members[id].status[0]} -> {newMember.status[0]} from merge_gossip')

                    self.members[id].hb = newMember.hb
                    self.members[id].status = newMember.status
                    self.members[id].local_time = local_time

    def updateStatus_gossip_s(self, curr_time, logger) -> None:
        '''
        Update the status of the member with id
        @param curr_time: current time
        @param logger: logger
        '''
        memberToRemove = []
        for id, member in self.members.items():
            time_diff = curr_time - member.local_time
            if id == self.serverID:
                continue
            if time_diff > T_SUSPECT + T_FAIL:
                memberToRemove.append(id)
                logger.log(f'{curr_time}: {fetchIdinHostName(id[0])} removed')
                logging.info(
                    f'{curr_time}: {fetchIdinHostName(id[0])} removed')
            elif time_diff > T_SUSPECT:
                member.status = ('suspected', member.status[1])
                logger.log(
                    f'{curr_time}: {fetchIdinHostName(id[0])} suspected')
                logging.info(
                    f'{curr_time}: {fetchIdinHostName(self.serverID[0])} suspects {fetchIdinHostName(id[0])} in update gossip_s')

        for id in memberToRemove:
            logging.debug(f"popping id: {id}")
            del self.members[id]
            logging.debug(
                f'after remove, number of members: {len(self.members)}')

        for id in memberToRemove:
            if id == self.leaderID:
                logging.info(
                    f'{curr_time}: {fetchIdinHostName(id[0])} is removed, leader is set to None')
                self.leaderID = None
                break

        self.quorum = len(self.members) // 2 + 1

    def merge_gossip_s(self, new_member_list, local_time, send_addr, logger) -> None:
        '''
        Merge new_members with self.members
        @param new_member_list: new membership list
        @param local_time: local time of the member
        @param send_addr: address of the sender
        @param logger: logger
        '''
        for id, newMember in new_member_list.members.items():
            if id not in self.members:
                if newMember.status[0] == 'alive':
                    self.add(id, local_time)
                    # logger.log(
                    #     f'{local_time}: Add {fetchIdinHostName(id[0])} to membership list')
                    # logging.info(
                    #     f'{local_time}: Add {fetchIdinHostName(id[0])} to membership list')
            else:
                if id == self.serverID:
                    if newMember.status[0] == 'suspected' and newMember.status[1] == self.members[id].status[1]:
                        self.members[id].status = (
                            'alive', newMember.status[1]+1)
                        logging.info(
                            f'{local_time}: {fetchIdinHostName(id[0])} received a suspected message of itself, update its status to alive and increment the incarnation number to {newMember.status[1]+1}')
                        logger.log(
                            f'{local_time}: {fetchIdinHostName(id[0])} received a suspected message of itself, update its status to alive and increment the incarnation number to {newMember.status[1]+1}')
                    continue

                # higher incarnation number overrides lower one
                if newMember.status[1] > self.members[id].status[1]:
                    logging.info(
                        f'{local_time}: {fetchIdinHostName(id[0])} {self.members[id].status} -> {newMember.status} from merge_gossip_s')
                    logger.log(
                        f'{local_time}: {fetchIdinHostName(id[0])} {self.members[id].status} -> {newMember.status} from merge_gossip_s')
                    self.members[id].status = newMember.status
                    self.members[id].hb = newMember.hb
                    self.members[id].local_time = local_time
                    continue

                if newMember.hb <= self.members[id].hb:
                    logging.debug(
                        f'{local_time}: {fetchIdinHostName(id[0])} heartbeat {newMember.hb} < {self.members[id].hb}, ignore')
                    continue

                if newMember.status[0] == 'suspected' and self.members[id].status[1] == 'alive' and newMember.status[1] == self.members[id].status[1]:
                    self.members[id].status = (
                        'suspected', self.members[id].status[1])
                    logger.log(
                        f'{local_time}: {fetchIdinHostName(id[0])} received a suspected message with same incarnation number {self.members[id].status[1]}, update its status to suspected')
                    logging.info(
                        f'{local_time}: {fetchIdinHostName(id[0])} received a suspected message with same incarnation number {self.members[id].status[1]}, update its status to suspected')

                if newMember.hb > self.members[id].hb:
                    self.members[id].hb = newMember.hb
                    self.members[id].local_time = local_time
                    logging.debug(
                        f'{local_time}: {fetchIdinHostName(id[0])} regular merge')

    def merge(self, new_member_list, local_time, send_addr, logger) -> None:
        '''
        Merge new_members with self.members
        @param new_member_list: new membership list
        @param local_time: local time of the member
        @param send_addr: address of the sender
        @param logger: logger
        '''
        if self.mode == 'gossip':
            self.merge_gossip(new_member_list, local_time, send_addr, logger)
        elif self.mode == 'gossip_s':
            self.merge_gossip_s(new_member_list, local_time, send_addr, logger)
        else:
            logging.error("Error: unknown mode")

    def updateHb(self, id, local_time) -> None:
        '''
        Update the heartbeat of the member with id
        @param id: id of the member
        @param local_time: local time of the member
        @param logger: logger
        '''
        self.members[id].hb += 1
        self.members[id].local_time = local_time
        self.members[id].status = ('alive', self.members[id].status[1])

    def updateStatus_gossip(self, curr_time, logger) -> None:
        '''
        Update the status of the member with id
        @param curr_time: current time
        @param logger: logger
        '''
        memberToRemove = []
        for id, member in self.members.items():
            if id == self.serverID:
                continue
            if curr_time - member.local_time > T_FAIL + T_CLEANUP:
                memberToRemove.append(id)
                logger.log(f'{curr_time}: {fetchIdinHostName(id[0])} removed')
                logging.info(
                    f'{curr_time}: {fetchIdinHostName(id[0])} removed')

            elif member.status[0] == 'alive' and curr_time - member.local_time > T_FAIL:
                tmp = member.status
                member.status = ('failed', member.status[1])
                logger.log(
                    f'{curr_time}: {fetchIdinHostName(id[0])} {tmp[0]} -> {self.members[id].status[0]} from update gossip')
                logging.info(
                    f'{curr_time}: {fetchIdinHostName(id[0])} {tmp[0]} -> {self.members[id].status[0]} from update gossip')

        for id in memberToRemove:
            logging.debug(f"popping id: {id}")
            del self.members[id]
            logging.debug(
                f'after remove, number of members: {len(self.members)}')

        for id in memberToRemove:
            if id == self.leaderID:
                logging.info(
                    f'{curr_time}: {fetchIdinHostName(id[0])} is removed, leader is set to None')
                self.leaderID = None
                break

        self.quorum = len(self.members) // 2 + 1

    def updateStatus(self, curr_time, logger) -> None:
        '''
        Update the status of the member with id
        @param curr_time: current time
        @param logger: logger
        '''
        if self.mode == 'gossip':
            self.updateStatus_gossip(curr_time, logger)
        elif self.mode == 'gossip_s':
            self.updateStatus_gossip_s(curr_time, logger)
        else:
            logging.error("Error: unknown mode")

    def print(self):
        '''
        Print the membership list
        '''
        l = self.members.items()
        l = sorted(l, key=lambda x: x[0][0][13:15])
        for id, member in l:
            print(id, member)
        print("leaderID: ", self.leaderID)

    def printSuspected(self):
        '''
        Print the suspected members in the membership list
        '''
        l = self.members.items()
        l = sorted(l, key=lambda x: x[0][0][13:15])
        for id, member in l:
            if member.status[0] == 'suspected':
                print(id, member)
