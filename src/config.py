import socket

DOCKER = False
PORT = 23456
QUERY_SIZE = 4096
T_FAIL = 10
T_CLEANUP = 10
T_SUSPECT = 10
T_HB = 1
T_GOSSIP = 1
T_TICK = 1
B = 5

# HOST = "127.0.0.1" if LOCAL else socket.gethostname()
# INTRODUCER = ("127.0.0.1", PORT) if LOCAL else ("fa23-cs425-9301.cs.illinois.edu", PORT)

HOST = socket.gethostname()
INTRODUCER = ("server01.mp1_default", PORT) if DOCKER else (
    "fa23-cs425-9301.cs.illinois.edu", PORT)


SERVERS = [(f"server{i:02}.mp1_default", PORT) if DOCKER else (
    f"fa23-cs425-93{i:02}.cs.illinois.edu", PORT) for i in range(1, 11)]  # 01-10 with padding
