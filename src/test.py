import unittest
import subprocess
import os
import random
import time
import docker
import re
import datetime
import uuid

TEST_SERVER_ID = 10

def create_random_string():
    random_string = str(uuid.uuid4())
    return random_string

def create_random_file_name():
    name = create_random_string()[:10]
    return name

def run_command(server, command):
    server.exec_run(f'bash -c "echo {command} > /pipe"')

def putfile(server):
    random_string = create_random_string()
    random_file_name = create_random_file_name()
    with open("test_file.txt", "w+") as f:
        f.write(random_string)
    run_command(server, f"put test_file.txt {random_file_name}")
    return random_string, random_file_name

def lsfile(server, file_name):
    run_command(server, f"ls {file_name}")

class TestServer(unittest.TestCase):
    def setUp(self) -> None:
        self.timeout = 5
        self.test_server = docker.from_env().containers.get(f"server{TEST_SERVER_ID}")

    def tearDown(self) -> None:
        for i in range(1, 11):
            subprocess.run([f"rm -f ../sdfs/m{i}/*"], shell=True)

    def test_put_file(self):
        random_string, random_file_name = putfile(self.test_server)

        # read log until success
        for i in range(self.timeout):
            docker_client = docker.from_env().containers.get(f"server01")
            logs = docker_client.logs(since=datetime.datetime.utcnow() - datetime.timedelta(seconds=30)).decode("utf-8")
            result = re.search(f"Put file test_file.txt to .* successfully", logs)
            if result is not None:
                break
            else:
                time.sleep(1)
        else:
            assert False

        # run ls to see where is the file
        lsfile(self.test_server, random_file_name)
        docker_client = docker.from_env().containers.get(f"server{TEST_SERVER_ID}")
        for i in range(self.timeout):
            logs = docker_client.logs(since=datetime.datetime.utcnow() - datetime.timedelta(minutes=1)).decode("utf-8")
            result = re.search(f"ls {random_file_name} =", logs)
            len = re.findall(f"len = \d+", logs)
            if result is not None:
                break
            else:
                time.sleep(1)
        else:
            assert False

        assert len[-1] == "len = 4"
        assert result is not None
        '''
        example string
        INFO:root:Put file test_file/b.txt to {('server05.mp1_default', 23456, 1701143564.6585293), ('server08.mp1_default', 23456, 1701143564.7735474), ('server06.mp1_default', 23456, 1701143565.044318), ('server07.mp1_default', 23456, 1701143565.235968)} from ('server02.mp1_default', 23456, 1701143564.3657732) successfully
        '''
        # fetch the server id following by word "server"
        servers_id = re.findall(r"server\d+", logs[result.end():])[:4]
        for server_id in servers_id:
            # get the container
            id = int(server_id[6:])
            server = docker.from_env().containers.get(f"server{id:02d}")

            # enter that container and check if the file exists
            output = server.exec_run(f'ls /app/sdfs/{random_file_name}').output
            assert re.search("No such file or directory", output.decode("utf-8")) is None

    def test_get_file(self):
        server = docker.from_env().containers.get(f"server{TEST_SERVER_ID}")
        server.exec_run(f'rm /result_get.txt')

        random_string, random_file_name = putfile(self.test_server)
        run_command(self.test_server, f"get {random_file_name} /result_get.txt")

        for i in range(self.timeout):
            output = server.exec_run(f'cat /result_get.txt').output
            if 'No such file or directory' not in output.decode("utf-8"):
                break
            else:
                time.sleep(1)
        else:
            assert False
        
        assert output == random_string.encode("utf-8")

    def test_ls(self):
        random_string, random_file_name = putfile(self.test_server)
        lsfile(self.test_server, random_file_name)

        docker_client = docker.from_env().containers.get(f"server{TEST_SERVER_ID}")
        for i in range(self.timeout):
            logs = docker_client.logs(since=datetime.datetime.utcnow() - datetime.timedelta(seconds=10)).decode("utf-8")
            result = re.search(f"ls {random_file_name} =", logs)
            if result is not None:
                break
            else:
                time.sleep(1)
        else:
            assert False


    def test_delete(self):
        random_string, random_file_name = putfile(self.test_server)
        run_command(self.test_server, f"delete {random_file_name}")
        lsfile(self.test_server, random_file_name)

        docker_client = docker.from_env().containers.get(f"server{TEST_SERVER_ID}")
        for i in range(self.timeout):
            logs = docker_client.logs(since=datetime.datetime.utcnow() - datetime.timedelta(seconds=30)).decode("utf-8")
            result = re.search(f"ls {random_file_name} =.*, len =", logs)
            if result is not None:
                break
            else:
                time.sleep(1)
        else:
            assert False
        assert logs[result.end()+1:result.end()+2] == "0"

    def test_multiread(self):
        random_string, random_file_name = putfile(self.test_server)
        servers = random.sample(range(2, 11), 5) # test server is 1?

        run_command(self.test_server, f"multiread {random_file_name} /result.txt {' '.join([str(server) for server in servers])}")

        docker_client = docker.from_env().containers.get(f"server01")
        for i in range(self.timeout):
            logs = docker_client.logs(since=datetime.datetime.utcnow() - datetime.timedelta(minutes=1)).decode("utf-8")
            result = re.search(f"Finish multiread file {random_file_name}", logs)
            if result is not None:
                break
            else:
                time.sleep(1)
        else:
            assert False

        for server in servers:
            server = docker.from_env().containers.get(f"server{server:02d}")
            output = server.exec_run(f'cat /result.txt').output
            assert output == random_string.encode("utf-8")
            server.exec_run(f'rm /result.txt')

    # def test_fail(self):
    #     # file transfer
    #     random_string, random_file_name = putfile(self.test_server)

    #     # read log until success
    #     for i in range(self.timeout):
    #         docker_client = docker.from_env().containers.get(f"server{TEST_SERVER_ID}")
    #         logs = docker_client.logs(since=datetime.datetime.utcnow() - datetime.timedelta(seconds=30)).decode("utf-8")
    #         result = re.search(f"Put file {random_file_name} successfully", logs)
    #         if result is not None:
    #             break
    #         else:
    #             time.sleep(1)
    #     else:
    #         assert False

    #     servers_id = re.findall(r"server\d+", logs[result.end():])[:4]
    #     if "server01" in servers_id:
    #         servers_id.remove("server01")
    #     if f"server{TEST_SERVER_ID}" in servers_id:
    #         servers_id.remove(f"server{TEST_SERVER_ID}")
    #     server = random.choice(servers_id)
    #     terminaL_docker_client = docker.from_env().containers.get(f"{server}")
    #     terminaL_docker_client.stop()
    #     time.sleep(30)

    #     # run ls to see where is the file
    #     lsfile(self.test_server, random_file_name)
    #     docker_client = docker.from_env().containers.get(f"server{TEST_SERVER_ID}")
    #     logs = docker_client.logs(since=datetime.datetime.utcnow() - datetime.timedelta(minutes=1)).decode("utf-8")
    #     result = re.search(f"ls {random_file_name} =", logs)
    #     len = re.findall(f"len = \d+", logs)
    #     assert len[-1] == "len = 4"
    #     assert result is not None
    #     '''
    #     example string
    #     INFO:root:Put file test_file/b.txt to {('server05.mp1_default', 23456, 1701143564.6585293), ('server08.mp1_default', 23456, 1701143564.7735474), ('server06.mp1_default', 23456, 1701143565.044318), ('server07.mp1_default', 23456, 1701143565.235968)} from ('server02.mp1_default', 23456, 1701143564.3657732) successfully
    #     '''
    #     # fetch the server id following by word "server"
    #     servers_id = re.findall(r"server\d+", logs[result.end():])[:4]
        
    #     for server_id in servers_id:
    #         # get the container
    #         id = int(server_id[6:])
    #         server = docker.from_env().containers.get(f"server{id:02d}")

    #         # enter that container and check if the file exists
    #         output = server.exec_run(f'ls /app/sdfs/{random_file_name}').output
    #         assert re.search("No such file or directory", output.decode("utf-8")) is None

    #     terminaL_docker_client.restart()
    #     time.sleep(5) # wait for connection

    def test_get_large_file(self):
        self.test_server.exec_run(f'rm /result_get.txt')

        random_file_name = create_random_file_name()
        mega_byte = 1_000_000

        instruction = f"python3 -c \"import os; open('test_file.txt', 'wb').write(os.urandom(100 * 1024 * 1024))\""
        self.test_server.exec_run(instruction)
        
        # time.sleep(10)
        run_command(self.test_server, f"put test_file.txt {random_file_name}")
        run_command(self.test_server, f"get {random_file_name} /result_get_large.txt")

        docker_client = docker.from_env().containers.get(f"server01")
        for i in range(60):
            logs = docker_client.logs(since=datetime.datetime.utcnow() - datetime.timedelta(seconds=10)).decode("utf-8")
            result = re.search(f"Get file /result_get_large.txt at {random_file_name} successfully", logs)
            if result is not None:
                break
            else:
                time.sleep(1)
        else:
            assert False
        
        random_string = self.test_server.exec_run(f'cat test_file.txt').output
        output = self.test_server.exec_run(f'cat /result_get_large.txt').output

        assert output == random_string

    def test_leader(self):
        pass
        
if __name__ == '__main__':
    unittest.main()