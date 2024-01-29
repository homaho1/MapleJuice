# Simple Distributed File System w. MapReduce AKA MapleJuice

MapleJuice is a simple distributed file system that utilizes membership protocols for its implementation. Additionally, it supports running MapReduce programs on the file system.

## Usage

### Docker

1. Set `DOCKER` in config.py to `true`
2. Run `docker built -t <image_name> .`
3. Run `docker compose up`
4. Type command by running `sh run_command.sh <server_id> "<operation> <args>"`

### Virtual Machine

1. Set `DOCKER` in config.py to `false`.
2. Run `python3 src/server.py` at 01 machine, then start other servers at other machines by running the same command.
3. Type commands to control the server on command line

### Config

You can edit the `src/config.py` to change the configuration of the system. 

## Debug mode

- Docker
  * Uncomment ` CMD ["python", "-u", "server.py", "-v"]` on docker file
  
- Virtual Machine
  * Run `python3 src/server.py -v`

## Commands

* Server operations:
    * `list_mem`: list the membership list
    * `list_self`: list self’s id
    * `print_leader`: print the current leader
    * `leave`: voluntarily leave the group (different from a failure)
    * `enable_suspicion`: enable suspicion mechanism
    * `disable_suspicion`: disable suspicion mechanism
    * `display_suspicion`: display suspicion status
    * `grep`: grep the log files
  
* File operations:
  * `put <local_filename> <sdfs_filename>`: put a file from local machine to SDFS
  * `get <sdfs_filename> <local_filename>`: get a file from SDFS to local machine
  * `delete <sdfs_filename>`: delete a file from SDFS
  * `ls <sdfs_filename>`: list all machine (VM) addresses where this file is currently being stored
  * `store`: list all files currently being stored at this machine
  * `multiread`: read files from SDFS from multiple servers at the same time

* MapleJuice:
  * `maple <maple_exe> <num_maples> <sdfs_intermediate_filename_prefix> <sdfs_src_directory>`: run map on job SDFS.
  * `juice <juice_exe> <num_juices> 
<sdfs_intermediate_filename_prefix> <sdfs_dest_filename> 
delete_input={0,1}`: run reduce job on SDFS.
  * `filter SELECT ALL FROM Dataset WHERE <regex condition>`: SQL-style query which select from dataset and filter based on regular expression
  * `join SELECT ALL FROM D1 D2 WHERE "D1.field=D2.field <output>"`: SQL-style query which join two tables based on the argument

## Developer

* [`chinhao2`](https://github.com/hankluo6)
* [`hcma2`](https://github.com/homaho1)
