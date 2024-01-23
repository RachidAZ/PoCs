from kazoo.client import KazooClient, KazooState
from kazoo.retry import KazooRetry
import sys
import logging
import time

logging.basicConfig()

client_timeout = 10
server_start_timeout = 10
node_path= "/myapp/rachid/init_config" 

print(f"starting client [ {sys.argv[1]} ] ..")
port=sys.argv[2]
zk = KazooClient(hosts=f"127.0.0.1:{port}", timeout=server_start_timeout, read_only=True)
zk.start()
print(f"client started")


shared_data={"config1": ""}
def do_work():
    data=shared_data["config1"]
    print(f"{sys.argv[1]} is doing something with the data fetched : '{data}' ..")

def get_zookeeper_data():
    # init read
    kr = KazooRetry(max_tries=3, ignore_expire=False)
    data, stat = kr(zk.get, node_path)
    print(f"[init_read] the data fetched from init read is '{data.decode()}' , version {stat.version} was updated at {stat.mtime}")
    shared_data["config1"]=data.decode("utf-8")


# watch the node for any updates
@zk.DataWatch(node_path)
def watch_node(data, stat):
    print("++++ [watch] Version: %s, data: %s" % (stat.version, data.decode("utf-8")))
    shared_data["config1"]= data.decode("utf-8")


is_server_up=[200]

@zk.add_listener
def my_listener(state):
    if state == KazooState.LOST:
        # Register somewhere that the session was lost
        print(f">> the session was lost")
        is_server_up[0]=500
    elif state == KazooState.SUSPENDED:
        # Handle being disconnected from Zookeeper
        print(f">> the session was disconnected")
        is_server_up[0]=100
        #print(f"+++ is_server_up is my_listener is {is_server_up}")
    else:
        # Handle being connected/reconnected to Zookeeper
        print(f">> the session was connected/reconnected")
        is_server_up[0]=200

do_init_read=True
while is_server_up[0] <= 200: 
    
    if is_server_up[0] == 200:
        get_zookeeper_data() if do_init_read else None # should be only onetime , we will get notified later for any updates by watch_node
        do_work()
        # do some work below
        time.sleep(3)
    elif is_server_up[0] == 100:
        print(f"Server connection lost, trying to reconnect..")
        time.sleep(client_timeout)
        if is_server_up[0] == 100:
            print(f"Client timeout..")
            break

    do_init_read=False




print(f"Stopping client..")
zk.stop()
zk.close()