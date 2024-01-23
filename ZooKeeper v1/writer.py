from kazoo.client import KazooClient, KazooState
from kazoo.retry import KazooRetry
import sys
import logging
import time
import random

logging.basicConfig()

client_timeout = 10
server_start_timeout = 10

print(f"starting client writer [ {sys.argv[1]} ] ..")
zk = KazooClient(hosts='127.0.0.1:2181', timeout=server_start_timeout)
zk.start()
print(f"client writer started")


def push_data():
    
    kr = KazooRetry(max_tries=3, ignore_expire=False)
    zk.ensure_path("/myapp/rachid/init_config")
    info="some important configuration with " + str(random.randint(0,100))
    result = kr(zk.set, "/myapp/rachid/init_config" , info.encode())
    print(">> data pushed")

    # read
    data, stat = kr(zk.get, "/myapp/rachid/init_config" )
    print(f"The data pushed is '{data.decode()}' , with version: {stat.version}, updated at {stat.mtime}")

    

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

while is_server_up[0] <= 200: 
    
    if is_server_up[0] == 200:
        print(f"{sys.argv[1]} is pushing data..")
        push_data()
        # do some work below
        time.sleep(7)
    elif is_server_up[0] == 100:
        print(f"Server connection lost, trying to reconnect..")
        time.sleep(client_timeout)
        if is_server_up[0] == 100:
            print(f"Client timeout..")
            break

print(f"Stopping client..")
zk.stop()
zk.close()


