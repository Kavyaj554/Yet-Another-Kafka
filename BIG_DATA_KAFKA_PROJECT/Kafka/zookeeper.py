#implementation mini-kafka zookeeper
from _thread import *
import threading
import socket
import time
import signal
import sys

PORT = 2181
MASTER_PORT = 4001

def connectBroker(brokerPort):
    global MASTER_PORT
    while True:
        try:
            client = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            client.connect((ip_addr,brokerPort))
            client.send("zookeeper".encode())
            print("broker alive on port: ",brokerPort,MASTER_PORT)
            client.close()
        except socket.error as err:
            if MASTER_PORT == brokerPort:
                if brokerPort == 4001:
                    MASTER_PORT = 4002
                elif brokerPort == 4002:
                    MASTER_PORT = 4003
                elif brokerPort == 4003:
                    MASTER_PORT = 4001
            print("Broker dead on port: ",brokerPort,MASTER_PORT)
        time.sleep(5)


if __name__ == '__main__':
    try:
        server = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        hostname = socket.gethostname()
        ip_addr = socket.gethostbyname(hostname)
        server.bind((ip_addr,PORT))
        server.listen(5)
        print("Socket is listening!!")

    except socket.error as err:
        print(err)
    start_new_thread(connectBroker,(4001,))
    start_new_thread(connectBroker,(4002,))
    start_new_thread(connectBroker,(4003,))
    while True:
        client,addr = server.accept()
        type_of_client = client.recv(1024).decode('utf-8')
        if(type_of_client == "producer"):
            client.send(str(MASTER_PORT).encode('utf-8'))
        elif(type_of_client == "consumer"):
            client.send(str(MASTER_PORT).encode('utf-8'))
            # pass
        