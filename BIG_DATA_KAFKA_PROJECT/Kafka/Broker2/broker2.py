import socket
from _thread import *
import json
import os

PORT = 4002
try:
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    hostname = socket.gethostname()
    ip_addr = socket.gethostbyname(hostname)
    server.bind((ip_addr, PORT))
    server.listen(5)
    print("Socket is listening!!")

except socket.error as err:
    print(err)

list_of_msg = {}    

path = os.path.join(os.getcwd(),'Topics')
partition_no = 3
partition = 1
def productThread(client, address):
    global list_of_msg,partition_no,partition
    
    topic_message = json.loads(client.recv(1024).decode('utf-8'))
    if(not os.path.exists(os.path.join(path,topic_message['topic']))):
        os.mkdir(os.path.join(path,topic_message['topic']))
        
    file = open(os.path.join(path,topic_message['topic'],f'partition{partition}.txt'),'a')
    file.write(topic_message['message']+'\n')

    replication_port = 4001
    try:
        replicate = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        replicate.connect((ip_addr,replication_port))
        replicate.send("broker00".encode('utf-8'))
        replicate.send((topic_message['topic'] + ',' + f'{partition}').encode('utf-8'))
        ack = replicate.recv(1024).decode('utf-8')
        replicate.send(topic_message['message'].encode('utf-8'))
        replicate.close()

    except socket.error as err:
        print(f"Broker with port {replication_port} Failed")
    
    try:
        replication_port = 4003
        replicate.connect((ip_addr,replication_port))
        replicate.send("broker00".encode('utf-8'))
        replicate.send((topic_message['topic'] + ',' + f'{partition}').encode('utf-8'))
        ack = replicate.recv(1024).decode('utf-8')
        replicate.send(topic_message['message'].encode('utf-8'))
        replicate.close()

    except socket.error as err:
        print(f"Broker with port {replication_port} Failed")

    if topic_message['topic'] in list_of_msg:
        list_of_msg[topic_message['topic']].append(topic_message['message'])
    else:
        list_of_msg[topic_message['topic']] = [topic_message['message']]
    print(list_of_msg)
    file.close()
    partition += 1
    if(partition>partition_no):
        partition = 1
    client.close()
    
def consumerThread(client, address):
    global list_of_msg,partition_no
    partition1 = 1
    topic_offset = json.loads(client.recv(1024).decode('utf-8'))
    topic = topic_offset['topic']
    offset = topic_offset['offset']
    if(os.path.exists(os.path.join(path,topic))):
        print('Continue')
    else:
        os.mkdir(os.path.join(path,topic))
    
    list_of_msg_temp = []
    while partition1 <= partition_no:
        if(os.path.isfile(os.path.join(path,topic,f'partition{partition1}.txt')) == False):
            file = open(os.path.join(path,topic,f'partition{partition1}.txt'),'w')
            file.close()
        file = open(os.path.join(path,topic,f'partition{partition1}.txt'),'r')
        msg_line =  file.readline()
        while msg_line != '':
            list_of_msg_temp.append(msg_line)
            
            msg_line =  file.readline()
        file.close()
        partition1 += 1
    list_of_msg[topic] = list_of_msg_temp
    index = len(list_of_msg[topic])
    # print(list_of_msg[topic])
    if(offset == '1'):
        index = 0
    while True:
        
        # print(list_of_msg[topic])
        if len(list_of_msg[topic]) == index:
            continue
        data = list_of_msg[topic][index]
        # print(data)
        index += 1
        client.send(data.encode('utf-8'))

def replicateTopic(client,address):
    global path
    topic_partition = client.recv(1024).decode('utf-8')
    client.send("OK".encode('utf-8'))
    topic,partition1 = topic_partition.split(',')
    msg = client.recv(1024).decode('utf-8')
    # print(topic_partition)
    if(not os.path.exists(os.path.join(path,topic))):
        os.mkdir(os.path.join(path,topic))
    file = open(os.path.join(path,topic,f'partition{partition1}.txt'),'a')
    file.write(msg+'\n')
    client.close()

if __name__ == '__main__':
    while True:
        client, address = server.accept()
        type_of_client = client.recv(9).decode('utf-8')
        if type_of_client == "zookeeper":
            client.close()
        elif type_of_client == "producers":
            start_new_thread(productThread, (client, address))
        elif type_of_client == "consumers":
            start_new_thread(consumerThread, (client, address))
        elif type_of_client == "broker00":
            start_new_thread(replicateTopic, (client,address))
