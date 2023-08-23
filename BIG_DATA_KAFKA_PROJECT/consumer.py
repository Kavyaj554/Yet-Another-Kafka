import sys
import socket
import argparse
import time

parser = argparse.ArgumentParser()
parser.add_argument('--fromBeginning', required=True)

args = parser.parse_args()
offset = args.fromBeginning

def req_zookeeper(consumer,ip_addr):
	PORT = 2181
	consumer.connect((ip_addr, PORT))
	consumer.send("consumer".encode('utf-8'))
	MASTER_PORT = consumer.recv(1024).decode('utf-8') 
	print(MASTER_PORT)
	consumer.close()
	return MASTER_PORT

def request_broker(consumer,ip_addr,MASTER_PORT):
	PORT = int(MASTER_PORT)
	consumer.connect((ip_addr,PORT))
	consumer.send("consumers".encode('utf-8'))
	consumer.send(('{"topic":"test1","offset":"' + offset + '"}').encode('utf-8'))
	while True:
		result = consumer.recv(1024).decode('utf-8')
		if result:
			print(result.strip())
		else:
			break

if __name__ == '__main__':
	while True:
		try:
			consumer = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
			print("Socket successfully created!!")
			hostname = socket.gethostname()
			ip_addr = socket.gethostbyname(hostname)
			MASTER_PORT = req_zookeeper(consumer,ip_addr)
		except socket.error as err:
			continue
			
		try:
			consumer = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
			print("Socket successfully created!!")
			request_broker(consumer,ip_addr,MASTER_PORT)
			consumer.close()
		except socket.error as err:
			continue

		time.sleep(6)