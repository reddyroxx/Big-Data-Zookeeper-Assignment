# Import socket module
import socket
import json
import select

s = socket.socket()

from kazoo import client as kz_client
count=1
my_client = kz_client.KazooClient(hosts='127.0.0.1:2181')
def my_listener(state):
    if state == kz_client.KazooState.CONNECTED:
        print("Client connected !")
 
my_client.add_listener(my_listener)
my_client.start(timeout=5)
b=my_client.get_children("/parent")
print(b)
b.sort()
port=int(b[0])
s.connect(('127.0.0.1', port))

run=True
while (run):
	aflag=False
	fflag=False
	todo = raw_input("p->put\ng->get\nq->quit\n")
	if(todo[0]=='p'):
		aflag=True
	elif(todo[0]=='g'):
		fflag=True
	elif(todo[0]=='q') :
		break
	else:
		print('invalid entry')
	i=True
	if(aflag):
		while (i):
			key = raw_input("What is your key?")
			check=ord(key[0])
			if(check>=65 and check<=90):
				i=False
			elif(check>=97 and check<=122):
				i=False
			else:
				print("please use keys starting with alphabets")
			
			value = raw_input("What is your value?")
			payload = {key: value}
			y = json.dumps(payload)
			s.send(y)
			print (s.recv(1024))
	if(fflag):
		while (i):
			keyret =raw_input("What is your query key?")
			check=ord(keyret[0])
			if(check>=65 and check<=90):
				i=False
			elif(check>=97 and check<=122):
				i=False
			else:
				print("keys start with alphabets")
			
			payload = keyret
			y = json.dumps(payload)
			s.send(y)
			print (s.recv(1024))
s.close()
