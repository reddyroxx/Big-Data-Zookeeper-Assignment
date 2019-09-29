# Import socket module
import socket
import json
import sys
import thread
# Create a socket object
s = socket.socket()
#ps=[4191,4192,4193]
ps=[]
total=3 

from kazoo import client as kz_client
count=1
my_client = kz_client.KazooClient(hosts='127.0.0.1:2181')
def my_listener(state):
	if state == kz_client.KazooState.CONNECTED:
		print("Client connected !")
 
my_client.add_listener(my_listener)
my_client.start(timeout=5)
a=my_client.create("/parent/"+sys.argv[1],ephemeral=True)
print(a)
b=my_client.get_children("/parent")
print(b)
for a in b:
	ps.append(int(a))
ps.remove(int(b[0]))
print(ps)


def my_func(event):
	print("hellllllllllllllo")
	b=my_client.get_children("/parent")
	print(b)
	for a in b:
		ps.append(int(a))
	ps.remove(int(b[0]))

def watcher():
	while(True):
		children = my_client.get_children("/parent", watch=my_func)

port = int(sys.argv[1])
s.connect(('127.0.0.1', port))
data={}
rdata={}
run=True
while (run):
	thread.start_new_thread( watcher, () )
	datacli=s.recv(1024)
	print(datacli)
	print(ps)
	ps=list(set(ps))
	ps.sort()
	print(ps)
	con= json.loads(datacli)
	print(type(con))
	if(isinstance(con, dict)):
		for i in con:
			qkey=i
			k=ord(qkey[0])%total
			k=k+ps[0]
			if(k==port):
				print("original")
				data[qkey]=con[qkey]
				s.send("information updated")
			else:
				print("replication")
				rdata[qkey]=con[qkey]
				s.send(json.dumps(rdata))
	else:
		qkey=str(con)
		k=ord(qkey[0])%total
		k=k+ps[0]
		if(k==port):
			try:
				qret=data[qkey]
			except:
				qret="key not present"
			print(qret)
		else:
			try:
				qret=rdata[qkey]
			except:
				qret="key not present"
		s.send(qret)
	print(data)
	print(rdata)
s.close()
	