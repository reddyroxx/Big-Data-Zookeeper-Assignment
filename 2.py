import socket
import json
import select
import sys
print "Socket successfully created"

port1= int(sys.argv[1])
ps=[4231,4232,4233]

total=3
ss={}
cs={}

r1={}
r2={}
r3={}

payload = {"hi": "bye"}
y=json.dumps(payload)

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
	print(a)

def my_func(event):
	print("hellllllllllllllo")
	b=my_client.get_children("/parent")
	print(b)
	for a in ps:
		if str(a) not in b:
			print(a)
			cs[a],addr1=ss[a].accept()
			print('***************')
			print(a-ps[0])
			print('***************')
			if(a-ps[0]==0):
				print('i am sending r1')
				cs[a].send(json.dumps(r1))
				d=cs[a].recv(1024)
				print('i am sending r2')
				cs[a].send(json.dumps(r2))
				d=cs[a].recv(1024)
			elif(a-ps[0]==1):
				print('i am sending r2')
				cs[a].send(json.dumps(r2))
				d=cs[a].recv(1024)
				print('i am sending r3')
				cs[a].send(json.dumps(r3))
				d=cs[a].recv(1024)
			else:
				print('i am sending r3')
				cs[a].send(json.dumps(r3))
				d=cs[a].recv(1024)
				print('i am sending r1')
				cs[a].send(json.dumps(r1))
				d=cs[a].recv(1024)
			print(d)


s1= socket.socket()
for l in ps:
	ss[l]= socket.socket()

s1.bind(('', port1))
for l in ps:
	ss[l].bind(('', l))
print "socket binded to %s" %(port1)

s1.listen(5)
for l in ps:
	ss[l].listen(5)

print "socket is listening"
c1, addr1 = s1.accept()
for l in ps:
	cs[l],addr1=ss[l].accept()
print("----------------------")
for l in ps:
	cs[l].settimeout(2)

while True:
	children = my_client.get_children("/parent", watch=my_func)
	a=c1.recv(1024)
	print(a)
	b=json.loads(a)
	if(isinstance(b, dict)):
		for i in b:
			qkey=i
	else:
		qkey=str(b)

	key=ord(qkey[0])%total
	k=key+ps[0]
	if k+1 in ps:
		rk=k+1
	else:
		rk=ps[0]
	if(isinstance(b, dict)):
		try:
			cs[k].send(a)
			c=cs[k].recv(1024)
			print(c)
			cs[rk].send(a)
			d=cs[rk].recv(1024)
			print(d)
		except:
			try:
				cs[rk].send(a)
				d=cs[rk].recv(1024)
				print(d)
			except:
				cs[k].send(a)
				c=cs[k].recv(1024)
				print(c)
		c="updated"
		try:
			r= json.loads(d)
			if(key==0):
				r1.update(r)
			elif(key==1):
				r2.update(r)
			else:
				r3.update(r)
		except:
			pass
	else:
		try:
			cs[k].send(a)
			c=cs[k].recv(1024)
			print(c)
		except:
			cs[rk].send(a)
			c=cs[rk].recv(1024)
			print(c)
	c1.send(c)
	print('========================================================')
	print(r1)
	print(r2)
	print(r3)
	print('========================================================')

c1.close()
c2.close()
s1.close()
s2.close()
