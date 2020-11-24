import os
import socket
import pickle
import threading
from PIL import Image
from io import BytesIO

MASTER_ADDR='127.0.0.1'
MASTER_PORT=9999

WAITING_INFO =0
WAITING_DATA =1

HI=0
DONE=1

class slave(object): 
    def __init__(self, addr, p):
        self.address=addr
        self.port=p
        self.active=True
        self.sending_socket=socket.socket()
        #self.sending_socket.bind((addr,4000))
        self.sending_socket.connect((MASTER_ADDR,MASTER_PORT))
        print("Slave", addr, p,">>Created")
        
    def say_hi(self):
        print("Slave ", self.address, self.port, ">>saying hi!")
        self.send(HI)
        
    def start_listener(self):
        s=socket.socket()
        s.bind((self.address, self.port))
        s.listen(1)
        print("Slave",self.address, self.port,">> Listener started")
        conn, address = s.accept()
        print("Slave ", self.address, self.port,">>Master connected !")
        t=[]
        while self.active :
            print("Slave ", self.address, self.port,">>receiving info")
            msg=conn.recv(73).decode().split("|")
            print("Slave received : ",msg)
            if msg[0]=="?" :
                print("Slave ", self.address, self.port,">>Rest received, ready to start uploading ...")
                self.active=False
                [a.join() for a in t]
                self.upload(conn)
            else:
                fname=msg[0].replace("?","")
                fsize=int(msg[1])
                print("Slave ", self.address, self.port,">>receiving data")
                data=conn.recv(fsize)
                a=threading.Thread(target=self.process,args=(fname,data))
                t.append(a)
                a.start()
                a.join()
                self.send(DONE)
                
        
        
    def send(self,what) :
        if what ==HI :
            st="hi|"+self.address+'|'+str(self.port)
            i= 23 - len(st)
            while i>0 :
                st+="?"
                i-=1
            
        else :
            print("Slave ", self.address, self.port,">>sending done")
            st="done|??????????????????"
            
        self.sending_socket.send(st.encode())
        
    def upload(self,conn:socket.socket) :
        print("Slave ", self.address, self.port,">>uploading files")
        for root, _, files in os.walk("./tmp") :
            for fname in files :              
                data=self.buffer.getvalue()
                fsize=str(len(data))
                    
                st="f|"+fname
                i=64-len(fname)
                while i> 0 :
                    st+="?"
                    i-=1
                    
                st+="|"
                i=8-len(fsize)
                while i>0 :
                    st+="0"
                    i-=1
                    
                st+=fsize
                print("Slave ", self.address, self.port,">>Uploading file ",fname)
                print("Slave ", self.address, self.port,">>Sending msg = ",st)
                self.sending_socket.send(st.encode())
                print("Slave ", self.address, self.port,">>msg = ",st," sent")                     
                self.sending_socket.send(data)
                ack=None
                while ack != "ACK" :
                    ack=conn.recv(3).decode()

                print("Slave ", self.address, self.port,">>msg = ",st," received") 
        
        st="e|+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
        self.sending_socket.send(st.encode())       
        print("Slave ", self.address, self.port,">>upload done")
    
    def process(self,fname,data_):
        self.buffer = BytesIO()
        data=pickle.loads(data_)
        print("Slave ", self.address, self.port,">>processing ",fname)
        img=Image.fromarray(data)
        img.save(self.buffer, "JPEG", quality=50,optimize=True)
        print("Slave ", self.address, self.port,">>processing ", fname, "done")
    
    def run(self) :
        t=threading.Thread(target=self.say_hi)
        t.start()
        self.start_listener()
        t.join()        