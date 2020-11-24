import os
from PIL import Image
import socket
import pickle
import threading
from senders import *
import numpy as np

Master_addr=("192.168.43.121",9998)

TAKE_REST=0
IMAGE=1

FILE_INFO=0
FILE_DATA=1


class _slave(object) :
    def __init__(self,addr) :
        self.address=addr[0]
        self.port=None
        self.uploading=False

class master(object) :
    def __init__(self) :
        self.SLAVES_LIST=[]
        self.WORKING=True
        self.sender=["0.0.0.0",0]
        self.listener=None
        self.imgs_num=0
        self.IMAGES_BUFFER=self.load_image_names()
        self.slaves_num=0
        
    def send_image(self, data, s) :
        s.send(data)
        print("Master>>Image sent !")
        
    def send_string(self, string, s) :
        s.send(string.encode())
            
    def load_image_names(self):
        f=[]
        for root, _ ,f in os.walk("c:\\Users\\pc\\Desktop\\GitHub\\MP Parallel\\dataset"):
            break

        files=f
        self.imgs_num=len(f)
        print("Loaded images : ",files)
        return files
     
    def transfer_image(self, data, s:socket.socket) :
        self.send_image(data,s)

    def inform_take_rest(self, s:socket.socket) :
        st="?|???????????????????????????????????????????????????????????????????????"
        
        self.send_string(st,s)
    
    def inform_going_to_send_image(self, s:socket.socket, fname, fsize):
        st=fname
        i=8-len(str(fsize))
        cw0=""
        while i>0 :
            cw0+="0"
            i-=1
            
        i=64-len(fname)
        while i>0 :
            st+="?"
            i-=1
        st+=("|"+cw0+str(fsize))
        self.send_string(st,s)       
        
    def pick_image(self):
        self.imgs_num-=1
        return self.IMAGES_BUFFER[self.imgs_num]
        
        
    def send(self,what,s:socket.socket,quota=0) :
        if what == IMAGE :
            fname=self.pick_image()
            fpath=os.path.join("c:\\Users\\pc\\Desktop\\GitHub\\MP Parallel\\dataset",fname)
            with Image.open(fpath) as raw :
                data=pickle.dumps(np.array(raw))
                self.inform_going_to_send_image(s, fname, len(data))
                print("Master>>Sending file ", fname,"to", s.getpeername())
                self.transfer_image(data,s)
            
        else :
            self.inform_take_rest(s)
            
    def remove_slave(self) :
        self.slaves_num-=1
            
    def start_server(self) :
        self.listener=socket.socket()
        self.listener.bind(Master_addr)
        self.listener.listen(100)
        print("Master>>Started on ",Master_addr)
        
    def affect_job(self,s:socket.socket, index) :
        if self.imgs_num<=0:
            self.SLAVES_LIST[index].uploading=True
            print("Master>>Sending take rest ")
            self.send(TAKE_REST, s)
            
        else:
            print("Master>>Sending image ")
            self.send(IMAGE,s,1)
                
    def  start_listener(self, conn:socket.socket, index):
        s=socket.socket()
        print("Master>>On thread",index,"peered with ", conn.getpeername())
        while True :
            if self.SLAVES_LIST[index].uploading :
                self.remove_slave()
                if self.slaves_num== 0 :
                    self.WORKING=False
                    print("Master>>Ready to receive upload")
                    self.receive_upload(conn,s)
                    self.close_listener()
                else :
                    print("Master>>Ready to receive upload")
                    self.receive_upload(conn,s)
                break
            else:
                msg=conn.recv(23).decode().split("|")
                print("Master>>Received message : ",msg)
                if msg[0]=="hi" :
                    self.SLAVES_LIST[index].address=msg[1]
                    self.SLAVES_LIST[index].port=int(msg[2].replace("?",""))
                    print(self.SLAVES_LIST[index].address,",",self.SLAVES_LIST[index].port,"is saying hi !")
                    s.connect((self.SLAVES_LIST[index].address, self.SLAVES_LIST[index].port))
                    print(self.SLAVES_LIST[index].address, self.SLAVES_LIST[index].port , " says hi !")
                
                print("Master>>Affecting job to", self.SLAVES_LIST[index].address, self.SLAVES_LIST[index].port)
                self.affect_job(s, index)
                print("Master>>Job affected to slave", self.SLAVES_LIST[index].address, self.SLAVES_LIST[index].port)
    
    def close_listener(self) :
        s=socket.socket()
        s.connect(Master_addr)
        s.close()
        
    def save(self, fname, data) :
        with open("c:\\Users\\pc\\Desktop\\GitHub\\MP Parallel\\out\\"+fname, "wb") as fout :
            fout.write(data)
        
    def receive_upload(self,conn:socket.socket, s:socket.socket) :
        stat=FILE_INFO
        more=True
        while more :
            if stat==FILE_INFO :
                msg=conn.recv(75)
                print("Master>>Received bytes : ",msg)
                msg=msg.decode().split("|")
                if msg[0] == 'e' :
                    more=False
                else :
                    fname=msg[1].replace("?","")
                    fsize=int(msg[2])
                    stat=FILE_DATA
            else :
                print("Master>>Receiving file ",fname,"...")
                data=conn.recv(fsize)
                print("Master>> File ",fname,"received")
                self.save(fname, data)
                s.send("ACK".encode())
                stat=FILE_INFO

        print("Master>>Receiving upload done !")
        
    def run(self) :
        self.start_server()
        print("Waiting connections ...")
        conn, address = self.listener.accept()
        print(address , "connected")
        while self.WORKING :
            index=len(self.SLAVES_LIST)
            new_slave= _slave(address)
            self.SLAVES_LIST.append(new_slave)
            self.slaves_num+=1
            t=threading.Thread(target=self.start_listener, args=(conn, index))
            t.start()
            print("Waiting connections ...")
            conn, address = self.listener.accept()
            print(address , "connected")
            