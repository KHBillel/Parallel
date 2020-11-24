from slave_tools import *
import multiprocessing as mp
import random
import os

def slave_runner(index):
    address=socket.gethostbyname(socket.gethostname())
    
    print(address,"on",os.getpid())
    s=slave(address,index)
    s.run()

if __name__ == "__main__" :
    NCPU=mp.cpu_count()-1
    #pool=mp.Pool(processes=NCPU)
    #_=pool.map_async(slave_runner,[i for i in range(1,NCPU+1)])
    #pool.close()
    #pool.join()
    slave_runner(random.randrange(15,1500))