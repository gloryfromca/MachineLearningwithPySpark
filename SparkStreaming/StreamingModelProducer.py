import sys
import socket
import time
import random
import numpy as np
from functools import reduce

def return_random_guass_seq(_type=list, len_term=10, mu=0, sigma=1, _scale=1):
    return _type([_scale*random.gauss(mu, sigma) for i in range(len_term)])

# set w vector
len_term = 100
w = return_random_guass_seq(_type=np.array, _scale=10, len_term=len_term)

socket_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
socket_server.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
socket_server.bind((sys.argv[1], int(sys.argv[2])))
socket_server.listen(5)

while True:
    conn, (ip, port) = socket_server.accept()
    print(ip, port)
    while True:
        random_sleep_time = random.random()
        noisy = random.gauss(0, 1)*5
        x = return_random_guass_seq(_type=np.array, len_term=len_term)
        y = x.dot(w) + noisy
        time.sleep(random_sleep_time)
        s = str(y) + reduce(lambda i, j:str(i)+','+str(j), x, '') 
        print(s)
        conn.send(bytes(s, 'utf-8'))
        conn.send(bytes('\n', 'utf-8'))# \n is separator of Streaming
    socket_server.close()
