import sys
import socket
import time
import random

#listen one recv, then send random purchase  
name = ['zhanghui', 'zhangruixiang', 'jiangsaiya', 'xuqiuyi', 'liudehua', 'chenxiaochun', 'lilianjie', 'mayun',
       'mahuateng', 'dechuanjiakang', 'zhitianxinchang', 'fenchenxiuji'
       ]
product = [('iphone cover', 29), ('ipad cover', 99), ('Mi band', 199), ('kindle', 999)]
name_len = len(name)
product_len = len(product)

socket_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
socket_server.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
socket_server.bind((sys.argv[1], int(sys.argv[2])))
socket_server.listen(5)

while True:
    conn, (ip, port) = socket_server.accept()
    print(ip, port)
    while True:
        random_sleep_time = random.random()
        random_product_index = random.randint(0, product_len-1)
        random_user_index = random.randint(0, name_len-1)
        time.sleep(random_sleep_time)
        s = name[random_user_index] +','+product[random_product_index][0]+','+str(product[random_product_index][1])
        print(s)
        conn.send(bytes(s, 'utf-8'))
        conn.send(bytes('\n', 'utf-8'))
    socket_server.close()
