#!/usr/bin/python3

import socket
import pickle
from random import randint
import argparse
import mysql.connector
from pythonping import ping




parser = argparse.ArgumentParser()
parser.add_argument('mode', help='mode of implementation of proxy, direct, random or custom')
args = parser.parse_args()

mode=args.mode 
# the first one is the master's IP

targets={0:{"ip":"184.72.114.22", "port":3306},1:{"ip":"52.206.95.187", "port":3306},2:{"ip":"44.201.140.131", "port":3306}}
target_list=["34.202.223.21","52.206.95.187","44.201.140.131"]

def main():

    listen = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listen.bind(('', 5001))   
    listen.listen(1)
    conn, addr = listen.accept()
    print(f"Connection from {addr} has been established.")
    with conn:
        print('Connected by', addr)

        while True:
            data = conn.recv(2048) 
            if not data:
                break
            
            cmd_type, command = load_data(data)
            
            if cmd_type=="insert" and mode=='direct':
                targ=0
                target_node="master"
                cnx = mysql.connector.connect(user='fatemeh', password='1234Fa$$', host='184.72.114.22', database='tp3')
                print ('Connection to DB')
                cursor = cnx.cursor()
                cursor.execute(command)
                cnx.commit()
                response = {'handled by':target_node, 'response': "OK"}
                response = pickle.dumps(response)
                conn.send(response)
                
                
            if cmd_type=="select" and mode=='direct':
                targ=0
                target_node="master"
                cnx = mysql.connector.connect(user='fatemeh', password='1234Fa$$', host='184.72.114.22', database='tp3')
                print ('Connection to DB')
                cursor = cnx.cursor()
                cursor.execute(command)
                print(f'handled by :{target_node}')
                result = cursor.fetchall()
                response={'handled by':{target_node}, 'result':result}
                response = pickle.dumps(response)
                conn.send(response)
                
            if cmd_type=="select" and mode=='random':
                targ=randint(1, 2)
                target_node="slave"+str(targ)

                cnx = mysql.connector.connect(user='fatemeh', password='1234Fa$$', host='184.72.114.22', database='tp3')
                print ('Connection to DB')
                cursor = cnx.cursor()
                cursor.execute(command)
                print(f'handled by :{target_node}')
                result = cursor.fetchall()
                response={'handled by' :{target_node}, 'result':result}
                response = pickle.dumps(response)
                
                conn.send(response)
              
            if cmd_type=="select" and mode=='custom':
                targ=custom()
                if targ==0:
                    target_node="master"
                else:
                    target_node="slave"+str(targ)
                cnx = mysql.connector.connect(user='fatemeh', password='1234Fa$$', host='184.72.114.22', database='tp3')
                print ('Connection to DB')
                cursor = cnx.cursor()
                cursor.execute(command)
                print(f'handled by :{target_node}')
                result = cursor.fetchall()
                response={'handled by' :{target_node}, 'result':result}
                response = pickle.dumps(response)
                conn.send(response)


    print ('Will close socket')
    # send.close()
    listen.close()


def load_data(data):
    data=pickle.loads(data)
    
    type = 'select'

    if data['type']=='insert':
        type = 'insert'

    command = data['command']

    return type, command


def custom():
    responses = {}
    global target_list
    connections = [mysql.connector.connect(user='fatemeh', password='1234Fa$$',
                              host=ip) for ip in target_list]

    for node in connections:
        response = ping(node.server_host)
        responses[node.server_host]=response.rtt_avg

    best_node = min(responses, key=responses.get)
    best_node=str(best_node)
    if best_node=='52.206.95.187':
        node=0
    elif best_node=="44.201.140.131":
        node=1

    return(node)


if __name__ == '__main__':
    main()