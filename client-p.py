#!/usr/bin/python3

import socket
import pickle
import argparse


parser = argparse.ArgumentParser()
parser.add_argument('operation', help='mode of proxy, direct, random or custom')
args = parser.parse_args()

operation=args.operation 

def main():

    host = "50.16.7.50"
    port = 5001 

    print ('connecting to ' + host + ':' + str(port))

    s = socket.socket()
    s.connect((host, port))
    print("connected")
    

    with open('data.csv', 'r') as f:
        next(f) 
        lines = f.readlines()
        if operation=='insert':
            for line in lines:
                line = line.strip('\n')
                firstname, lastname, zip1= line.split(",")
                cmd = 'INSERT INTO transactions VALUES (' +"'"+  + firstname + "'," + lastname + "," + zip1 + "');"
                cmd_type = 'insert'
                obj = {'type': cmd_type, 'command': cmd}
                pickledobj = pickle.dumps(obj)
                s.send(pickledobj)
                data = s.recv(2048)
                data=pickle.loads(data)
                print ('insert into master: ' + data['response'])
            
        if operation=='select':

            for line in lines:
                line = line.strip('\n')
                firstname, lastname, zip1 = line.split(",")
                cmd = 'SELECT * FROM transactions WHERE zip1 = ' + zip1 + ";"
                cmd_type = 'select'
                obj = {'type': cmd_type, 'command': cmd}
                pickledobj = pickle.dumps(obj)
                s.send(pickledobj)
                data = s.recv(1024)
                data=pickle.loads(data)
                print (data["handled by"], data["result"])
            

    print ('closing socket')
    s.close()


if __name__ == '__main__':
    main()