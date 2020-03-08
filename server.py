#!/usr/bin/env python3

import socket
import sys
import os
import time 
import pickle
from collections import defaultdict
import threading

HOST = '127.0.0.1'  # Standard loopback interface address (localhost)
PORT = 5000        # Port to listen on (non-privileged ports are > 1023)

class block_chain():
    def __init__(self, other_chain=None):
        self.chain = other_chain if other_chain != None else []

    def add(self, dest, source, amt):
        balances = self.get_balance()
        if(balances[source] < amt):
            return False
        else:
            self.chain.append({
                'source': source,
                'dest': dest,
                'amt': amt
            })
            return True 
    def get_balance(self):
        balances = {'a' : 10, 'b' : 10, 'c' : 10}
        for transaction in self.chain:
            balances[transaction['source']] -= transaction['amt']
            balances[transaction['dest']] += transaction['amt']
        return balances

bc = block_chain()

def listen():
    global bc
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, PORT))
        s.listen()
        while True:
            conn, addr = s.accept()
            with conn:
                print('Connected by', addr)
                while True:
                    raw = conn.recv(4096)
                    data  = pickle.loads(raw)
                    print(data)
                    if data['dest'] != "balance":
                        result = ['transaction', bc.add(data['dest'], data['source'], int(data['amt']))]
                    else:
                        result = ['balance', bc.get_balance()]
                    conn.sendall(pickle.dumps(result))
                    print("Balances:", bc.get_balance())
                    print("Chain:", bc.chain)
                    conn.close()
                    break


if __name__ == "__main__":
    # print(bc.add("a", "b", 123))
    # print(bc.add("a", "b", 10))
    # print(bc.get_balance())
    # sys.exit()
    threading.Thread(target=listen, args=()).start()
    while True:
        i = input("(1) get balance (2) history:")
        if i == '1':
            print("Balance", bc.get_balance())
        else:
            print("Transaction history", bc.chain)

