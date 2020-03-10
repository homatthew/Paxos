#!/usr/bin/env python3

import pickle
import threading
import socket as sock
from collections import defaultdict
import time
import sys
import os
import pprint
import copy
import traceback
import hashlib
import random
from struct import pack, unpack



client_list = [3001,3002,3003]
client_name_to_port = {'a' : 3001, 'b' : 3002, 'c' : 3003}
network_partition = [] #This prevents communication with others
client_port_to_name = {value:key for key, value in client_name_to_port.items()}
my_index = client_list.index(int(sys.argv[1]))
proc_id = int(sys.argv[1])
class t_seq:
    def __init__(self):
        self.count = 0
        self.lock = threading.Condition()
    def get_seq(self):
        with self.lock:
            self.count += 1
            return self.count
trans_seq = t_seq()

""" 
    Pending_trans -> transactions that need to be verified 
"""
events = {
    'queue' : [],
    'parallel_events': [],
    'lock' : threading.Condition()
}

class Blockchain():
    def __init__(self, new_chain=[]):
        self.chain = new_chain
        self.chain_lock = threading.Condition()
        self.init_list = ['a', 'b', 'c']
    def dump_chain(self):
        with self.chain_lock:
            f = open("ledger_%d.txt" % (int(sys.argv[1])), "wb+")
            pickle.dump(self.chain, f)
            f.close()
    def calc_hash(self, index = 0):
        with self.chain_lock:
            prev_hash = ""
            if(index == -1):
                prev_hash = "NULL"
                return prev_hash
            else:
                prev_hash = self.chain[index]['prev_hash']
            b = self.chain[index]
        return str(hashlib.sha256(str(prev_hash+str(index)+b['nonce']+str(b['tx'])).encode()).hexdigest())
    def add(self, block, depth):
        with self.chain_lock:
            if depth != len(self.chain):
                print("Ignoring add. Tried to insert depth(%d). Current lenth is %d" % (depth, len(self.chain)))
                return False
            self.chain.append(block)
            self.dump_chain()
            return True 
    def create_block(self,trans_li, depth):
        prev_hash = self.calc_hash(depth-1)
        nonce = ""
        h = ""
        i = 0
        while(not (h.endswith('0') or h.endswith('1'))):
            i+= 1
            nonce = str(hashlib.sha256(str.encode(str(i))).hexdigest())
            a = str(trans_li)
            h = str(hashlib.sha256(str.encode(str(prev_hash)+str(depth)+str(nonce)+a)).hexdigest())
        block = {
            'depth': depth,
            'tx' : copy.deepcopy(trans_li),
            'nonce': nonce,
            'prev_hash': prev_hash
        }
        return block
    def get_balance(self):
        with self.chain_lock:
            balances = defaultdict(lambda: 10)
            for item in self.init_list:
                balances[item] = 10
            for block in self.chain:
                for transaction in block['tx']:
                    balances[transaction['source']] -= transaction['amt']
                    balances[transaction['dest']] += transaction['amt']
            return dict(balances)
    def validate(self):
        with self.chain_lock:
            for i, e in enumerate(self.chain):
                if self.calc_hash(i-1) != self.chain[i]['prev_hash']:
                    pprint.pprint(self.chain[i])
                    return False
                cur = self.calc_hash(i)
                if not (cur.endswith('0') or cur.endswith('1')):
                    # print("her", i, cur)
                    return False
            return True

def create_transaction(source, dest, amt):
    global trans_seq
    return {
        'source' : source,
        'dest': dest,
        'amt': amt,
        'seq': trans_seq.get_seq()
    }

def decomp(dic, li):
    ret = []
    for e in li:
        ret.append(dic[e])
    return tuple(ret)

class Paxos:
    class __Paxos():
        def __init__(self):
            self.lock = threading.Condition()
            self.proc_id = int(sys.argv[1])
            self.ballot = (0, self.proc_id)
            self.bc = Blockchain()
            self.timed_out = False
            self.timeout_phase = 1
            self.num_promises = 0
            self.acceptNum = None
            self.acceptVal= None
            self.num_acceptack = 0
            self.NUM_SERVERS = 3
            self.REPLY_COUNT = int(self.NUM_SERVERS / 2)
            self.TIMEOUT_IN_SEC = (5,10)
            self.output_sockets = None
            self.my_trans = []
            self.received_trans= []
            self.name =  None
        def initialize(self, output_sockets, chain=[], my_trans=[],num_servers=3, name=None):
            self.output_sockets = output_sockets
            self.my_trans = my_trans if my_trans != None else []
            self.bc = Blockchain(chain) if chain != None else Blockchain()
            self.NUM_SERVERS = num_servers
            self.REPLY_COUNT = int(self.NUM_SERVERS / 2)
            self.name= name
        def get_round(self):
            with self.lock:
                return len(self.bc.chain)
        def get_new_ballot(self, other_ballot = None):
            with self.lock:
                if other_ballot != None:
                    self.ballot = (max(self.ballot[0], other_ballot[0]), self.proc_id)
                self.ballot = (self.ballot[0]  + random.randint(1,5), self.proc_id)
                return self.ballot
        def add_localtx(self, trans):
            with self.lock:
                self.my_trans.append(trans)
                f = open("curr-trans_%d.txt" % (self.proc_id), "wb+")
                pickle.dump(self.my_trans, f)
                f.close()
        def get_balance(self):
            bal = self.bc.get_balance()
            for t1 in self.my_trans:
                bal[t1['source']] -= t1['amt']
                bal[t1['dest']] += t1['amt']
            return bal
        def is_valid_tx(self, trans):
            with self.lock:
                bal = self.get_balance()
                bal[trans['source']] -= trans['amt']
                bal[trans['dest']] += trans['amt']
                for balance in bal.values():
                    if balance < 0:
                        return False
                return True
        """
            Phase 1: Leader election / Value discovery
        """
        def wait_for_promises(self):
            with self.lock:
                self.num_promises = 0
                self.acceptVal = None
                self.acceptNum = None
                self.received_trans= []
                roundNum = Paxos().get_round()
                bal = Paxos().get_new_ballot()
                msg = {
                    'type': 'prepare',
                    'ballot': bal,
                    'roundNum': roundNum,
                }
                send_message(self.output_sockets, -1, msg)

                threading.Thread(target=self.timeout, args=(roundNum,1)).start()
                return bal
        def check_promise_count(self, roundNum):
            with self.lock:
                if self.timed_out:
                    return -1
                elif self.get_round() != roundNum:
                    return -2
                else:
                    return self.num_promises
        def send_promise(self, dest_port, ballot):
            with self.lock:
                Paxos().get_new_ballot(ballot)
                msg={
                    "type": "promise",
                    "ballot": ballot,
                    "roundNum": self.get_round()
                }
                if self.acceptNum != None:
                    msg["acceptNum"] = self.acceptNum
                    msg["acceptVal"] = self.acceptVal
                else:
                    msg["acceptNum"] = None
                    msg["acceptVal"] = copy.deepcopy(self.my_trans)
                send_message(self.output_sockets, dest_port, msg)
        def send_higher_ballot(self, dest_port):
            with self.lock:
                msg = {
                    "type": "higher_ballot",
                    "ballot": self.ballot,
                    "roundNum": self.get_round()
                }
                send_message(self.output_sockets, dest_port, msg)
        def receive_promise(self, source_port, otherRound, other_ballot, acceptNum, acceptVal):
            with self.lock:
                if otherRound < self.get_round():
                    self.send_resync(source_port)
                    return #Ignore old rounds
                elif otherRound > self.get_round():
                    self.ask_resync(source_port)
                    return
                if other_ballot != self.ballot:
                    # self.send_higher_ballot(source_port) TODO:
                    print("Received a promise from old ballot")
                    return 

                if acceptNum != None:
                    if self.acceptNum == None:
                        self.acceptNum = (acceptNum[0] - 1, self.proc_id)
                    if acceptNum > self.acceptNum:
                        self.acceptNum = (acceptNum[0], self.proc_id)
                        self.acceptVal = acceptVal
                else:
                    self.received_trans.extend(acceptVal)
                self.num_promises += 1
        """
            Phase 2: Value proposition
        """
        def wait_acceptack(self, ballot, roundNum):
            if self.acceptVal == None:
                all_transactions = copy.deepcopy(self.received_trans) + copy.deepcopy(self.my_trans)
                propose_block = self.bc.create_block(all_transactions, roundNum)
            else:
                propose_block = self.acceptVal
            self.acceptVal = propose_block
            self.acceptNum = ballot
            self.num_acceptack = 0
            msg = {
                'type': 'accept',
                'acceptVal': propose_block,
                'acceptNum': ballot,
                'roundNum': roundNum 
            }
            send_message(self.output_sockets, -1, msg)
            threading.Thread(target=self.timeout, args=(roundNum,2)).start()
            return self.acceptVal
        def check_acceptack_count(self, roundNum):
            with self.lock:
                if self.timed_out:
                    return -1
                elif self.get_round() != roundNum:
                    return -2
                else:
                    return self.num_acceptack
        def send_acceptack(self, dest_port, ballot, roundNum, proposedValue):
            with self.lock:
                if roundNum < self.get_round():
                    send_resync(dest_port)
                    return
                elif roundNum > self.get_round():
                    ask_resync(dest_port)
                    return
                if self.acceptNum == None or self.acceptNum < ballot:
                    self.acceptVal = proposedValue 
                    self.acceptNum = ballot
                    msg={
                        "type": "acceptack",
                        "ballot": ballot,
                        "roundNum": self.get_round()
                    }
                    print('sending accept_ack')
                    send_message(self.output_sockets, dest_port, msg)
                else:
                    print("Received accept: acceptNum(%s) is stale. Mine is (%s)" % (str(ballot), str(self.ballot)))

        def receive_acceptack(self, source_port, otherRound, acceptNum):
            with self.lock:
                if otherRound < self.get_round():
                    self.send_resync(source_port)
                    return #Ignore old rounds
                elif otherRound > self.get_round():
                    self.ask_resync(source_port)
                    return
                if acceptNum != self.acceptNum:
                    print("Received acceptNum(%d) is stale. Mine is (%d)" % (acceptNum, self.acceptNum))
                    return
                self.num_acceptack += 1
        
        
        def send_decide(self, value, roundNum):
            msg = {
                "type": "decide",
                "value": value,
                "roundNum": roundNum,
                "tx": None
            }
            send_message(self.output_sockets, -1, msg)
            self.acceptVal = None
            self.acceptNum = None
        def receive_decision(self, value, depth):
            if not self.bc.add(value, depth):
                print('Already inserted block of depth %d. My depth is %d' % (depth, self.get_round()))
                return
            max_seq = -1
            for tx in value['tx']:
                if (tx['source'] == self.name and max_seq < tx['seq']):
                    max_seq = tx['seq']
            print(max_seq)
            self.my_trans = [tx for tx in self.my_trans if tx['seq'] > max_seq]
            self.send_decide(value, depth)
            return
        def do_resync(self, other_chain):
            with self.lock:
                if len(other_chain) > len(self.bc.chain):
                    self.bc = Blockchain(other_chain)
                    self.acceptNum = None
                    self.acceptVal = None
                    self.timed_out=False
                    self.ballot = (0,self.proc_id)
        def send_resync(self, dest_port):
            with self.lock:
                msg = {
                    'type': 'resync',
                    'chain': copy.deepcopy(self.bc.chain)
                }
                print('sending resync')
                send_message(self.output_sockets, dest_port, msg)
        def ask_resync(self, dest_port):
            with self.lock:
                msg = {
                    'type': 'resync_request'
                }
                send_message(self.output_sockets, dest_port, msg)
        def timeout(self, roundNum, timeout_phase):
            self.timed_out = False
            self.timeout_phase  = timeout_phase
            rTime = random.randint(*self.TIMEOUT_IN_SEC)
            print("sleeping for", rTime)
            time.sleep(rTime)
            print("TIME OUT THREAD WOKE UP")
            with self.lock:
                print("TIME OUT THREAD WOKE UP")
                if roundNum == self.get_round() and self.timeout_phase == timeout_phase:
                    self.timed_out = True
    instance = None
    def __new__(cls): # __new__ always a classmethod
            if not Paxos.instance:
                Paxos.instance = Paxos.__Paxos()
            return Paxos.instance
    def __getattr__(self, name):
        return getattr(self.instance, name)
    def __setattr__(self, name):
        return setattr(self.instance, name)


exists = os.path.isfile('ledger_%d.txt' % proc_id)
from_file_bc = []
if(exists and os.stat("ledger_%d.txt" % proc_id).st_size != 0):
    f = open("ledger_%d.txt" % (proc_id), "rb")
    from_file_bc = pickle.load(f)
    f.close()

from_file_trans =[]
exists = os.path.isfile('curr-trans_%d.txt' % proc_id)
if(exists and os.stat("curr-trans_%d.txt" % proc_id).st_size != 0):
    f = open("curr-trans_%d.txt" % (proc_id), "rb")
    from_file_trans = pickle.load(f)
    f.close()

def listen_to_socket(socket, port, out_sockets):
    """
    Receives messages for this socket and adds events to the global event queue
    """
    global events
    try:
        while True:
            rl = socket.recv(8)
            (raw_len,) = unpack('>Q', rl)
            raw_data = b''
            while(len(raw_data) < raw_len):
                to_read = raw_len - len(raw_data)
                raw_data += socket.recv(4096 if to_read > 4096 else to_read)
            message = pickle.loads(raw_data)
            print("Received the following message:")
            pprint.pprint(message)

            with events['lock']:
                if message:
                    events['queue'].append({"source": port, "data" : message})
    except:
        out_sockets.pop(port, None)
        traceback.print_exc()
        print(port, "disconnected")
    return 0
def handle_new_connections(socket, outputs):
    while True:
        conn, addr = socket.accept()
        print('Connected by', addr)
        rl = conn.recv(8)
        (raw_len,) = unpack('>Q', rl)
        raw_data = b''
        while(len(raw_data) < raw_len):
            to_read = raw_len - len(raw_data)
            raw_data += conn.recv(4096 if to_read > 4096 else to_read)
        message = pickle.loads(raw_data)
        print('here', message)
        try:
            if(message['type'] != 'greet' or int(message['body']) in outputs.keys()):
                msg = {'type': 'goodbye'}
                raw_msg = pickle.dumps(msg)
                send_bytes(conn, raw_msg)
                conn.close()
            else:
                outputs[message['body']] = conn
                threading.Thread(target=listen_to_socket, args=(conn, message['body'], outputs)).start()
        except:
            traceback.print_exc()
            outputs.pop(message['body'], None)
            conn.close()

def send_message(output_sockets, dest_port,msg):
    ports_li = []
    if dest_port == -1:
        for port in output_sockets.keys():
            if port not in network_partition:
                ports_li.append(port)
    elif dest_port not in network_partition:
        ports_li.append(dest_port)

    for port in ports_li:
        raw = pickle.dumps(msg)
        send_bytes(output_sockets[port], raw)

def send_bytes(sock, msg_raw):
    length = pack('>Q', len(msg_raw))
    sock.sendall(length)
    sock.sendall(msg_raw)

def event_loop(output_sockets):
    global events
    global my_index
    global client_list
    global client_name_to_port
    global client_port_to_name
    global from_file_bc
    global from_file_trans

    def pop_current(has_seq_event, event_i, total_events):
        global events
        with events['lock']:
            if has_seq_event and event_i == len(randomSample) - 1:
                events['queue'].pop(0)
            else:
                events['parallel_events'].pop(event_i)


    Paxos().initialize(output_sockets, from_file_bc,  from_file_trans, 3, client_port_to_name[client_list[my_index]])
    while True:
        time.sleep(1)
        #Get entry if queue is not empty
        try:
            with events['lock']:
                if len(events['queue']) + len(events['parallel_events']) > 0:
                    randomSample = []
                    randomSample.extend(events['parallel_events'])
                    has_seq_event = False
                    if len(events['queue']) > 0:
                        has_seq_event = True
                        randomSample.append(events['queue'][0])
                    event_i = random.randint(0, len(randomSample) - 1)
                    event = randomSample[event_i]

                    print('-' * 50)
                    event_type = event['data']['type']
                    print("Reading event", end="\n\t")
                    pprint.pprint(event)
                    print("Current eventQueue", end="\n\t")
                    pprint.pprint(events['queue'])
                    print("Current eventParallelQueue", end="\n\t")
                    pprint.pprint(events['parallel_events'])
                    print('-' * 50)


                    if event_type == "transaction":
                        tr = create_transaction(*decomp(event['data'], ['source', 'dest', 'amt']))
                        if Paxos().is_valid_tx(tr):
                            Paxos().add_localtx(tr)
                        else:
                            events['queue'].append({
                                    'data': {
                                        'type': 'wait_prepare',
                                        'tx': tr
                                    }
                                })

                        
                    elif event_type == "wait_prepare":
                        bal = Paxos().wait_for_promises()
                        events['parallel_events'].append({
                            'data': {
                                'type': 'check_promise_count',
                                'tx': event['data']['tx'],
                                'roundNum': Paxos().get_round(),
                                'ballot': bal
                            }
                        })
                    elif event_type == "check_promise_count":
                        tx = event['data']['tx']
                        roundNum = event['data']['roundNum']
                        bal = event['data']['ballot']
                        result = Paxos().check_promise_count(roundNum)
                        if result == -1:
                            print('Paxos Phase 1: Timeout during leader election')
                            ans = input("Do you want to drop tx (aka fix nw partition before retrying)?") 
                            if ans == 'y':
                                pop_current(has_seq_event, event_i, len(randomSample))
                                continue 
                            else:
                                events['queue'].append({'data': { 'type': 'wait_prepare', 'tx': tx}})
                        elif result == -2:
                            print('Paxos Phase 1: My ballot out of date. Will try again with new ballot')
                            events['queue'].append({'data': { 'type': 'wait_prepare', 'tx': tx}})
                        elif result < Paxos().REPLY_COUNT:
                            print('Paxos Phase 1: Waiting for enough replies. Currently have %d replies' % result)
                            events['parallel_events'].append({'data': {'type': 'check_promise_count', 'tx': tx, 'roundNum': roundNum, 'ballot': bal}})
                        else:
                            print('SUCCESS Paxos Phase 1: RECEIVED %d REPLIES' % result)
                            events['queue'].append({'data': {'type': 'wait_acceptack', 'tx': tx, 'roundNum': roundNum, 'ballot': bal}})
                    elif event_type == "prepare":
                        reply_dest = event['source']
                        received_roundNum= event['data']['roundNum']
                        received_ballot = event['data']['ballot']
                        my_roundNum = Paxos().get_round()
                        my_ballot = Paxos().ballot
                        if received_roundNum < my_roundNum:
                            print("Sending resync")
                            Paxos().send_resync(reply_dest)
                        elif received_roundNum > my_roundNum:
                            print("Asking for resync")
                            Paxos().ask_resync(reply_dest)
                        elif received_ballot > my_ballot:
                            print("sending promise")
                            Paxos().send_promise(reply_dest, received_ballot)
                        else:
                            print("my_ballot(%s) is greater than received_ballot(%s)" % (str(my_ballot), str(received_ballot)))
                    elif event_type == "promise":
                        Paxos().receive_promise(event['source'], *decomp(event['data'],['roundNum', 'ballot', 'acceptNum', 'acceptVal']))
                    elif event_type == "accept":
                        dest_port = event['source']
                        bal, roundNum, acceptVal = decomp(event['data'], ['acceptNum', 'roundNum', 'acceptVal'])
                        Paxos().send_acceptack(dest_port, bal, roundNum, acceptVal)
                    elif event_type == "wait_acceptack":
                        acceptVal = Paxos().wait_acceptack(event['data']['ballot'], event['data']['roundNum'])
                        events['parallel_events'].append({
                            'data': {
                                'type': 'check_acceptack_count',
                                'tx': event['data']['tx'],
                                'acceptVal': acceptVal,
                                'roundNum': event['data']['roundNum'],
                                'acceptNum': event['data']['ballot'],
                            }
                        })
                    elif event_type == "check_acceptack_count":
                        tx = event['data']['tx']
                        roundNum = event['data']['roundNum']
                        bal = event['data']['acceptNum']
                        value = event['data']['acceptVal']
                        result = Paxos().check_acceptack_count(roundNum)
                        if result == -1:
                            print('Paxos Phase 2: Timeout during value proposal')
                            ans = input("Do you want to try again? (aka fix nw partition before retrying) (Y/n)?") 
                            if ans == 'n':
                                pop_current(has_seq_event, event_i, len(randomSample))
                                continue 
                            else:
                                events['queue'].append({'data': { 'type': 'wait_prepare', 'tx': tx}})
                        elif result == -2:
                            print('Paxos Phase 2: I received a decision since starting paxos. Trying again on next round')
                            events['queue'].append({'data': { 'type': 'wait_prepare', 'tx': tx}})
                        elif result < Paxos().REPLY_COUNT:
                            print('Paxos Phase 2: Waiting for enough accept_ack. Currently have %d replies' % result)
                            events['parallel_events'].append({'data': copy.deepcopy(event['data'])})
                        else:
                            print('SUCCESS Paxos Phase 2: RECEIVED %d REPLIES' % result)
                            events['queue'].append({'data': {'type': 'decide', 'roundNum': roundNum, 'value': value, 'tx': tx}})
                    elif event_type == "acceptack":
                        Paxos().receive_acceptack(event['source'], *decomp(event['data'], ['roundNum', 'ballot']))
                    elif event_type == "decide":
                        received_value = event['data']['value']
                        roundNum = event['data']['roundNum']
                        tr = event['data']['tx']
                        Paxos().receive_decision(received_value, roundNum)
                        if tr != None:
                            if Paxos().is_valid_tx(tr):
                                Paxos().add_localtx(tr)
                            else:
                                print('Invalid transaction', tr)
                    elif event_type == "resync":
                        received_chain = event['data']['chain']
                        Paxos().do_resync(received_chain)
                    elif event_type == "resync_request":
                        Paxos().send_resync(event['source'])
                    else:
                        print('\n\nERROR: Received invalid event!!!!!!!!, %s\n\n' % (event_type))

                    pop_current(has_seq_event, event_i, len(randomSample))

        except Exception as e:
            traceback.print_exc()

def transaction_client(host, port):
    global events
    global my_index
    global client_list
    global client_name_to_port
    global network_partition

    with sock.socket(sock.AF_INET, sock.SOCK_STREAM) as s_listen:
        s_listen.bind((host, port))
        s_listen.listen(5)
        output_sockets = {} #{port: socket}
        threading.Thread(target=handle_new_connections, args=(s_listen, output_sockets)).start() 
        threading.Thread(target=event_loop, args=(output_sockets,)).start()
        #Connect to all other clients
        input("Give an input when ready to connect()...\n")
        for i in range(len(client_list)):
            client_port = client_list[i]
            if client_port != port:
                try:
                    s = sock.socket(sock.AF_INET, sock.SOCK_STREAM)
                    if client_port not in output_sockets.keys():
                        s.connect(('localhost', client_port))
                        msg={'type': 'greet', 'body': port}
                        raw = pickle.dumps(msg)
                        send_bytes(s, raw)
                        output_sockets[client_port] = s
                        print("Successfully connected to ", client_port)
                        threading.Thread(target=listen_to_socket, args=(s, client_port,output_sockets)).start()
                except Exception as e:
                    s.close()
                    print("Error connecting to", client_port, ": \n\t", e)
                    continue
        print(output_sockets.keys())

        
        #Run Transaction client
        while True:
            time.sleep(1)
            try:
                """
                    (0) Wait for event to resolve
                """
                while(len(events['queue']) > 0 or len(events['parallel_events']) > 0):
                    time.sleep(5)
                pprint.pprint(Paxos().get_balance())
                print(Paxos().my_trans)
                print(network_partition)
                """
                    (1) Get transaction
                """
                print("Pick an action:\n\t(1) Transfer money to another client\n\t(2) Initiate Paxos\n\t(3) Print blockchain\n\t(4) Print balance\n\t(5) Network Partition\n")
                action = input()
                event_data = {}
                if action == "1":
                    """
                        (1a) Add transaction to blockchain
                    """
                    print("You are logged in as", client_port_to_name[client_list[my_index]])
                    transaction = input("Enter transaction: ")
                    source, dest, amt = transaction.split(" ")
                    if(client_name_to_port[source]  != client_list[my_index]):
                        print("You cannot steal money from other people!")
                        continue
                    if(not (int(amt) > 0)):
                        print("Transfers must be positive")
                        continue
                    event_data = {
                        "type": "transaction", 
                        "source": source, 
                        "dest": dest, 
                        "amt": int(amt)
                        }
                elif action == "2":
                    """
                        (1b) Initiate Paxos
                    """
                    print("Initiating Paxos")
                    event_data = {
                        "type": "wait_prepare", 
                        "tx": None
                        }
                elif action == "3":
                    pprint.pprint(Paxos().bc.chain)
                    continue
                elif action == "4":
                    pprint.pprint(Paxos().get_balance())
                    continue
                elif action == "5":
                    ports_li = input("Enter which ports to stop sending to: ").split(" ")
                    if len(ports_li) == 0:
                        network_partition = []
                    else:
                        network_partition = [int(i) for i in ports_li]
                    continue
                else:
                    print("Please enter a valid action")
                    continue

                """
                    (2) add event to event_loop
                """
                with events['lock']:
                    event = {"source" : client_list[my_index], "data" : event_data}
                    events['queue'].append(event)                

            except KeyboardInterrupt:
                sys.exit()
            except Exception as e:
                traceback.print_exc()


if __name__ == "__main__":
    transaction_client('localhost', int(sys.argv[1]))