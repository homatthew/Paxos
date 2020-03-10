from client import Blockchain
import pprint
import random


bc = Blockchain()

t1 = {
  'source': 'b',
  'dest': 'a',
  'amt': 1,
  'seq': 2,
}
t2 = {
  'source': 'b',
  'dest': 'c',
  'amt': 2,
  'seq': 1,
}
trans_li = [t1, t2]
val1 = bc.create_block(trans_li, 0)
pprint.pprint(val1)
bc.add(val1, 0)
def create_transaction(s, d, a, seq):
  return {
    'source': s,
    'dest': d,
    'amt': a,
    'seq': 1
  }


clients = {'a': 1,'b': 1,'c': 1}
for i in range(1,100):
  trans_li = []
  for j in range(random.randint(4,10)):
    s, d = random.sample(clients.keys(), 2)
    clients[s] += 1
    trans_li.append(create_transaction(s, d, random.randint(1,4), clients[s]))
  bc.add(bc.create_block(trans_li, i), i)

print(bc.validate())