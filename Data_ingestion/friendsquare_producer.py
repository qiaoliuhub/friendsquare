# coding: utf-8

import six
from datetime import datetime
from datetime import timedelta
from kafka.client import SimpleClient
from kafka.producer import KeyedProducer
import sys
import random
import pandas as pd
import json
import time
import numpy as np

class Producer(object):

    '''
    Messages are sent to a single kafka topic "Friendsquare" as a json formatted string
    '''
    
    def __init__(self, addr, userslist, venueslist):
        self.client = SimpleClient(addr)
        self.producer = KeyedProducer(self.client)
        self.userslist = userslist[0:500000]
        self.venueslist = venueslist[0:250000]

    def produce_msgs(self, partitionkey):
        new_time = datetime.now()
        msg_cnt = 0
        while True:
            if ((msg_cnt%40)!=0):
                userid=int(random.choice(self.userslist))
                venueid=int(random.choice(self.venueslist))
            else:
                userid=int(random.choice(self.userslist[0:100000]))
                venueid=int(random.choice(self.venueslist[0:50000]))
            rating= random.randint(0,5)
            randomdelta = np.random.normal(3, 3, 1)[0]
            new_time += timedelta (seconds = randomdelta)
            created_time = new_time.strftime("%Y-%m-%d %H:%M:%S")
            message_info = {'partitionkey':partitionkey, 'userid':userid, 'venueid': venueid, 'created_at':created_time, 'rating': rating}
            msg_info = json.dumps(message_info)
            print message_info
            self.producer.send_messages('Friendsquare', partitionkey, msg_info)
            msg_cnt += 1
            #time.sleep(0.02)

if __name__ == "__main__":
    args = sys.argv
    ip_addr = str(args[1])
    partitionkey = str(args[2])
    with open("./userslist.id",'r') as f:
        userslist=json.load(f)
    with open("./venueslist.id",'r') as v:
        venueslist=json.load(v)
    prod = Producer(ip_addr, userslist, venueslist)
    prod.produce_msgs(partitionkey) 
