import json
import os
from dateutil.parser import parse
import pandas as pd
import hashlib
import numpy

log_path = "./discv5-test/logs"

def get_msg_df(log_path, op_df):
    topic_mapping = {} #reverse engineer the topic hash
    for i in range(1, 100):
        topic_mapping[hashlib.sha256(('t'+str(i)).encode('utf-8')).hexdigest()] = i

    op_info = {}
    for opid in set([i for i in op_df['opid']]):
        op_type = op_df.loc[op_df['opid'] == opid, 'method'].values[0]
        topic = op_df.loc[op_df['opid'] == opid, 'topic'].values[0]
        op_info[opid] = {'op_type':op_type, 'topic':topic}
        
    rows = []
    for log_file in os.listdir(log_path):
        if (not log_file.startswith("node-")):
            continue
        print("Reading", log_file)
        node_id = log_file.split('-')[1].split('.')[0] #node-10.log
        for line in open(log_path + '/' + log_file, 'r').readlines():
            #not a json line
            if(line[0] != '{'):
                continue
            row = {}
            row['node_id'] = node_id
            #print("\t", line)
            jsons = json.loads(line)
            #it's not a message sent between peers
            if('addr' not in jsons):
                continue
            #get peer ID from the port number
            row['peer_id'] = int(jsons['addr'].split(':')[1]) - 30200
            in_out_s = jsons['msg'].split(' ')[0]
            if(in_out_s == '<<'):
                row['in_out'] = 'in'
            elif(in_out_s == '>>'):
                row['in_out'] = 'out'
            else:
                #it's not a message sent between peers
                continue
            row['timestamp'] = parse(jsons['t'])
            row['msg_type'] = jsons['msg'].split(' ')[1].split(':')[0]
            if('req' in jsons):
                row['req_id'] = jsons['req']
            #print(row)
            if('opid' in jsons):
                row['opid'] = jsons['opid']

            if("total-wtime" in jsons):
                row['total_wtime'] = jsons['total-wtime']
            if("ok" in jsons):
                row['ok'] = jsons['ok']
            
            #we have a key to the message specified
            #currently it can only be the topic
            if(':' in jsons['msg'].split(' ')[1]):
                #replace topic digest by topic name
                row['key'] = topic_mapping[jsons['msg'].split(' ')[1].split(':')[1]]
            #print(row)
            rows.append(row)


    msg_df = pd.DataFrame(rows) 
    #keep only the send messages (as they have the opid)
    msg_df = msg_df[msg_df['in_out'] == 'out']

    mapping = {}
    def process(row):
        op_id = row['opid']
        req_id = row['req_id']
    
        if(not numpy.isnan(op_id)):
            mapping[req_id] = op_id

        if(req_id in mapping):
            row['tmp'] = mapping[req_id]
            if(not numpy.isnan(row['tmp'])):
                row['topic'] = op_info[row['tmp']]['topic']
                row['op_type'] = op_info[row['tmp']]['op_type']
        else:
            row['tmp'] = numpy.NaN
            row['topic'] = numpy.NaN
            row['op_type'] = numpy.NaN
        return row
     
    msg_df = msg_df.apply(lambda row : process(row), axis = 1)
    msg_df['opid'] = msg_df['tmp']
    msg_df.drop('tmp', axis=1, inplace=True)
    msg_df = msg_df.dropna(subset=['opid'])
    
    return msg_df

def get_op_df(log_path):
    topic_mapping = {} #reverse engineer the topic hash
    for i in range(1, 100):
        topic_mapping[hashlib.sha256(('t'+str(i)).encode('utf-8')).hexdigest()] = i

    operations = {} #indexed by opid
    for line in open(log_path + '/logs.json', 'r').readlines():
        #not a json line
        if(line[0] != '{'):
            continue
        #print("###line:")
        #print(line)
        row = {}
        jsons = json.loads(line)
        #it's a RPC request
        opid = jsons['opid']
        if('method' in jsons):
            #print("opid:", opid, "req")
            #we can't have 2 operations with the same ID
            assert (opid not in operations)
            row = {}
            row['opid'] = opid
            row['method'] = jsons['method']
            row['reply_received'] = False
            
            jsons['params'][0] = int(topic_mapping[jsons['params'][0][2:]])#drop the 0x at the begining
            row['params'] = jsons['params']
            row['start_time'] = jsons['time']
        #it's a RPC reply
        else:
            #we shouldn't receive a reply without seeing a request
            assert (opid in operations)
            #print("opid:", opid, "reply")
            row = operations[opid]
            #we should have only one reply per request
            assert(row['reply_received'] == False)
            row['reply_received'] = True
            row['result'] = jsons['result']
            row['end_time'] = jsons['time']
            row['time'] = row['end_time'] - row['start_time']
            assert(row['time'] >= 0)

        #print("~~~row:")
        #print(row)
        operations[opid] = row

    #print(operations)

    op_df = pd.DataFrame(operations.values())
    for i, row in op_df.iterrows():
        op_df.at[i, 'topic'] = row['params'][0]


    return op_df

    



            
    
#op_df = get_op_df('./discv5-test/logs')
#print("op_df")
#print(op_df)
#msg_df = get_msg_df('./discv5-test/logs', op_df)
#msg_df = msg_df.dropna(subset=['tmp'])
#print("msg_df")
#print(msg_df)



#df = logs_into_df(log_path)

#print(df['msg_type'].value_counts())
#print(df['in_out'].value_counts())

        
        