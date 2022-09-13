import json
import os
from dateutil.parser import parse
import pandas as pd

log_path = "./discv5-test/logs"

def logs_into_df(log_path):
    rows = []
    for log_file in os.listdir(log_path):
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
            
            #we have a key to the message specified
            #currently it can only be the topic
            if(':' in jsons['msg'].split(' ')[1]):
                row['key'] = jsons['msg'].split(' ')[1].split(':')[1]
            #print(row)
            rows.append(row)
            
    return pd.DataFrame(rows)

df = logs_into_df(log_path)

print(df['msg_type'].value_counts())
print(df['in_out'].value_counts())

        
        