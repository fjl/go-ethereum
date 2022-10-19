import json
import os
from dateutil.parser import parse
import pandas as pd
import hashlib
import numpy
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import seaborn as sns
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
            row['msg_type'] = jsons['msg'].split(' ')[1]
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
            if("topic" in jsons):
                #replace topic digest by topic name
                topic = jsons['topic']
                row['key'] = topic_mapping[topic]
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

def plot_operation_returned(fig_dir,op_df):
    ax = op_df['reply_received'].value_counts().plot(kind = 'pie', autopct='%1.0f%%', legend=True, title='Operation returned')
    ax.figure.savefig(fig_dir + 'op_returned.eps',format='eps')

def plot_operation_times(fig_dir,op_df):
    fig, axes = plt.subplots()
    df = op_df[~op_df['time'].isna()]
    sns.violinplot(x='method',y='time', data=df, ax = axes, cut=0)
    fig.savefig(fig_dir + 'operation_time.eps',format='eps')

def plot_msg_operation(fig_dir,msg_df):

    colors = ['red', 'green', 'blue', 'yellow']

    for op_type, group_op_type in msg_df.groupby('op_type'):

        fig, ax = plt.subplots()
        legend_elements = []
        added = set()
        for opid, group_opid in group_op_type.groupby('opid'):
            #print("\t", op_type)
            i = 0
            sum = 0
            for msg_type, group_msg_type in group_opid.groupby('msg_type'):
                val = len(group_msg_type)
                ax.bar(opid, val, color=colors[i], bottom = sum)
                sum += val
            # print("\t\t", msg_type, len(group_msg_type))
                if(msg_type not in added):
                    added.add(msg_type)
                    legend_elements.append(Line2D([0], [0], color=colors[i], lw=4, label=msg_type))
                i += 1
        ax.legend(handles=legend_elements)
        ax.set_title(op_type)

        fig.savefig(fig_dir + op_type+'.eps',format='eps')

def plot_msg_sent_recv(fig_dir,msg_df):
    ax = msg_df['in_out'].value_counts().plot(kind='pie', autopct='%1.0f%%', legend=True, title='Msgs sent/received')
    ax.figure.savefig(fig_dir + 'msg_sent_received.eps',format='eps')

def plot_msg_sent_recv2(fig_dir,msg_df):
    sent = msg_df[msg_df['in_out'] == 'out']['node_id'].value_counts().to_dict()
    sent = {int(k):int(v) for k,v in sent.items()} #convert IDs to int

    received = msg_df[msg_df['in_out'] == 'out']['peer_id'].value_counts().to_dict()
    received = {int(k):int(v) for k,v in received.items()} #convert IDs to int

    fig, ax = plt.subplots()

    width =0.3
    ax.bar(sent.keys(), sent.values(), width=width, label = 'sent')
    ax.bar([x + width for x in received.keys()], received.values(), width=width, label = 'received')
    ax.legend()
    ax.set_title('Messages exchanged')
    ax.set_xlabel('Node ID')
    ax.set_ylabel('#Messages')
    fig.savefig(fig_dir + 'messages.eps',format='eps')

def plot_msg_sent_distrib(fig_dir,msg_df):
    fig, axes = plt.subplots()

    df_in = msg_df[msg_df['in_out']=='in']['node_id'].value_counts().rename_axis('node_id').reset_index(name='count')
    df_in['in_out'] = 'in'
    df_out = msg_df[msg_df['in_out']=='out']['node_id'].value_counts().rename_axis('node_id').reset_index(name='count')
    df_out['in_out'] = 'out'

    df = pd.concat([df_in, df_out], axis=0)
    sns.violinplot(x='in_out', y='count', data=df, ax = axes, cut=0, title='#Msg received/sent distribution per node')
    fig.savefig(fig_dir + 'msg_rcv_dist.eps',format='eps')

def plot_msg_topic(fig_dir,msg_df):
    ax = msg_df['msg_type'].value_counts().plot(kind='bar')
    ax.figure.savefig(fig_dir + 'msg_type_count.eps',format='eps')

    for op_type, group_op_type in msg_df.groupby('op_type'):
        print(op_type)
        fig, ax = plt.subplots()
        group_op_type['topic'].value_counts().plot(kind='bar', title=op_type)
        ax.set_ylabel("#Messages")
        fig.savefig(fig_dir + op_type+'_msg_per_topic.eps',format='eps')

def plot_times_discovered(fig_dir,op_df):
    op_df_exploded = op_df.copy()
    op_df_exploded = op_df_exploded.explode('result')
    fig, axes = plt.subplots()
    op_df_exploded['result'].value_counts().plot(ax = axes,kind='bar')
    axes.set_xticklabels([])
    axes.set_xlabel("Discovered Nodes")
    axes.set_ylabel("Count")
    axes.set_yticks(list(op_df_exploded['result'].value_counts()))
    fig.savefig(fig_dir + 'times_discovered.eps',format='eps')

def plot_search_results(fig_dir,op_df):
    op_df_exploded = op_df.copy()
    op_df_droppedNone = op_df_exploded.dropna(subset=['result'])
    fig, axes = plt.subplots()
    op_df_droppedNone['opid'].value_counts().plot(ax=axes, kind='bar')
    axes.set_xlabel("Topic search operation")
    axes.set_ylabel("Number of results")
    axes.set_yticks(list(op_df_droppedNone['opid'].value_counts()))
    fig.savefig(fig_dir + 'discovered_search.eps',format='eps')

def plot_waiting_time(fig_dir,msg_df):

    #consider only final regconfig message
    df = msg_df.dropna(subset=['ok'], inplace=False)
    fig, ax = plt.subplots()
    sns.violinplot(x='topic',y='total_wtime', data=df, ax = ax, cut = True)
    fig.savefig(fig_dir + 'waiting_time.eps',format='eps')

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
