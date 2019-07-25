import findspark
findspark.init()
from pyspark import SparkContext

import networkx as nx
from datetime import datetime

#The user defined parameters
money_cut_off = 10000.
commission_cut_off = 0.9
group_by_time = 1 #days

sc = SparkContext(appName = "graph")
lines = sc.textFile('./small_transactions.csv')
header = lines.first()
lines = lines.filter(lambda x: x != header)
lines = lines.filter(lambda x: x.split('|')[1] != '2006-02-29')


def get_amount_per_node_transfer(lines):
    '''This function groups the transactions by a user defined timeframe and 
    the sender account id. It returns a dictionary where keys are (date,senderID)
    pairs and values are the total amount transferred based on these pairs.
    Also, if the total transferred amount per node is below a user defined amount
    that pair is being filtered.
    '''
    filtered_by_amount_per_node = {key: amount for key, amount in lines.map(lambda x: (
                                    (datetime.strptime(x.split('|')[1],'%Y-%m-%d'
                                    ).toordinal() // group_by_time, x.split('|')[3]),
                                    float(x.split('|')[2]))).reduceByKey(lambda x,y: 
                                    x+y).filter(lambda x: x[1]>money_cut_off).collect()}
    
    return filtered_by_amount_per_node

filtered_by_amount_per_node = get_amount_per_node_transfer(lines)


def get_graph_edges(lines, filtered_by_amount_per_node):
    '''This function filters all the transactions by using the previously defined
    dictionary, based on the cut off transaction value per node. It returns two
    dictionaries; first one has keys that are dates and values are 
    (sender, receiver, amount) to construct the graph. The other dictionary has the 
    (transaction, day) tuple as the keys and (sender, receiver, amount) as the values.
    '''
    def filter_transactions(row):
        if (datetime.strptime(row.split('|')[1],'%Y-%m-%d'
            ).toordinal() // group_by_time, row.split('|')[3]) in filtered_by_amount_per_node.keys():
            return (datetime.strptime(row.split('|')[1],'%Y-%m-%d').toordinal() // group_by_time, 
                    row.split('|')[0], [[row.split('|')[3], row.split('|')[4], float(row.split('|')[2])]])
        else:
            return 0
    
    graph_edge_constructor = {key: data for key, data in lines.map(filter_transactions
                            ).filter(lambda x: x != 0).map(lambda x: (x[0], x[2])
                            ).reduceByKey(lambda x,y: x+y).collect()}
    
    filtered_transactions = {key: data for key, data in lines.map(filter_transactions
                            ).filter(lambda x: x != 0).map(lambda x: (x[:2], x[2])
                            ).collect()}
    
    return graph_edge_constructor ,filtered_transactions                           

graph_edge_constructor, filtered_transactions = get_graph_edges(lines, filtered_by_amount_per_node)


def create_graph(edges):
    '''This function returns a multi edged directed graph based on an edge list'''
    G=nx.MultiDiGraph()
    G.add_weighted_edges_from(edges)
    return G


def get_depth(G, node):
    '''This function returns the number of edges of a given node'''
    return max(nx.single_source_shortest_path_length(G, node).values())



def breadth_first_search(G, node):
    '''This function returns all the connected nodes from a given node with bfs order'''
    return list(nx.shortest_path(G, source=node))


def get_tansaction_traffic_per_node(G, time, node, transaction_volume):
    '''This function calculates the traffic parameter of a given node'''
    if (time, node) not in transaction_volume:
            transaction_volume[(time, node)] = (G.in_degree(node)
                                                    + G.out_degree(node))
    else:
        transaction_volume[(time, node)] += (G.in_degree(node)
                                                + G.out_degree(node))
    return transaction_volume


def get_normalized_edge_weights(G, time, connected_nodes, transaction_volume, filtered_by_amount_per_node):
    '''This function returns the ratio of each money transfer normalized by 
    the money initially sent by the sender node'''
    
    normalized_transactions = []
    for node in connected_nodes:
        neighbors = G.neighbors(node)
        transaction_volume = get_tansaction_traffic_per_node(G, time, node, 
                                                             transaction_volume)
        betweenness_dict[(time, node)] = betweenness_nodes[node]
        closeness_dict[(time, node)] = closeness_nodes[node]
        
        for neighbor in neighbors:
            #loop over the possible multiple edges connecting two nodes
            for k in range(len(G.get_edge_data(node, neighbor))):
                edge_amount = G.get_edge_data(node, neighbor)[k]['weight']
                initial_sent_amount = filtered_by_amount_per_node[tuple([time, connected_nodes[0]])]

                normalized_transactions.append([time, node, neighbor, edge_amount,
                                                edge_amount/initial_sent_amount])
    
    return (normalized_transactions, transaction_volume)


all_normalized_transactions, transaction_volume, betweenness_dict, closeness_dict = [], {}, {}, {}
###main function
for date, all_transactions_per_time in zip(graph_edge_constructor.keys(), graph_edge_constructor.values()):
    #Creates different graphs for different days
    G = create_graph([trans for trans in all_transactions_per_time])
    max_subgraph_size, visited_subgraphs = 0, []
    
    for transaction in all_transactions_per_time:
        # The depth of the transaction chain starting from the inital sender
        depth = get_depth(G, transaction[0])
        
        if depth > 1:
            #get connected nodes in bfs order
            connected_nodes = breadth_first_search(G, transaction[0])
            
            if max_subgraph_size  < len(connected_nodes) or not set(connected_nodes) <= set(visited_subgraphs):
                max_subgraph_size = len(connected_nodes)
                visited_subgraphs += connected_nodes
                # I looked at the closeness and betweenness for only 
                # transaction groups with multiple edges.
                # This saved significant amount of computational time 
                # as graph size decreases once the 
                # transactions with single edges are filtered.
                sub_G = G.subgraph(connected_nodes)
                betweenness_nodes = nx.betweenness_centrality(sub_G, normalized=True)
                closeness_nodes = nx.closeness_centrality(sub_G)
                
                # Edge weights are normalized by the money initially sent from the sender.
                # Also in and out degree weights are calculated for each node.
                normalized_transactions, transaction_volume = get_normalized_edge_weights(G, 
                                date, connected_nodes, transaction_volume, filtered_by_amount_per_node)
                
                if normalized_transactions not in all_normalized_transactions:
                    all_normalized_transactions.append(normalized_transactions)


def get_receivers(sender_id, all_day_transactions):
    receivers = []
    for transaction in all_day_transactions:
        if transaction[1] != sender_id:
            receivers.append(transaction[2])
    return receivers


def get_multiple_receivers(receivers):
    seen, multiple_receiver = [], set()
    for r in receivers:
        if r not in seen:
            seen.append(r)
        else:
            multiple_receiver.add(r)
    return multiple_receiver

def fix_multiple_receiver_amount(all_day_transactions, multiple_receivers):
    '''This function and the above two functions are helper functions to fix
    the edge weights pointing to the same receiver from multiple nodes. It
    sums over these separate edge weights to accurately detect the money 
    sent in batches to the final receiver node
    '''
    for r in multiple_receivers:
        r_sum, edge_sum = 0, 0
        
        for transaction in all_day_transactions:
            if transaction[2] == r:
                r_sum += transaction[4]
                edge_sum += transaction[3]
        
        if r_sum >= commission_cut_off and r_sum <= 1.:
            all_day_transactions.append([all_day_transactions[0][0], 'sender', r, edge_sum, r_sum])
    return all_day_transactions


def get_suspicious_transactions(all_normalized_transactions, commission_cut_off):
    '''This function differentiates the 'dirty' transfer chains from the 'clean'
    ones. It returns the suspicious transfer chains based on a user defined
    threshold parameter
    '''
    suspicious_transactions = []
    for all_day_transactions in all_normalized_transactions:
        sender_id = all_day_transactions[0][1]
        receivers = get_receivers(sender_id, all_day_transactions)
        multiple_receivers = get_multiple_receivers(receivers)
        if multiple_receivers:
            all_day_transactions = fix_multiple_receiver_amount(all_day_transactions, 
                                                                multiple_receivers)
                
        for transaction in all_day_transactions:
            if sender_id != transaction[1] and transaction[4] >= commission_cut_off and transaction[4] <= 1.:
                suspicious_transactions.append(all_day_transactions)     
    return suspicious_transactions


#fix multiple edge receiver problem        
suspicious_transactions = get_suspicious_transactions(all_normalized_transactions, commission_cut_off)


def get_suspicious_traffic(transaction_volume):
    '''This function normalizes the traffic parameter by the maximum number
    of inbound and outbound transfers on a given day'''
    max_volume = max(transaction_volume.values())
    suspicious_traffic = {k: v / max_volume for k, v in transaction_volume.items()}
    return suspicious_traffic

#normalize the transaction traffic parameter
suspicious_traffic = get_suspicious_traffic(transaction_volume)


def get_gamma(suspicious_traffic, betweenness_dict, closeness_dict):
    '''This function returns a gamma parameter per node which is a linear combination 
    of three network features
    '''
    gamma_day_node, gamma_dict = {}, {}
    a, b, c = 0.5, 0.4, 0.1
    for key in suspicious_traffic.keys():
        gamma_day_node[key] = a*suspicious_traffic[key] + b*betweenness_dict[key] + c*closeness_dict[key]
    
    for key in gamma_day_node.keys():
        if key[1] not in gamma_dict:
            gamma_dict[key[1]] = gamma_day_node[key]
        else:
            gamma_dict[key[1]] += gamma_day_node[key]
            
    sorted_gamma_nodes = sorted(gamma_dict.items(), key=lambda kv: -kv[1])
    sorted_gamma_nodes = [node[0] for node in sorted_gamma_nodes]
    return sorted_gamma_nodes, gamma_dict

gamma_nodes, gamma_dict = get_gamma(suspicious_traffic, betweenness_dict, closeness_dict)


def sort_suspicious_transactions(suspicious_transactions, gamma):
    '''This function ranks the suspicion of the transaction chains based on the
    gamma parameter of the involving nodes. It returns the transactions from 
    high to low suspicion order
    '''
    suspicious_transactions_parameter = []
    for transaction_chain in suspicious_transactions:
        gamma_sum = 0
        for transaction in transaction_chain:
            if transaction[1] != 'sender':
                gamma_sum += gamma[transaction[1]] + gamma[transaction[2]]
        avg_gamma = gamma_sum/(2*(len(transaction_chain)))
        if (gamma_sum, transaction_chain) not in suspicious_transactions_parameter:
            suspicious_transactions_parameter.append((avg_gamma, transaction_chain))
    
    suspicious_transactions_parameter = sorted(suspicious_transactions_parameter,key=lambda x:(-x[0],x[1]))
    rank_suspicious_transactions = [line[1] for line in suspicious_transactions_parameter]
    suspicious_transactions_by_line = [transaction for sublist in rank_suspicious_transactions for transaction in sublist]
    return suspicious_transactions_by_line

sorted_suspicious_transactions = sort_suspicious_transactions(suspicious_transactions, gamma_dict)

sc.stop()