import gc
import pandas as pd

from py2neo import Graph
from py2neo.bulk import merge_nodes, create_nodes, create_relationships

from math import ceil

default_uri = "bolt://localhost:7687"
default_user = "neo4j"
default_pword = "AIfD"

def connect_to_graph(uri=default_uri, user=default_user, password=default_pword):
  return Graph(uri, user=user, password=password)

def count_nodes(graph, node_type):
  return graph.run(f"match (n:{node_type}) return count(n) as count").evaluate()

def count_rels(graph, relationship):
  return graph.run(f"match ()-[r:{relationship}]->() return count(r) as count").evaluate()

def count_total_rows(csv_path):
    with open(csv_path) as csvfile:
        total_row_count = sum(1 for _ in csvfile)
        return total_row_count

def load_csv(csv_path, start_row=0, number_rows=1e6, datetime_keys=[]):
  cdf = pd.read_csv(csv_path, nrows=number_rows, skiprows=range(1, int(start_row)))
  for dt_key in datetime_keys:
    cdf[dt_key] = pd.to_datetime(cdf[dt_key], errors='coerce')
    # need this for graph imports too (is in some cases the same as in the DF, but this enforces useful consistency)
    str_key = dt_key + "_str"
    cdf[str_key] = cdf[dt_key].apply(lambda dt: "" if pd.isnull(dt) else dt.strftime("%Y-%m-%d"))
  return cdf

def add_nodes_df_to_graph(graph, df, label, merge_key, graph_keys, df_keys, batch_size=50000, delete_first=False, index_to_add=None, use_merge=True):
    gc.collect()
    if delete_first:
      graph.run(f"match (n: {label}) detach delete n")
    for batch in range(0, ceil(len(df)/batch_size)):
      print("Loading nodes, batch: ", batch)
      start_row=batch * batch_size
      end_row = (batch + 1) * batch_size
      dfslice = df[start_row:end_row].iterrows()
      data = [[row[df_key] for df_key in df_keys] for _, row in dfslice]
      if use_merge:
          merge_nodes(graph.auto(), data, merge_key=merge_key, labels={label}, keys=graph_keys)
      else:
          create_nodes(graph.auto(), data, labels={label}, keys=graph_keys)
        
    # eg index_name = idx_case_id, index_key = case_id    
    if index_to_add:
      graph.run(f"CREATE CONSTRAINT {index_to_add['index_name']} IF NOT EXISTS ON (n:{label}) ASSERT n.{index_to_add['index_key']} IS UNIQUE")
        

def add_csv_to_graph(graph, csv_file, label, merge_key, graph_keys, df_keys, datetime_keys=[], index_to_add=None, max_iter=None, use_merge=True, assume_all_merge=False):
  number_nodes = 0 if assume_all_merge else count_nodes(graph, label)
  total_nodes = count_total_rows(csv_file) - 1
  print("Total nodes: ", total_nodes, ", Total graph size: ", count_nodes(graph, label))
  it = 0
  while number_nodes < total_nodes and (max_iter is None or it < max_iter):
    start_row = number_nodes + 1
    print("Loading cases from: ", start_row)
    df = load_csv(csv_file, start_row=start_row, number_rows=1e6, datetime_keys=datetime_keys)
    add_nodes_df_to_graph(graph, df, label, merge_key, graph_keys, df_keys, batch_size=50000, index_to_add=index_to_add if number_nodes==0 else None, use_merge=use_merge)
    if assume_all_merge:
      number_nodes += len(df)
    else:
      number_nodes = count_nodes(graph, label)
    it += 1
    print("Completed loading DF segment, number now: ", number_nodes)

# Note: use merge seems to return a lot of false positives on supposed duplicates, so defaulting to false for now
# ensure to do a thorough search to make sure no false duplicates later
def add_batch_relationships(graph, join_df, graph_keys, df_keys, relationship_type, prop_dict=None, df_start=0, minor_batch_size=int(1e3), major_batch_size=int(1e6), verbose=True):
    number_in_graph = count_rels(graph, relationship_type)
    start_index = int(df_start)
    end_index = max(len(join_df), int(start_index + major_batch_size))
    if verbose:
      print(f"Adding relationships of type {relationship_type} from {start_index} to {end_index}")
    
    middle_element = prop_dict if prop_dict else { "type": relationship_type }
    data = [(row[df_keys[0]], middle_element, row[df_keys[1]]) for _, row in join_df[start_index:end_index].iterrows()]
    for i in range(ceil(len(data) / minor_batch_size)):
        if verbose and i % 10 == 0:
            print(".", end="")
            
        create_relationships(graph.auto(), 
                             data[i * minor_batch_size:(i + 1) * minor_batch_size], 
                             relationship_type, 
                             start_node_key=graph_keys[0], 
                             end_node_key=graph_keys[1])
    
    number_in_graph = count_rels(graph, relationship_type)
    return number_in_graph, end_index


# graph_keys = [("Case", "case_id"), ("Act", "act_id")] // df_keys = ["ddl_case_id", "act"]
def add_relationships(graph, join_csv_file, relationship_type, target_id_series, target_id_key, graph_keys, df_keys, prop_dict=None, df_read_start=0, max_df_loads=None, rows_per_df=5e6, verbose=True):
  total_relationships = count_total_rows(join_csv_file)
  number_in_graph = count_rels(graph, relationship_type)

  df_it = 0

  while number_in_graph < total_relationships and df_read_start < total_relationships and (max_df_loads is None or df_it < max_df_loads):
    raw_join_df = pd.read_csv(join_csv_file, nrows=rows_per_df, skiprows=range(1, int(df_read_start)))
    adf = raw_join_df[raw_join_df[target_id_key].isin(target_id_series)]
    if verbose:
      print('Initiating outer loop, loading dataframe, from row: ', df_read_start, "length of this relationship frame: ", len(adf))
    
    end_index = 0
    while end_index < len(adf):      
        number_in_graph, end_index = add_batch_relationships(graph, adf, graph_keys, df_keys, relationship_type, prop_dict, df_start=end_index)
    
    print('Completed loading this DF')
    number_in_graph = count_rels(graph, relationship_type)
    df_read_start += rows_per_df # to avoid the duplicates (given adf length < df read length)
    df_it += 1
