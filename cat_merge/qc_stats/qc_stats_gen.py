import pandas as pd
import glob
import os
import yaml

path = r'/Users/victoria/Documents/GitHub/cat-merge/data/output'
edge_files = glob.glob(path + "/*edges.tsv")
node_files = glob.glob(path + "/*nodes.tsv")
edge_df = []
node_df = []

for x_files in edge_files:
    temp_edge_df = pd.read_csv(x_files, sep='\t', dtype="string", lineterminator="\n")
    temp_edge_df["source"] = os.path.basename(x_files)
    edge_df.append(temp_edge_df)

for y_files in node_files:
    temp_node_df = pd.read_csv(y_files, sep='\t', dtype="string", lineterminator="\n")
    temp_node_df["source"] = os.path.basename(y_files)
    node_df.append(temp_node_df)

edges = pd.concat(edge_df, axis=0)
edges.rename({'id': 'uuid'}, axis=1, inplace=True)
edges_long = edges.melt(id_vars=["source"], value_vars=['subject', 'object'], value_name="id")
edges_long['prefix'] = edges_long['id'].str.split(':').str[0]
edges_long_no_dupes = edges_long.drop_duplicates(subset=['id', 'source', 'variable', 'prefix'])

nodes = pd.concat(node_df, axis=0)
nodes_long = nodes.filter(['id'])
nodes_long_no_dupes = nodes_long.drop_duplicates(subset=['id'])

diff_df = pd.merge(edges_long_no_dupes, nodes_long_no_dupes, how='outer', on=['id'], indicator='Exist')
diff_df_missing = diff_df[diff_df['Exist'] == 'left_only']

diff_df_missing.to_csv("/Users/victoria/Documents/GitHub/cat-merge/data/qc_stats/merged.csv", index=False)

qc_stats = diff_df_missing.groupby(['source', 'variable', 'prefix']).size().sort_values(ascending=False) \
  .reset_index(name='Missing From Nodes File')

qc_stats.to_csv("/Users/victoria/Documents/GitHub/cat-merge/data/qc_stats/qc_stats.csv", index=False)
dictionary = qc_stats.to_dict('records')

with open('/Users/victoria/Documents/GitHub/cat-merge/data/qc_stats/qc_stats.yaml', 'w') as testfile:
    yaml.dump(dictionary, testfile)

