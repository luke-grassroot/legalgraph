# louvain.csv: file from output of all_data_query, all nodes and corresponding page rank and louvain community

file = open('louvain.csv')
data = file.read()
file.close()

rows = data.split('\n')[1:]
headings = data.split('\n')[0].split(',')

louvain_list = []
for row in rows:
	row_list = row.split(',')
	dic = {}
	for h in range(len(headings)):
		dic[headings[h]] = row_list[h]
	louvain_list.append(dic)

# map community number to list of nodes
communities = {}
for node in louvain_list:
	com = node['l_community']
	if com in communities:
		communities[com].append(node)
	else:
		communities[com] = [node]

# map community number to number of nodes in community
community_count = {}
for c, nodes in communities.items():
	community_count[c] = len(nodes)

for k, v in community_count.items():
	if not v == 1:
		print("community id: ", k, ", number of nodes: ", v)
print("")

# print(len(louvain_list))
# print(len(community_count))

# takes list of nodes
def check_for_nodes(community):
	types = {'act': [], 'case': [], 'district': [], 'state': []}
	for node in community:
		if not node['act'] == 'null':
			types['act'].append(node)
		elif not node['file_date'] == 'null':
			types['case'].append(node)
		elif not node['district'] == 'null':
			types['district'].append(node)
		elif not node['state'] == 'null':
			types['state'].append(node)

	for t, l in types.items():
		print("number of " + t + "s: ", len(l))

	print("states: ")
	for node in types['state']:
		print("\t" + node['state'])

	print("important acts: ",)
	for node in types['act']:
		if float(node['page_rank_score']) > 4000:
			print("\t" + node['act'])

# for each community, display number of node types and relevant acts and states
for community_id, nodes in communities.items():
	if len(nodes) > 1:
		print("community id: ", community_id)
		check_for_nodes(nodes)
		print("")























