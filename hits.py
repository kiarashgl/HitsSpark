import networkx as nx
import sys

filename = sys.argv[1]
G = nx.DiGraph()

graph = {}

print(filename)
with open(filename, "r") as f:
    lines = f.readlines()
    for line in lines:
        line = line.strip().split()
        edges = [int(i) for i in line]
        if len(edges) > 1:
            u = edges[0]
        for v in edges[1:]:
                if u not in graph:
                    graph[u] = {"in": [], "out": []}
                if v not in graph:
                    graph[v] = {"in": [], "out": []}
                graph[u]["out"].append(v)
                graph[v]["in"].append(u)
                G.add_edge(u, v)

authority = {i: 1 for i in G}
hub = {i: 1 for i in G}

iteration = 20
for _ in range(iteration):
    for node in G:
        sm = 0
        for adj in graph[node]["in"]:
            sm += hub[adj]
        authority[node] = sm

    norm = 0
    for i in authority:
        norm += authority[i] * authority[i]
    norm = norm ** 0.5
    for i in authority:
        authority[i] /= norm

    for node in G:
        sm = 0
        for adj in graph[node]["out"]:
            sm += authority[adj]
        hub[node] = sm

    norm = 0
    for i in hub:
        norm += hub[i] * hub[i]
    norm = norm ** 0.5
    for i in hub:
        hub[i] /= norm


for i in sorted(G.nodes):
    print(f"{i}, hub: {hub[i]:.10f}, auth: {authority[i]:.10f}")
