import sys

filename = sys.argv[1]

nodes = []
with open(filename, "r") as f:
	lines = f.readlines()
	for line in lines:
		line = line.strip().strip("()").split(",")
		node, h, a = map(float, line)
		nodes.append((node, h, a))

nodes.sort()
for node, h, a in nodes:
	print(f"{int(node)}, hub: {h:.10f}, auth: {a:.10f}")
