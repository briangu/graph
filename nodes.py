import asyncio
import numpy as np

class Node():
    def __init__(self, name, cost = 0, dependencies = []):
        self.name = name
        self.dependencies = dependencies
        self.node_cost = cost

    def __iter__(self):
        def recursive_iter(node):
            yield node
            for d in node.dependencies:
                yield from recursive_iter(d)
        yield from recursive_iter(self)

    def dependency_tasks(self):
        return [d.apply() for d in self.dependencies]

    def dependencies_cost(self):
        return sum([d.cost() for d in self.dependencies])

    def process_cost(self):
        return self.node_cost

    def cost(self):
        return self.process_cost() + self.dependencies_cost()

    async def post_process(self, res):
        print("post process: {} {}".format(self.name, res))
        await asyncio.sleep(self.process_cost())
        return self.name

    async def apply(self):
        tasks = self.dependency_tasks() 
        res = await asyncio.gather(*tasks)
        return await self.post_process(res)

class MinCostNode(Node):
    def min_dependency(self):
        if not self.dependencies:
            return 0, None
        return min((n.cost(), i) for (i, n) in enumerate(self.dependencies))

    def dependency_tasks(self):
        v, i = self.min_dependency()
        min_dep = self.dependencies[i]
        return [min_dep.apply()]

    def dependencies_cost(self):
        v, i = self.min_dependency()
        return v

def print_nodes(n, indent = "", siblingCount = 0):
    if siblingCount > 1:
        child_indent = indent + "|"
    else:
        child_indent = indent + " "
    print("{}{} [{}:{}+{}]".format(indent, n.name, n.cost(), n.process_cost(), n.dependencies_cost()))
    [print_nodes(d, child_indent, len(n.dependencies)) for d in n.dependencies]

def count_nodes(node):
    return len(node)

def map_nodes(node):
    return {n.name: i for i, n in enumerate(node)}

def build_adj_matrix(node, node_map, adj_mat = None):
    if adj_mat is None:
        n = len(node_map.keys())
        adj_mat = np.zeros((n, n))
    for n in node:
        m = node_map[n.name]
        for d in n.dependencies:
            adj_mat[m,node_map[d.name]] = d.cost()
    return adj_mat

aNode = Node("a", 1)
sb1Node = Node("sb1", cost = 0)
sb2bNode = Node("sb2", cost = 2)
bNode = Node("b", cost = 2, dependencies = [sb1Node, sb2bNode])
cNode = Node("c", cost = 4, dependencies = [aNode])
dNode = Node("d", cost = 3)
eNode = Node("e", cost = 1, dependencies = [cNode, dNode])
fNode = Node("f", cost = 5)
root = Node("h", cost = 1, dependencies = [eNode, fNode, bNode])

node_map = map_nodes(root)
n = len(node_map.keys())

print("node count: {}".format(n))
print(node_map)
print_nodes(root)

adj_matrix = build_adj_matrix(root, node_map)
print(adj_matrix)

print("total cost: {}".format(root.cost()))

[print(d.name) for d in root]

print("running graph:")

async def foo(node):
    await node.apply()
    print("{}: {}".format(node.name, node.process_cost()))

async def run_graph(root):
    await root.apply()

asyncio.run(run_graph(root))

print("running min graph")

sb1Node = Node("sb1", cost = 0)
sb2bNode = Node("sb2", cost = 2)
bNode = MinCostNode("b", cost = 2, dependencies = [sb1Node, sb2bNode])
root = Node("h", cost = 1, dependencies = [eNode, fNode, bNode])

asyncio.run(run_graph(root))
