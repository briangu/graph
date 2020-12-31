import asyncio
import itertools
import numpy as np
import time
from graph_flow.nodes import *

def print_nodes(node, indent = "", siblingCount = 0):
    if siblingCount > 1:
        child_indent = indent + "|"
    else:
        child_indent = indent + " "
    print("{}{} [{}:{}+{}]".format(indent, node.name, node.cost(), node.process_cost(), node.dependencies_cost()))
    [print_nodes(d, child_indent, len(node.dependencies)) for d in node.dependencies]

def count_nodes(node):
    return len(node.traverse())

def map_nodes(node):
    return {n.name: i for i, n in enumerate(node.traverse())}

def build_adj_matrix(node, node_map, adj_mat = None):
    if adj_mat is None:
        n = len(node_map.keys())
        adj_mat = np.zeros((n, n))
    for n in node.traverse():
        m = node_map[n.name]
        for d in n.dependencies:
            adj_mat[m,node_map[d.name]] = d.cost()
    return adj_mat

def print_header(str):
    print()
    print("="*len(str))
    print(str)
    print("="*len(str))
    print()


def test_sync_node():
    print_header("testing sync node graphs:")

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

    [print(d.name) for d in root.traverse()]

    print_header("running graph:")

    print(root.apply())

    print_header("running min graph")

    sb1Node = Node("sb1", cost = 0)
    sb2bNode = Node("sb2", cost = 2)
    bNode = MinCostNode("b", cost = 2, dependencies = [sb1Node, sb2bNode])
    root = Node("h", cost = 1, dependencies = [eNode, fNode, bNode])

    root.apply()

    sb1Node = Node("sb1", cost = 10)
    sb2Node = CacheNode("sb2-cache", sb1Node, cost = 0)
    bNode = Node("b", cost = 2, dependencies = [sb2Node])
    root = Node("h", cost = 1, dependencies = [eNode, fNode, bNode])

    print_header("running with cache node: pass 1")
    print("total cost: {}".format(root.cost()))
    root.apply()
    print_header("running with cache node: pass 2")
    print("total cost: {}".format(root.cost()))
    root.apply()


def test_async_node():
    print_header("testing async node graphs:")

    aNode = AsyncNode("a", 1)
    sb1Node = AsyncNode("sb1", cost = 0)
    sb2bNode = AsyncNode("sb2", cost = 2)
    bNode = AsyncNode("b", cost = 2, dependencies = [sb1Node, sb2bNode])
    cNode = AsyncNode("c", cost = 4, dependencies = [aNode])
    dNode = AsyncNode("d", cost = 3)
    eNode = AsyncNode("e", cost = 1, dependencies = [cNode, dNode])
    fNode = AsyncNode("f", cost = 5)
    root = AsyncNode("h", cost = 1, dependencies = [eNode, fNode, bNode])

    node_map = map_nodes(root)
    n = len(node_map.keys())

    print("node count: {}".format(n))
    print(node_map)
    print_nodes(root)

    adj_matrix = build_adj_matrix(root, node_map)

    [print(d.name) for d in root.traverse()]

    print_header("running graph:")

    async def run_graph(root):
        await root.apply()

    print(asyncio.run(run_graph(root)))

    print_header("running min graph")

    sb1Node = AsyncNode("sb1", cost = 0)
    sb2bNode = AsyncNode("sb2", cost = 2)
    bNode = MinCostAsyncNode("b", cost = 2, dependencies = [sb1Node, sb2bNode])
    root = AsyncNode("h", cost = 1, dependencies = [eNode, fNode, bNode])

    asyncio.run(run_graph(root))

    sb1Node = AsyncNode("sb1", cost = 10)
    sb2Node = CacheAsyncNode("sb2-cache", sb1Node, cost = 0)
    bNode = AsyncNode("b", cost = 2, dependencies = [sb2Node])
    root = AsyncNode("h", cost = 1, dependencies = [eNode, fNode, bNode])

    print_header("running with cache node: pass 1")
    print("total cost: {}".format(root.cost()))
    asyncio.run(run_graph(root))
    print_header("running with cache node: pass 2")
    print("total cost: {}".format(root.cost()))
    asyncio.run(run_graph(root))

def test_stream_node():
    print_header("testing async node graphs:")

    class RangeNode(StreamNode):
        def __init__(self, name, b, e, cost = 0):
            super().__init__(name, cost = cost)   
            self.b = b
            self.e = e

        def apply(self):
            for i in range(self.b,self.e):
                yield i

    class SumNode(StreamNode):
        def __init__(self, name, dependencies, cost = 0):
            super().__init__(name, dependencies = dependencies, cost = cost, fn = lambda r: sum([x or 0 for x in r]))   


    root = RangeNode("a", 1, 10, cost = 1)
    print_header("running range graph:")
    [print(x) for x in root]

    print_header("running sum graph:")

    aNode = RangeNode("a", 1, 10, cost = 1)
    bNode = RangeNode("b", 20, 30, cost = 1)
    root = SumNode("c", [aNode, bNode], cost = 3)

    print("total cost: {}".format(root.cost()))

    [print(x) for x in root]

    print_header("running double sum chains")

    aNode = RangeNode("a", 0, 10, cost = 1)
    bNode = RangeNode("b", 20, 30, cost = 1)
    cNode = SumNode("c", [aNode, bNode], cost = 3)

    dNode = RangeNode("d", 100, 110, cost = 1)
    eNode = RangeNode("e", 200, 210, cost = 1)
    fNode = SumNode("f", [dNode, eNode], cost = 3)

    gNode = SumNode("g", [cNode, fNode], cost = 10)
    root = StreamNode("h", dependencies=[gNode])

    [print(x) for x in root]

    print_header("running tuple chains")

    aNode = RangeNode("a", 0, 10, cost = 1)
    bNode = RangeNode("b", 20, 30, cost = 1)
    cNode = SumNode("c", [aNode, bNode], cost = 3)

    dNode = RangeNode("d", 100, 110, cost = 1)
    eNode = RangeNode("e", 200, 210, cost = 1)
    fNode = SumNode("f", [dNode, eNode], cost = 3)

    root = StreamNode("g", dependencies=[cNode, fNode])

    [print(x) for x in root]

    print_header("running timer node")

    class TimerNode(StreamNode):
        def __init__(self, name, count, duration, cost = 0):
            super().__init__(name, cost = cost)   
            self.count = count
            self.duration = duration

        def apply(self):
            for i in range(self.count):
                time.sleep(self.duration / 10)
                yield i

    aNode = TimerNode("a", 10, 1, cost = 1)
    bNode = RangeNode("b", 20, 30, cost = 1)
    root = SumNode("c", [aNode, bNode], cost = 3)
    [print(x) for x in root]

    print_header("timer trigger node")

    def subgraph():
        a = Node("a", fn = lambda r: 1)
        b = Node("b", fn = lambda r: 2)
        c = Node("c", dependencies=[a,b], fn = lambda r: sum(r))
        return c

    aNode = TimerNode("s_a", 10, 1, cost = 1)
    root = StreamNode("s_b", dependencies=[aNode], fn = lambda r: subgraph().apply())
    [print(x) for x in root]

