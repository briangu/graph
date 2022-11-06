import unittest

import asyncio
import time

from graph_flow.node import *
from graph_flow.util import print_nodes, MinCostNode, CacheNode
from graph_flow.stream_nodes import StreamNode, ProcessingStreamNode


# if not self.fn:
#     await asyncio.sleep(self.process_cost() / 10)
class SlowNode(Node):

    def __init__(self, node):
        name = "s_{}".format(node.name)
        deps = [SlowNode(d) for d in node.dependencies]
        super().__init__(*deps, name = name, cost = node.cost, fn = node.fn)
        self.node = node

    def post_process(self, res):
        time.sleep(self.process_cost() / 10)
        return super().post_process(res)


async def run_graph(root):
    return await root.aapply()

def count_nodes(node):
    return len(node.traverse())

# def get_node_key(n, i):
#     return n.name if isinstance(n, Node) else f"n_{i}"

# def map_nodes(node):
#     return {n.name if isinstance(n,Node) else f"n_{i}": i for i, n in enumerate(node.traverse())}

# def build_adj_matrix(node, node_map, adj_mat = None):
#     if adj_mat is None:
#         n = len(node_map.keys())
#         adj_mat = [[0]*n for _ in range(n+1)]
#     for i,n in enumerate(node.traverse()):
#         m = node_map[n.name]
#         for d in n.dependencies:
#             adj_mat[m][node_map[get_node_key(n, i)]] = d.cost()
#     return adj_mat

def print_header(str):
    print()
    print("="*len(str))
    print(str)
    print("="*len(str))
    print()


def get_test_graph():
    aNode = Node(1, name="a")
    sb1Node = Node(name="sb1", cost = 0)
    sb2bNode = Node(name = "sb2", cost = 2)
    bNode = Node(sb1Node, sb2bNode, name="b", cost = 2)
    cNode = Node(aNode, name="c", cost = 4)
    dNode = Node(cost = 3, name="d")
    eNode = Node(cNode, dNode, name="e", cost = 1)
    fNode = Node(name="f", cost = 5)
    root = Node(eNode, fNode, bNode, name="h", cost = 1)
    return root


def get_min_cost_graph():
    aNode = Node(name="a", fn=lambda _: 1)
    sb1Node = Node(name="sb1", cost = 0)
    sb2bNode = Node(name = "sb2", cost = 2)
    bNode = Node(sb1Node, sb2bNode, name="b", cost = 2)
    cNode = Node(aNode, name="c", cost = 4)
    dNode = Node(cost = 3, name="d")
    eNode = Node(cNode, dNode, name="e", cost = 1)
    fNode = Node(name="f", cost = 5)
    sb1Node = Node(name="sb1", cost = 0)
    sb2bNode = Node(name="sb2", cost = 2)
    bNode = MinCostNode(sb1Node, sb2bNode, name="b", cost = 2)
    root = Node(eNode, fNode, bNode, name="h", cost = 1)
    return root


def get_cache_node_graph():
    aNode = Node(name="a", fn=lambda _: 1)
    sb1Node = Node(name="sb1", cost = 0)
    sb2bNode = Node(name = "sb2", cost = 2)
    bNode = Node(sb1Node, sb2bNode, name="b", cost = 2)
    cNode = Node(aNode, name="c", cost = 4)
    dNode = Node(cost = 3, name="d")
    eNode = Node(cNode, dNode, name="e", cost = 1)
    fNode = Node(name="f", cost = 5)
    sb1Node = Node(name="sb1", cost = 10)
    sb2Node = CacheNode(sb1Node, name="sb2-cache", cost = 0)
    bNode = Node(sb2Node, name="b", cost = 2)
    root = Node(eNode, fNode, bNode, name="h", cost = 1)
    return root


class TestSyncNode(unittest.TestCase):
    def test_node_name(self):
        a = Node(name="a")
        self.assertEqual(a.name, "a")

    def test_eval(self):
        x = Node(1)
        self.assertEqual(x.apply(), 1)
        self.assertEqual(x.apply(), x())

    def test_simple_node(self):
        a = Node(1, name="a")
        self.assertEqual(a.name, "a")
        b = Node(name="b", fn=lambda _: 1)
        self.assertEqual(b.name, "b")
        c = Node(a, name="c")
        self.assertEqual(c.name, "c")
        d = Node(lambda: 1, name="d")
        self.assertEqual(d.name, "d")
        e = Node(1)
        self.assertEqual(e.name, None)
        self.assertEqual(a(), b())
        self.assertEqual(a(), c())
        self.assertEqual(a(), d())
        self.assertEqual(a(), e())

    def test_simple_cost(self):
        a = Node(cost=1)
        self.assertEqual(a.cost(), 1)

        a = Node(9, cost=1)
        self.assertEqual(a.cost(), 1)

        a = Node(lambda: 9, cost=1)
        self.assertEqual(a.cost(), 1)

    def test_complex_cost(self):
        a = Node(cost=1)
        b = Node(cost=2)
        c = Node(cost=3)
        d = Node(a, b, c, cost=4)
        self.assertEqual(d.dependencies_cost(), a.cost() + b.cost() + c.cost())
        self.assertEqual(d.cost(), a.cost() + b.cost() + c.cost() + 4)

    def test_min_choice_cost(self):
        a = Node(cost=1)
        b = Node(cost=2)
        c = Node(cost=3)
        d = MinCostNode(a, b, c, cost=4)
        self.assertEqual(d.cost(), a.cost() + 4)

    def test_sync_node(self):
        print_header("testing sync node graphs:")

        root = get_test_graph()
        # node_map = map_nodes(root)
        # n = len(node_map.keys())

        # print("node count: {}".format(n))
        # print(node_map)
        print_nodes(root)

        # adj_matrix = build_adj_matrix(root, node_map)
        # print(adj_matrix)

        print("total cost: {}".format(root.cost()))

        # [print(d.name) for d in root.traverse()]

        r = root()
        print(f"result: {r}")
        self.assertEqual(r, 1)

    def test_min_cost_node(self):
        print_header("running min graph")

        root = get_min_cost_graph()
        self.assertEqual(root(), 1)

    def test_cache_node(self):
        root = get_cache_node_graph()
        print_header("running with cache node: pass 1")
        print("total cost: {}".format(root.cost()))
        root()
        print_header("running with cache node: pass 2")
        print("total cost: {}".format(root.cost()))
        root()


def test_async_node():
    print_header("testing async node graphs:")

    root = get_test_graph()
    node_map = map_nodes(root)
    n = len(node_map.keys())

    print("node count: {}".format(n))
    print(node_map)
    print_nodes(root)

    adj_matrix = build_adj_matrix(root, node_map)
    print(adj_matrix)

    [print(d.name) for d in root.traverse()]

    print_header("running graph:")

    print(asyncio.run(run_graph(root)))

    print_header("running min graph")

    root = get_min_cost_graph()
    asyncio.run(run_graph(root))

    root = get_cache_node_graph()
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
            super().__init__(name=name, cost = cost)
            self.b = b
            self.e = e

        def apply(self):
            for i in range(self.b,self.e):
                yield i

    class SumNode(StreamNode):
        def __init__(self, *dependencies, name=None, cost = 0):
            super().__init__(*dependencies, name = name, cost = cost, fn = lambda r: sum([x or 0 for x in r]))


    root = RangeNode("a", 1, 10, cost = 1)
    print_header("running range graph:")
    [print(x) for x in root]

    print_header("running sum graph:")

    aNode = RangeNode("a", 1, 10, cost = 1)
    bNode = RangeNode("b", 20, 30, cost = 1)
    root = SumNode(aNode, bNode, name="c", cost = 3)

    print("total cost: {}".format(root.cost()))

    [print(x) for x in root]

    print_header("running double sum chains")

    aNode = RangeNode("a", 0, 10, cost = 1)
    bNode = RangeNode("b", 20, 30, cost = 1)
    cNode = SumNode(aNode, bNode, name="c", cost = 3)

    dNode = RangeNode("d", 100, 110, cost = 1)
    eNode = RangeNode("e", 200, 210, cost = 1)
    fNode = SumNode(dNode, eNode, name="f", cost = 3)

    gNode = SumNode(cNode, fNode, name="g", cost = 10)
    root = StreamNode(gNode, name="h")

    [print(x) for x in root]

    print_header("running tuple chains")

    aNode = RangeNode("a", 0, 10, cost = 1)
    bNode = RangeNode("b", 20, 30, cost = 1)
    cNode = SumNode(aNode, bNode, name="c", cost = 3)

    dNode = RangeNode("d", 100, 110, cost = 1)
    eNode = RangeNode("e", 200, 210, cost = 1)
    fNode = SumNode(dNode, eNode, name="f", cost = 3)

    root = StreamNode(cNode, fNode, name="g")

    [print(x) for x in root]

    print_header("running timer node")

    class TimerNode(StreamNode):
        def __init__(self, name, count, duration, cost = 0):
            super().__init__(name=name, cost = cost)
            self.count = count
            self.duration = duration

        def apply(self):
            for i in range(self.count):
                time.sleep(self.duration / 10)
                yield i

    aNode = TimerNode("a", 10, 1, cost = 1)
    bNode = RangeNode("b", 20, 30, cost = 1)
    root = SumNode(aNode, bNode, name="c", cost = 3)
    [print(x) for x in root]

    print_header("timer trigger sync subgraph")

    def subgraph():
        a = Node(1, name="a")
        b = Node(name="b", fn = lambda r: 2)
        c = Node(a, b, name="c", fn = lambda r: sum(r))
        return c

    aNode = TimerNode("s_a", 10, 1, cost = 1)
    root = StreamNode(aNode, "s_b", fn = lambda r: subgraph()())
    [print(x) for x in root]

    print_header("timer trigger async subgraph")

    async def async_val(a):
        return a

    async def async_fn(a, fn):
        return fn(a)

    def asubgraph():
        a = Node("a", fn = lambda r: async_val(1))
        b = Node("b", fn = lambda r: async_val(2))
        c = Node("c", dependencies=[a,b], fn = lambda r: async_fn(r, lambda x: sum(x)))
        return c

    print("subgraph: {}".format(asyncio.run(run_graph(asubgraph()))))

    aNode = TimerNode("s_a", 10, 1, cost = 1)
    root = StreamNode("s_b", dependencies=[aNode], fn = lambda r: asyncio.run(run_graph(asubgraph())))
    [print("stream: {}".format(x)) for x in root]


def test_processing_stream_nodes():

    class TimerProcessingNode(StreamNode):
        def __init__(self, name, count, duration, cost = 0):
            super().__init__(name, cost = cost)
            self.count = count
            self.duration = duration

        def apply(self):
            for i in range(self.count):
                time.sleep(self.duration / 10)
                yield i


    aNode = TimerProcessingNode("aNode")
    bNode = ProcessingStreamNode("bNode", dependencies=[aNode], fn = print)

    tasks = [asyncio.ensure_future(b.run()) for b in bNode.traverse()]

    asyncio.run(tasks)



if __name__ == '__main__':
    unittest.main()
