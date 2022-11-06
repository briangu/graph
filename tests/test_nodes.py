import unittest

import time

from graph_flow.node import *
from graph_flow.util import MinCostNode, CacheNode


# if not self.fn:
#     await asyncio.sleep(self.process_cost() / 10)
class SlowNode(Node):

    def __init__(self, node):
        name = "s_{}".format(node.name)
        deps = [SlowNode(d) for d in node.dependencies]
        super().__init__(*deps, name = name, cost = node.cost, fn = node.fn)
        self.node = node

    def post_process(self, res):
        time.sleep(self.fn_cost / 10)
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
    def test_name(self):
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

    def test_chain(self):
        a = Node(1)
        b = Node(2)
        c = (a + b)
        self.assertEqual(c(), [1,2])
        d = c | (lambda x: sum(x()))
        self.assertEqual(d(), 3)

    def test_pipe(self):
        x = (Node(1) + Node(2)) | (lambda x: sum(x()))
        self.assertTrue(isinstance(x, Node))
        self.assertEqual(x(), 3)
        y = (Node(1, cost=10) + Node(2, cost=20) + Node(3, cost=30))
        self.assertTrue(isinstance(y, Node))
        self.assertEqual(y.dependencies_cost(), 60)
        y = y | (lambda x: sum(x()))
        self.assertEqual(y(), 6)
        z = y | (lambda x: x() ** 2)
        self.assertEqual(z(), 36)
        z.fn_cost = 3
        self.assertEqual(z.cost(), 3)


    def test_simple_cost(self):
        a = Node(cost=1)
        self.assertEqual(a.cost(), 1)

        a = Node(9, cost=1)
        self.assertEqual(a.cost(), 1)

        a = Node(lambda: 9, cost=1)
        self.assertEqual(a.cost(), 1)

        a = Node(lambda: 9) @ 1
        self.assertEqual(a.cost(), 1)


    def test_complex_cost(self):
        a = Node() @ 1
        b = Node() @ 2
        c = Node() @ 3
        d = (a + b + c) @ 4
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
        # print_nodes(root)
        root.pretty_print()

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


if __name__ == '__main__':
    unittest.main()
