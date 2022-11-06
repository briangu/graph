import unittest

from graph_comp.node import *
from graph_comp.util import MinCostNode, CacheNode


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
        a = Node() % "a"
        self.assertEqual(a.name, "a")

    def test_eval(self):
        x = Node(1)
        self.assertEqual(x.apply(), 1)
        self.assertEqual(x.apply(), x())

    def test_fn(self):
        a = Node(name="a", fn=lambda x: 1)
        self.assertEqual(a(), 1)
        b = a | Node(name="b", fn=lambda x: x[0] * 2)
        self.assertEqual(b(), 2)

    def test_dep_types(self):
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

    def test_functional_composition(self):
        a = Node(1, name="a")
        b = Node(2, name="b")
        c = Node(3, name="c", fn=lambda x: sum(x))(a)
        self.assertEqual(c(), 4)
        c = Node(3, name="c", fn=lambda x: sum(x))(a, b)
        self.assertEqual(c(), 6)

    def test_combine_function(self):
        a = Node(1)
        b = Node(2)
        c = (a + b)
        self.assertEqual(c(), [1,2])
        d = c | (lambda x: sum(x[0]))
        self.assertEqual(d(), 3)

    def test_combine_boxing(self):
        e = Node(3) + 4
        self.assertEqual(sum(e()), 7)

    def test_pipe_function(self):
        x = Node(1, 2) | (lambda x: sum(x()))
        self.assertTrue(isinstance(x, Node))
        self.assertEqual(x(), 3)
        y = Node(1) @ 10 + Node(2) @ 20 + Node(3) @ 30
        self.assertTrue(isinstance(y, Node))
        self.assertEqual(y.dependencies_cost(), 60)
        y = y | (lambda x: sum(x()))
        self.assertEqual(y(), 6)
        z = y | (lambda x: x() ** 2)
        self.assertTrue(isinstance(z, Node))
        self.assertEqual(z(), 36)

    def test_pipe_node(self):
        x = Node(1)
        y = Node(2)
        self.assertFalse(x in y.deps)
        z = x | y
        self.assertEqual(y, z)
        self.assertTrue(x in y.deps)

    def test_pipe_node_multi(self):
        a = Node(1, name="a")
        b = Node(2, name="b")
        c = b | Node(name="c", fn=lambda x: x[0] * 3)
        self.assertEqual(c(), 6)
        d = (a + c) % "a + c" | Node(name="d", fn=lambda x: sum(x[0]))
        r = d()
        self.assertEqual(r, 7)
        e = Node(name="d", fn=lambda x: sum(x))
        e = a | e
        e = c | e
        r = e()
        self.assertEqual(r, 7)

    def test_pipe_function(self):
        x = Node(1, 2) | (lambda x: sum(x[0]))
        self.assertTrue(isinstance(x, Node))
        self.assertEqual(x(), 3)
        y = Node(1) @ 10 + Node(2) @ 20 + Node(3) @ 30
        self.assertTrue(isinstance(y, Node))
        self.assertEqual(y.dependencies_cost(), 60)
        y = y | (lambda x: sum(x[0]))
        self.assertEqual(y(), 6)
        z = y | (lambda x: x[0] ** 2)
        self.assertEqual(z(), 36)

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

    def test_graph_cost(self):
        root = get_test_graph()
        self.assertEqual(root.cost(), sum([x.process_cost() for x in root.traverse()]))
        r = root()
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

    def test_graph(self):
        a = Node(name='inputs', cost=1)
        b = Node(name="b", cost=3, fn=lambda x: sum(x[0]) if type(x[0]) == list else sum(x))(a)
        g = Graph(inputs=a, outputs=b, name="g")
        self.assertEqual(g.cost(), 4)
        g.summary()
        self.assertEqual(g(1), 1)
        self.assertEqual(g(1,2,3), 6)

if __name__ == '__main__':
    unittest.main()
