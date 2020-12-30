import asyncio
import itertools
import numpy as np
import time

#todo:
# add either a generic way to pass types in or dict or just use typed sub-classes
# time parameters for graph operations can be at the node level,
# essentially just build the graph you need with time params appropriately set for each subgraph

# identifying reused nodes and topologically sorting according to base nodes -or failing
# streaming

# 3 kinds of nodes
# synchronous
# async
# streaming

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

    def graph():
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

    def post_process(self, res):
        print("post process: {} {}".format(self.name, res))
        time.sleep(self.process_cost() / 10)
        return self.name

    def apply(self):
        task_results = self.dependency_tasks() 
        return self.post_process(task_results)


class AsyncNode(Node):
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

    def graph():
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
        await asyncio.sleep(self.process_cost() / 10)
        return self.name

    async def apply(self):
        tasks = self.dependency_tasks() 
        res = await asyncio.gather(*tasks)
        return await self.post_process(res)


class StreamNode(Node):
    def __init__(self, name, cost = 0, dependencies = []):
        self.name = name
        self.dependencies = dependencies
        self.node_cost = cost

    def __iter__(self):
        yield from self.apply()

    def dependency_tasks(self):
        return [d.apply() for d in self.dependencies]

    def post_process(self, res):
        print("post process: {}".format(self.name))
        time.sleep(self.process_cost() / 10)
        return res

    def apply(self):
        for q in itertools.zip_longest(*self.dependency_tasks()):
            res = self.post_process(q)
            # if hasattr(res, "__len__") and len(res) > 0:
            #     yield from res
            # else:
            #     yield res
            yield res


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

class MinCostAsyncNode(MinCostNode, AsyncNode):
    pass

class CacheNode(Node):

    internal_cache = None

    def __init__(self, name, dependency, cost = 0):
        super().__init__(name, dependencies=[dependency], cost = cost)   

    def cost(self):
        return self.process_cost() + self.dependencies_cost() if not self.internal_cache else 0

    def set_cache(self, val):
        print("set cache: {} {}".format(self.name, val))
        self.internal_cache = val
        return val

    def get_cache(self):
        val = self.internal_cache
        print("get cache: {} {}".format(self.name, val))
        return val

    def apply(self):
        return self.get_cache() if self.internal_cache else self.set_cache(super().apply())

class CacheAsyncNode(AsyncNode):

    internal_cache = None

    def __init__(self, name, dependency, cost = 0):
        super().__init__(name, dependencies=[dependency], cost = cost)   

    def cost(self):
        return self.process_cost() + self.dependencies_cost() if not self.internal_cache else 0

    async def set_cache(self, val):
        print("set cache: {} {}".format(self.name, val))
        self.internal_cache = val
        return val

    async def get_cache(self):
        val = self.internal_cache
        print("get cache: {} {}".format(self.name, val))
        return val

    async def apply(self):
        return await self.get_cache() if self.internal_cache else await self.set_cache(await super().apply())


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

def test_sync_node():
    print("-"*20)
    print("testing sync node graphs:")
    print("-"*20)

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

    root.apply()

    print("running min graph")

    sb1Node = Node("sb1", cost = 0)
    sb2bNode = Node("sb2", cost = 2)
    bNode = MinCostNode("b", cost = 2, dependencies = [sb1Node, sb2bNode])
    root = Node("h", cost = 1, dependencies = [eNode, fNode, bNode])

    root.apply()

    sb1Node = Node("sb1", cost = 10)
    sb2Node = CacheNode("sb2-cache", sb1Node, cost = 0)
    bNode = Node("b", cost = 2, dependencies = [sb2Node])
    root = Node("h", cost = 1, dependencies = [eNode, fNode, bNode])

    print("running with cache node: pass 1")
    print("total cost: {}".format(root.cost()))
    root.apply()
    print("running with cache node: pass 2")
    print("total cost: {}".format(root.cost()))
    root.apply()


def test_async_node():
    print("-"*20)
    print("testing async node graphs:")
    print("-"*20)

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

    [print(d.name) for d in root]

    print("running graph:")

    # async def foo(node):
    #     await node.apply()
    #     print("{}: {}".format(node.name, node.process_cost()))

    async def run_graph(root):
        await root.apply()

    asyncio.run(run_graph(root))

    print("running min graph")

    sb1Node = AsyncNode("sb1", cost = 0)
    sb2bNode = AsyncNode("sb2", cost = 2)
    bNode = MinCostAsyncNode("b", cost = 2, dependencies = [sb1Node, sb2bNode])
    root = AsyncNode("h", cost = 1, dependencies = [eNode, fNode, bNode])

    asyncio.run(run_graph(root))

    sb1Node = AsyncNode("sb1", cost = 10)
    sb2Node = CacheAsyncNode("sb2-cache", sb1Node, cost = 0)
    bNode = AsyncNode("b", cost = 2, dependencies = [sb2Node])
    root = AsyncNode("h", cost = 1, dependencies = [eNode, fNode, bNode])

    print("running with cache node: pass 1")
    print("total cost: {}".format(root.cost()))
    asyncio.run(run_graph(root))
    print("running with cache node: pass 2")
    print("total cost: {}".format(root.cost()))
    asyncio.run(run_graph(root))

def test_stream_node():
    print("-"*20)
    print("testing async node graphs:")
    print("-"*20)

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
            super().__init__(name, dependencies = dependencies, cost = cost)   

        def post_process(self, r):
            val = sum([x or 0 for x in r])
            print("adding: {} = {}".format(r, val))
            return val

    root = RangeNode("a", 1, 10, cost = 1)
    print("running range graph:")
    [print(x) for x in root]

    print("running sum graph:")

    aNode = RangeNode("a", 1, 10, cost = 1)
    bNode = RangeNode("b", 20, 30, cost = 1)
    root = SumNode("c", [aNode, bNode], cost = 3)

    print("total cost: {}".format(root.cost()))

    [print(x) for x in root]

    print("running double sum chains")

    aNode = RangeNode("a", 0, 10, cost = 1)
    bNode = RangeNode("b", 20, 30, cost = 1)
    cNode = SumNode("c", [aNode, bNode], cost = 3)

    dNode = RangeNode("d", 100, 110, cost = 1)
    eNode = RangeNode("e", 200, 210, cost = 1)
    fNode = SumNode("f", [dNode, eNode], cost = 3)

    gNode = SumNode("g", [cNode, fNode], cost = 10)
    root = StreamNode("h", dependencies=[gNode])

    [print(x) for x in root]

    print("running tuple chains")

    aNode = RangeNode("a", 0, 10, cost = 1)
    bNode = RangeNode("b", 20, 30, cost = 1)
    cNode = SumNode("c", [aNode, bNode], cost = 3)

    dNode = RangeNode("d", 100, 110, cost = 1)
    eNode = RangeNode("e", 200, 210, cost = 1)
    fNode = SumNode("f", [dNode, eNode], cost = 3)

    root = StreamNode("g", dependencies=[cNode, fNode])

    [print(x) for x in root]


#test_sync_node()
#test_async_node()
test_stream_node()