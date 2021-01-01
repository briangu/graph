import asyncio
import itertools
import numpy as np
import time

#todo:
# add either a generic way to pass types in or dict or just use typed sub-classes
# time parameters for graph operations can be at the node level,
# essentially just build the graph you need with time params appropriately set for each subgraph

# identifying reused nodes and topologically sorting according to base nodes -or failing

# 3 kinds of nodes
# synchronous
# async
# streaming

class Node():
    def __init__(self, name, cost = 0, dependencies = [], fn = None):
        self.name = name
        self.dependencies = dependencies
        self.node_cost = cost
        self.fn = fn

    def __iter__(self):
        yield self.apply()

    def traverse(self):
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
        return self.fn(res) if self.fn else res

    def apply(self):
        return self.post_process(self.dependency_tasks())

class TraceNode(Node):

    def __init__(self, node):
        name = "t_{}".format(node.name)
        deps = [TraceNode(d) for d in node.dependencies]
        super().__init__(name, cost = node.cost, dependencies=deps, fn = node.fn)
        self.node = node

    def post_process(self, res):
        print("post process: {} {}".format(self.name, res))
        pp_res = super().post_process(res)
        return pp_res if pp_res else self.name

class AsyncNode(Node):
    def __init__(self, name, cost = 0, dependencies = [], fn = None):
        super().__init__(name, cost=cost, dependencies=dependencies, fn=fn)

    async def __iter__(self):
        yield await self.apply()

    async def post_process(self, res):
        return await self.fn(res) if self.fn else res

    async def apply(self):
        tasks = self.dependency_tasks() 
        res = await asyncio.gather(*tasks)
        return await self.post_process(res)


class StreamNode(Node):
    def __init__(self, name, cost = 0, dependencies = [], fn = None):
        super().__init__(name, cost=cost, dependencies=dependencies, fn=fn)

    def __iter__(self):
        yield from self.apply()

    def post_process(self, res):
        return self.fn(res) if self.fn else res

    def apply(self):
        for q in itertools.zip_longest(*self.dependency_tasks()):
            yield self.post_process(q)

class StreamAsyncNode(Node):
    def __init__(self, name, cost = 0, dependencies = [], fn = None):
        super().__init__(name, cost=cost, dependencies=dependencies, fn=fn)

    async def __aiter__(self):
        async for x in self.apply():
            yield x

    async def post_process(self, res):
        return self.fn(res) if self.fn else res

    async def apply(self):
        async for q in itertools.zip_longest(*self.dependency_tasks()):
            yield await self.post_process(q)

# class StreamAsyncNode(AsyncNode):
#     def __init__(self, name, cost = 0, dependencies = [], fn = None):
#         super().__init__(name, cost=cost, dependencies=dependencies, fn=fn)

#     async def __iter__(self):
#         yield from await self.apply()

#     async def post_process(self, res):
#         return await self.fn(res) if self.fn else res

#     async def apply(self):
#         for q in itertools.zip_longest(*self.dependency_tasks()):
#             yield await self.post_process(q)


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

