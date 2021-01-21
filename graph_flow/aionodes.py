from aiostream import stream
import asyncio
import itertools
import numpy as np
import time

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

    async def __aiter__(self):
        yield await self.aapply()

    async def apost_process(self, res):
        return await self.fn(res) if self.fn else res

    # TODO similar to aaply in stream, use queue
    async def aapply(self):
        tasks = [d.aapply() for d in self.dependencies]
        res = await asyncio.gather(*tasks)
        return await self.apost_process(res)

class StreamNode(Node):

    queues = None

    def __init__(self, name, cost = 0, dependencies = [], fn = None):
        super().__init__(name, cost=cost, dependencies=dependencies, fn=fn)

    def __iter__(self):
        yield from self.apply()

    def post_process(self, res):
        return self.fn(res) if self.fn else res

    def apply(self):
        for q in itertools.zip_longest(*self.dependency_tasks()):
            yield self.post_process(q)

    async def __aiter__(self):
        async for x in self.aapply():
            yield x

    #TODO: package results with node name dictionary
    async def apost_process(self, res):
        def unwrap(x, l=None):
#            print("post_process: {} {} {} {}".format(self.name, type(x), len(x) if hasattr(x, '__len__') else 0, x))
            if type(x) is tuple: # and len(x) == 1:
                return unwrap(x[0], x)
            return l
        _res = unwrap(res) if len(self.dependencies) == 1 else res
        # print("post_process: {} {} {} {}".format(self.name, type(_res), _res, res))

        return self.fn(_res) if self.fn else _res

    async def aapply(self):
        if self.queues is None:
            self.queues = []
            dep_futures = [d.aapply() for d in self.dependencies]
            async for fr in stream.ziplatest(*dep_futures):
                res = await self.apost_process(fr)
                if not res is None:
                    if self.queues:
                        for q in self.queues:
                            q.put_nowait(res)
                    yield res
        else:
            q = asyncio.Queue()
            self.queues.append(q)
            xs = stream.call(q.get)
            ys = stream.cycle(xs)
            async with ys.stream() as streamer:
                async for item in streamer:
                    yield item


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

class MinCostNode(Node):
    def min_dependency(self):
        if not self.dependencies:
            return 0, None
        return min((n.cost(), i) for (i, n) in enumerate(self.dependencies))

    def dependency_tasks(self):
        _, i = self.min_dependency()
        min_dep = self.dependencies[i]
        return [min_dep.apply()]

    def dependencies_cost(self):
        v, _ = self.min_dependency()
        return v

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

    async def aset_cache(self, val):
        print("set cache: {} {}".format(self.name, val))
        self.internal_cache = val
        return val

    async def aget_cache(self):
        val = self.internal_cache
        print("get cache: {} {}".format(self.name, val))
        return val

    async def aapply(self):
        return await self.aget_cache() if self.internal_cache else await self.aset_cache(await super().aapply())

