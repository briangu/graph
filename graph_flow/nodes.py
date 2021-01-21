from aiostream import stream
import asyncio
import itertools
import numpy as np
import time
import aioprocessing

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

    async def __aiter__(self):
        yield await self.aapply()

    async def apost_process(self, res):
        return await self.fn(res) if self.fn else res

    # TODO similar to aaply in stream, use queue
    async def aapply(self):
        tasks = [d.aapply() for d in self.dependencies]
        res = await asyncio.gather(*tasks)
        return await self.apost_process(res)

    async def start(self):
        pass

    async def join(self):
        pass

class StreamNode(Node):

    subscribers = None

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
        if self.subscribers is None:
            self.subscribers = []
            dep_futures = [d.aapply() for d in self.dependencies]
            async for fr in stream.ziplatest(*dep_futures):
                res = await self.apost_process(fr)
                if not res is None:
                    if self.subscribers:
                        for q in self.subscribers:
                            q.put_nowait(res)
                    yield res
        else:
            q = asyncio.Queue()
            self.subscribers.append(q)
            xs = stream.call(q.get)
            ys = stream.cycle(xs)
            async with ys.stream() as streamer:
                async for item in streamer:
                    yield item


class ProcessingStreamNode(StreamNode):

    p = None
    subscriber_queues = None

    async def aprocess(self):
        # dep_futures = [d.aapply() for d in self.dependencies]
        async for fr in stream.ziplatest(*self.dep_futures):
            yield await self.apost_process(fr)

    async def __broadcast__(self):
        async for res in self.aprocess():
            if not res is None:
                print(self.name)
                for q in self.subscriber_queues:
                    await q.coro_put(res)                

    # TODO: we can also check the sq each iteration before broadcast
    def __process__(self):
        asyncio.run(self.__broadcast__())

    # TODO: we can start while building the graph
    async def aapply(self):
        if self.subscriber_queues is None:
            self.dep_futures = [d.aapply() for d in self.dependencies]
            self.subscriber_queues = []
        print("aapply: {} subscribers: {}".format(self.name, len(self.subscriber_queues)))
        q = aioprocessing.AioQueue()
        self.subscriber_queues.append(q)
        xs = stream.call(await q.coro_get())
        ys = stream.cycle(xs)
        async with ys.stream() as streamer:
            async for item in streamer:
                yield item

    async def start(self):
        if self.p is None:
            print("node: {} subscribers: {}".format(self.name, len(self.subscriber_queues)))
            self.p = aioprocessing.AioProcess(target=self.__process__, args=())
            self.p.start()

    async def join(self):
        if self.p:
            await self.p.coro_join()
            self.p = None

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

