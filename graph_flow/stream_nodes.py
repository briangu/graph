import asyncio
import itertools

import aioprocessing
from aiostream import stream

from .node import Node


class StreamNode(Node):

    subscribers = None

    def __init__(self, name, *dependencies, cost = None, fn = None):
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

    async def start(self):
        pass

    async def join(self):
        pass



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
