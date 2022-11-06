import asyncio
import types


def flatten(arr):
    arr = [x for x in arr if x]
    return arr[0] if len(arr) == 1 else arr


class Node():
    def __init__(self, *dependencies, name = None, cost = None, fn = None):
        self.dependencies = dependencies
        self.name = name
        self.processing_cost = cost or 0
        self.fn = fn or flatten

    def __iter__(self):
        yield self.apply()

    def traverse(self):
        def recursive_iter(node):
            yield node
            for d in node.dependencies:
                yield from recursive_iter(d)
        yield from recursive_iter(self)

    def dependency_tasks(self):
        return [d() if isinstance(d, Node) or type(d) == types.FunctionType else d for d in self.dependencies]

    def dependencies_cost(self):
        return sum([d.cost() if isinstance(d, Node) else 0 for d in self.dependencies])

    def process_cost(self):
        return self.processing_cost() if type(self.processing_cost) == types.FunctionType else self.processing_cost

    def cost(self):
        return self.process_cost() + self.dependencies_cost()

    def post_process(self, res):
        return self.fn(res) if self.fn else res

    def apply(self):
        return self.post_process(self.dependency_tasks())

    def __or__(self, func):
        return func(self())

    def __add__(self, value):
        return Node(*self.dependencies, value)

    def __call__(self, *args, **kwds):
        return self.apply()

    async def __aiter__(self):
        yield await self.aapply()

    async def apost_process(self, res):
        return await self.fn(res) if self.fn else res

    # TODO similar to aaply in stream, use queue
    async def aapply(self):
        tasks = [d.aapply() for d in self.dependencies]
        res = await asyncio.gather(*tasks)
        return await self.apost_process(res)

