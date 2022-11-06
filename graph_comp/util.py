from .node import Node


class TraceNode(Node):

    def __init__(self, node):
        name = "t_{}".format(node.name)
        deps = [TraceNode(d) for d in node.dependencies]
        super().__init__(*deps, name=name, cost = node.cost, fn = node.fn)
        self.node = node

    def post_process(self, res):
        print("post process: {} {}".format(self.name, res))
        pp_res = super().post_process(res)
        return pp_res if pp_res else self.name


class MinCostNode(Node):
    def min_dependency(self):
        if not self.deps:
            return 0, None
        return min((n.cost() if isinstance(n, Node) else 0, i) for (i, n) in enumerate(self.deps))

    def dependency_results(self):
        _, i = self.min_dependency()
        return [self.deps[i]()] if i else []

    def dependencies_cost(self):
        return self.min_dependency()[0]


class CacheNode(Node):

    internal_cache = None

    def __init__(self, dependency, name = None, cost = None):
        super().__init__(dependency, name = name, cost = cost)

    def cost(self):
        return self.process_cost() + self.dependencies_cost() if not self.internal_cache else 0

    def set_cache(self, val):
        self.internal_cache = val
        return val

    def get_cache(self):
        val = self.internal_cache
        return val

    def apply(self):
        return self.get_cache() if self.internal_cache else self.set_cache(super().apply())

    async def aset_cache(self, val):
        self.internal_cache = val
        return val

    async def aget_cache(self):
        val = self.internal_cache
        return val

    async def aapply(self):
        return await self.aget_cache() if self.internal_cache else await self.aset_cache(await super().aapply())

