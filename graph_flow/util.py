from .node import Node

def print_nodes(node, include_costs=False):
    def _print_nodes(node, indent = "", siblingCount = 0):
        if siblingCount > 1:
            child_indent = indent + "|"
        else:
            child_indent = indent + " "
        if include_costs:
            print("{}{} [{}: (node){} + (deps){}]".format(indent, node.name, node.cost(), node.process_cost(), node.dependencies_cost()))
        else:
            print("{}{}".format(indent, node.name))
        [_print_nodes(d, child_indent, len(node.dependencies)-i) for i,d in enumerate(node.dependencies)]

    _print_nodes(node)


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

    def __init__(self, dependency, name = None, cost = None):
        super().__init__(dependency, name = name, cost = cost)

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

