import asyncio
import types


class Node():
    @staticmethod
    def __flatten(arr):
        arr = [x for x in arr if x]
        return arr[0] if len(arr) == 1 else arr

    def __init__(self, *deps, name = None, cost = None, fn = None):
        self.deps = deps
        self.name = name
        self.fn_cost = cost or 0
        self.fn = fn or Node.__flatten

    def __iter__(self):
        yield self.apply()

    def traverse(self):
        def recursive_iter(node):
            yield node
            for d in node.dependencies:
                yield from recursive_iter(d)
        yield from recursive_iter(self)

    def dependency_results(self):
        return [d() if isinstance(d, Node) or type(d) == types.FunctionType else d for d in self.deps]

    def dependencies_cost(self):
        return sum([d.cost() if isinstance(d, Node) else 0 for d in self.deps])

    def process_cost(self):
        return self.fn_cost() if type(self.fn_cost) == types.FunctionType else self.fn_cost

    def cost(self):
        return self.process_cost() + self.dependencies_cost()

    def post_process(self, res):
        return self.fn(res) if self.fn else res

    def apply(self):
        return self.post_process(self.dependency_results())

    def __or__(self, func):
        return Node(func(self))

    def __add__(self, value):
        return Node(self, value) if self.fn != Node.__flatten or self.process_cost() != 0 else Node(*self.deps, value)

    def __call__(self, *args, **kwds):
        return self.apply()

    def pretty_print(self, include_costs=False):
        def _print_nodes(node, indent = "", siblingCount = 0):
            if siblingCount > 1:
                child_indent = indent + "|"
            else:
                child_indent = indent + " "
            name = node.name if isinstance(node, Node) else node
            if include_costs:
                print("{}{} [{}: (node){} + (deps){}]".format(indent, name, node.cost(), node.process_cost(), node.dependencies_cost()))
            else:
                print("{}{}".format(indent, name))
            if isinstance(node, Node):
                [_print_nodes(d, child_indent, len(node.deps)-i) for i,d in enumerate(node.deps)]

        _print_nodes(self)
