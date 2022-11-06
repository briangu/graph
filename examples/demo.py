from graph_flow.node import *
# from graph_flow.util import MinCostNode, CacheNode


class MinNode(Node):
    def __init__(self):
        super().__init__(name="min", fn=lambda x: min(x[0]))

class MaxNode(Node):
    def __init__(self):
        super().__init__(name="max", fn=lambda x: max(x[0]))

class AverageNode(Node):
    def __init__(self):
        super().__init__(name="avg", fn=lambda x: sum(x[0]) / len(x[0]))


if __name__ == '__main__':
    t = Node()
    a = MinNode()(t)
    b = MaxNode()(t)
    v = AverageNode()(t)
    c = Node(name="output", fn=lambda x: print(f"min: {x[0]} max: {x[1]} avg: {x[2]}"))(a,b,v)
    g = Graph(inputs=t, outputs=c)
    g.summary()
    g([1,2,3,4,5])

