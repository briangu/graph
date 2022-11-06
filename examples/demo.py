from graph_comp.node import *


class MinNode(Node):
    def __init__(self):
        super().__init__(name="min", fn=lambda x: min(x[0]))

class MaxNode(Node):
    def __init__(self):
        super().__init__(name="max", fn=lambda x: max(x[0]))

class AverageNode(Node):
    def __init__(self):
        super().__init__(name="avg", fn=lambda x: sum(x[0]) / len(x[0]))


def functional_style():
    t = Node()
    a = MinNode()(t)
    b = MaxNode()(t)
    v = AverageNode()(t)
    c = Node(name="output")(a, b, v)
    g = Graph(inputs=t, outputs=c)
    g.summary()
    r = g([1,2,3,4,5])
    print(f"min: {r[0]} max: {r[1]} avg: {r[2]}")


def pipe_style():
    t = Node()
    a = t | MinNode()
    b = t | MaxNode()
    v = t | AverageNode()
    c = (a + b + v) % "output"
    g = Graph(inputs=t, outputs=c)
    g.summary()
    r = g([1,2,3,4,5])
    print(f"min: {r[0]} max: {r[1]} avg: {r[2]}")


def only_nodes():
    t = Node([1,2,3,4,5])
    a = t | MinNode()
    b = t | MaxNode()
    v = t | AverageNode()
    c = (a + b + v) % "output"
    r = c()
    print(f"min: {r[0]} max: {r[1]} avg: {r[2]}")


def lambda_style_nodes():
    t = Node([1,2,3,4,5])
    a = t | Node(name="min", fn=lambda x: min(x[0]))
    b = t | Node(name="max", fn=lambda x: max(x[0]))
    v = t | Node(name="avg", fn=lambda x: sum(x[0]) / len(x[0]))
    c = (a + b + v) % "output"
    r = c()
    print(f"min: {r[0]} max: {r[1]} avg: {r[2]}")


def lambda_style_pipes():
    t = Node([1,2,3,4,5])
    a = t | (lambda x: min(x[0]))
    b = t | (lambda x: max(x[0]))
    v = t | (lambda x: sum(x[0]) / len(x[0]))
    c = (a + b + v) % "output"
    r = c()
    print(f"min: {r[0]} max: {r[1]} avg: {r[2]}")


if __name__ == '__main__':
    functional_style()
    pipe_style()
    only_nodes()
    lambda_style_nodes()
    lambda_style_pipes()
