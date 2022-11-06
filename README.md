# Graph Flow

Graph Flow is a simple library that lets you construct DAG synchronous computation graphs in various ways.

It's loosely based on https://github.com/twitter/nodes, which I designed the v1 of.  Twitter nodes was used to represent search execution graphs, making it easy for data scientists to create new search experiences.

Of course you can represent compute using native language constructs (such as comprehensions), however there a few advantages that make an explicit graph representation appealing for some use cases:

1. Explicit graph which an be displayed
2. Cost can be assigned to nodes to allow for optimizations
3. Reusable compute graphs
4. Modularization of compute operations
5. Cache support for nodes to prevent recomputation / fetching
6. Alternative ergonomics for computation graph construction

# Examples

Graph flow provides a bunch of ways to construct graphs based on the desired ergonimics.

For these examples, assume we have temperature timeseries data and we want to compute some stats about it: min, max, and average temps.


    # Here we construct a set of custom nodes to perform some actions.

    class MinNode(Node):
        def __init__(self):
            super().__init__(name="min", fn=lambda x: min(x[0]))

    class MaxNode(Node):
        def __init__(self):
            super().__init__(name="max", fn=lambda x: max(x[0]))

    class AverageNode(Node):
        def __init__(self):
            super().__init__(name="avg", fn=lambda x: sum(x[0]) / len(x[0]))

## Functional style

    def functional_style():
        t = Node([1,2,3,4,5])
        a = MinNode()(t)
        b = MaxNode()(t)
        v = AverageNode()(t)
        c = Node(name="output")(a, b, v)
        r = c()
        print(f"min: {r[0]} max: {r[1]} avg: {r[2]}")

##  Pipes style

    def graph_pipe_style():
        t = Node([1,2,3,4,5])
        a = t | MinNode()
        b = t | MaxNode()
        v = t | AverageNode()
        c = (a + b + v) % "output"
        r = c()
        print(f"min: {r[0]} max: {r[1]} avg: {r[2]}")

## Nodes w/ lambda style

    def lambda_style_nodes():
        t = Node([1,2,3,4,5])
        a = t | Node(name="min", fn=lambda x: min(x[0]))
        b = t | Node(name="max", fn=lambda x: max(x[0]))
        v = t | Node(name="avg", fn=lambda x: sum(x[0]) / len(x[0]))
        c = (a + b + v) % "output"
        r = c()
        print(f"min: {r[0]} max: {r[1]} avg: {r[2]}")


## Pure lambda style
    def lambda_style_pipes():
        t = Node([1,2,3,4,5])
        a = t | (lambda x: min(x[0]))
        b = t | (lambda x: max(x[0]))
        v = t | (lambda x: sum(x[0]) / len(x[0]))
        c = (a + b + v) % "output"
        r = c()
        print(f"min: {r[0]} max: {r[1]} avg: {r[2]}")

## Graph functional style

    def graph_functional_style():
        t = Node()
        a = MinNode()(t)
        b = MaxNode()(t)
        v = AverageNode()(t)
        c = Node(name="output")(a, b, v)
        g = Graph(inputs=t, outputs=c)
        g.summary()
        r = g([1,2,3,4,5])
        print(f"min: {r[0]} max: {r[1]} avg: {r[2]}")

## Graph with Pipes style

    def graph_pipe_style():
        t = Node()
        a = t | MinNode()
        b = t | MaxNode()
        v = t | AverageNode()
        c = (a + b + v) % "output"
        g = Graph(inputs=t, outputs=c)
        g.summary()
        r = g([1,2,3,4,5])
        print(f"min: {r[0]} max: {r[1]} avg: {r[2]}")

