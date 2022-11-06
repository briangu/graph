# graph flow

# Examples


a = Node(1, name="a")
b = Node(2, name="b")
c = Node(name="c", fn=lambda x: sum(x))(a, b)
c.pretty_print()
c
 a
 |1
 b
  2
c()
3


class TemperaturesNode(Node):
    def


t = TemperaturesNode(timedelta(days=1))
a = MinNode()(t)
b = MaxNode()(t)
v = AverageNode()(t)
c = Node()(a,b,v)


a = Node(1, name="a")
b = Node(2, name="b")
c = (a + b) % "combined nodes" | Node(name="c", fn=lambda x: sum(x[0]))
c.pretty_print()
c
 combined nodes
  1
  b
   2
> print(c())
3


a = Node(1, name="a")
b = Node(2, name="b")
c = Node(name="c", fn=lambda x: sum(x))
c = a | c
c = b | c
c.pretty_print()
c
 a
 |1
 b
  2
c()
3
