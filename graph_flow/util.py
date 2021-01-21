
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


