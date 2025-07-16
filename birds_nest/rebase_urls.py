import ast
import os
with open("pybirdai/urls.py") as f:
    tree = ast.parse(f.read())
keys = set()
new_elts = list()
for elt in tree.body[-1].value.elts:
    if elt.args[0].value not in keys:
        keys.add(elt.args[0].value)
        new_elts.append(elt)
tree.body[-1].value = ast.List(new_elts)
with open("pybirdai/urls.py","w") as f:
    f.write(ast.unparse(tree))
os.system("uv run ruff format pybirdai/urls.py")
