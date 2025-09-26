# coding=UTF-8
# Copyright (c) 2025 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Neil Mackenzie - initial API and implementation

import ast
import os
with open("../pybirdai/urls.py") as f:
    tree = ast.parse(f.read())
keys = set()
new_elts = list()
for elt in tree.body[-1].value.elts:
    if elt.args[0].value not in keys:
        keys.add(elt.args[0].value)
        new_elts.append(elt)
tree.body[-1].value = ast.List(new_elts)
with open("../pybirdai/urls.py","w") as f:
    f.write(ast.unparse(tree))
os.system("uv run black ../pybirdai/urls.py")
