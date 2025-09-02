# coding=UTF-8
# Copyright (c) 2025 Arfa Digital Consulting
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Benjamin Arfa - initial API and implementation
#

import os

adc_line_comment_python = """# coding=UTF-8
# Copyright (c) 2025 Arfa Digital Consulting
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Benjamin Arfa - initial API and implementation
#"""

bss_line_comment_python = """# coding=UTF-8
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
#    Benjamin Arfa - improvements
#"""

save_path_for_correction = list()
for root, _, files in os.walk("."):
    if "./pybirdai" in root:
        for file in files:
            pathlike = root + os.sep + file
            if ".py" in file[-3:] and "model.py" not in file and "models.py" not in file and "migrations" not in root:
                if "SPDX-License-Identifier: EPL-2.0" not in open(pathlike).read():
                    save_path_for_correction.append(pathlike)

for path in save_path_for_correction:
    with open(path) as f:
        data = f.read()
        file_data = bss_line_comment_python + "\n" + data

    with open(path,"w") as f:
        f.write(file_data)

html_line_comment = """<!--
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
#    Benjamin Arfa - improvements
#
-->"""

save_path_for_correction = list()
for root, _, files in os.walk("."):
    if "./pybirdai" in root:
        for file in files:
            pathlike = root + os.sep + file
            if ".html" in file and "_REF" not in file:
                if "SPDX-License-Identifier: EPL-2.0" not in open(pathlike).read():
                    save_path_for_correction.append(pathlike)

for path in save_path_for_correction:
    with open(path) as f:
        data = f.read()
        file_data = html_line_comment + "\n" + data

    with open(path,"w") as f:
        f.write(file_data)
