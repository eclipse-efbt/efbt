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
import subprocess

subprocess.run(["uv", "run", "pybirdai/standalone/standalone_fetch_artifacts_eil.py"], check=True)
subprocess.run(["uv", "run", "pybirdai/standalone/standalone_setup_migrate_database.py"], check=True)
