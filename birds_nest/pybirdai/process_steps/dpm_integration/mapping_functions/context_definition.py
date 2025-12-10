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
import pandas as pd
from pybirdai.process_steps.dpm_integration.mapping_functions.utils import (
    pascal_to_upper_snake
)


def map_context_definition(path=os.path.join("target", "ContextDefinition.csv"), dimension_map: dict = {}, member_map: dict = {}):
    """Map context definitions from ContextDefinition.csv to the target format"""
    df = pd.read_csv(path, dtype=str)

    # Transform column names to UPPER_SNAKE_CASE
    df.columns = [pascal_to_upper_snake(col) for col in df.columns]

    df['MAINTENANCE_AGENCY_ID'] = "EBA"

    # Convert to int then string for mapping (handles floats like "123.0")
    df['DIMENSION_ID'] = pd.to_numeric(df['DIMENSION_ID'], errors='coerce').fillna(0).astype(int).astype(str)
    df['MEMBER_ID'] = pd.to_numeric(df['MEMBER_ID'], errors='coerce').fillna(0).astype(int).astype(str)

    # Vectorized ID mapping
    df['DIMENSION_ID'] = df['DIMENSION_ID'].map(dimension_map).fillna(df['DIMENSION_ID'])
    df['MEMBER_ID'] = df['MEMBER_ID'].map(member_map).fillna(df['MEMBER_ID'])

    return df, {}
