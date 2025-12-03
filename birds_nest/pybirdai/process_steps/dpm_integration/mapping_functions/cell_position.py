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
    pascal_to_upper_snake, apply_cascade_filter
)


def map_cell_position(path=None, cell_map: dict = {}, ordinate_map: dict = {}, start_index_after_last: bool = False, base_path="target"):
    """Map cell positions from CellPosition.csv to the target format

    Args:
        path: Path to CellPosition.csv (deprecated, use base_path instead)
        cell_map: Dictionary mapping cell IDs
        ordinate_map: Dictionary mapping ordinate IDs
        start_index_after_last: Whether to start ID index after the last existing ID
        base_path: Base directory containing CSV files (default: "target")
    """
    if path is None:
        path = os.path.join(base_path, "CellPosition.csv")
    df = pd.read_csv(path, dtype=str)

    # Transform column names to UPPER_SNAKE_CASE
    df.columns = [pascal_to_upper_snake(col) for col in df.columns]

    # Filter positions: only keep positions where both CELL_ID and ORDINATE_ID are valid (cascade filter)
    df = apply_cascade_filter(df, 'CELL_ID', cell_map)
    df = apply_cascade_filter(df, 'ORDINATE_ID', ordinate_map)

    # Map cell and ordinate IDs
    df['CELL_ID'] = df['CELL_ID'].astype(str).map(cell_map).fillna(df['CELL_ID'])
    df['ORDINATE_ID'] = df['ORDINATE_ID'].astype(str).map(ordinate_map).fillna(df['ORDINATE_ID'])

    # Generate IDs
    if start_index_after_last and 'ID' in df.columns and not df.empty:
        valid_ids = pd.to_numeric(df['ID'], errors='coerce').dropna()
        max_id = int(valid_ids.max()) if not valid_ids.empty else 0
        start_idx = max_id + 1 if max_id else 0
        df['ID'] = range(start_idx, start_idx + len(df))
    else:
        df['ID'] = range(len(df))

    # Rename column
    df = df.rename(columns={"ORDINATE_ID": "AXIS_ORDINATE_ID"})

    # Reorder columns: ID, CELL_ID, AXIS_ORDINATE_ID, then others
    desired_cols = ["ID", "CELL_ID", "AXIS_ORDINATE_ID"]
    existing_desired = [col for col in desired_cols if col in df.columns]
    remaining = [col for col in df.columns if col not in existing_desired]
    df = df[existing_desired + remaining]

    return df, {}
