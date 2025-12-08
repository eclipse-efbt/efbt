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
import re
import pandas as pd
import numpy as np
from pybirdai.process_steps.dpm_integration.mapping_functions.utils import (
    pascal_to_upper_snake, normalize_id_map
)


def traceback_restrictions(path=None, base_path="target"):
    """Load and process open member restrictions

    Args:
        path: Path to OpenMemberRestriction.csv (deprecated, use base_path instead)
        base_path: Base directory containing CSV files (default: "target")
    """
    if path is None:
        path = os.path.join(base_path, "OpenMemberRestriction.csv")
    df = pd.read_csv(path, dtype=str)

    # Rename columns with "Restriction" prefix (except RestrictionID)
    rename_dict = {col: "Restriction" + col for col in df.columns if col != "RestrictionID"}
    df = df.rename(columns=rename_dict)

    return df


def map_ordinate_categorisation(path=None, member_map: dict = {}, dimension_map: dict = {}, ordinate_map: dict = {}, hierarchy_map: dict = {}, metrics_map: dict = {}, start_index_after_last: bool = False, base_path="target"):
    """Map ordinate categorisation from OrdinateCategorisation.csv to the target format

    Args:
        path: Path to OrdinateCategorisation.csv (deprecated, use base_path instead)
        member_map: Dictionary mapping member IDs
        dimension_map: Dictionary mapping dimension IDs
        ordinate_map: Dictionary mapping ordinate IDs
        hierarchy_map: Dictionary mapping hierarchy IDs
        metrics_map: Dictionary mapping metrics
        start_index_after_last: Whether to start ID index after the last existing ID
        base_path: Base directory containing CSV files (default: "target")
    """
    if path is None:
        path = os.path.join(base_path, "OrdinateCategorisation.csv")
    df = pd.read_csv(path, dtype=str)

    # Merge with restrictions
    restrictions_df = traceback_restrictions(base_path=base_path)
    if not restrictions_df.empty:
        df = df.merge(restrictions_df, on="RestrictionID", how="left")

    # Transform column names to UPPER_SNAKE_CASE
    df.columns = [pascal_to_upper_snake(col) for col in df.columns]
    df['MAINTENANCE_AGENCY_ID'] = "EBA"

    # Filter categorisations: only keep items where ORDINATE_ID exists in ordinate_map (cascade filter)
    if ordinate_map:
        df = df[df['ORDINATE_ID'].astype(str).isin(ordinate_map.keys())]

    # Normalize ID maps for .0 variants
    member_map_norm = normalize_id_map(member_map)
    hierarchy_map_norm = normalize_id_map(hierarchy_map)

    # Vectorized ID mapping (all 5 ID fields)
    df['MEMBER_ID'] = df['MEMBER_ID'].astype(str).map(member_map_norm).fillna(df['MEMBER_ID'])
    df['DIMENSION_ID'] = df['DIMENSION_ID'].astype(str).map(dimension_map).fillna(df['DIMENSION_ID'])
    df['ORDINATE_ID'] = df['ORDINATE_ID'].astype(str).map(ordinate_map).fillna(df['ORDINATE_ID'])

    if 'RESTRICTION_HIERARCHY_ID' in df.columns:
        df['RESTRICTION_HIERARCHY_ID'] = df['RESTRICTION_HIERARCHY_ID'].astype(str).map(hierarchy_map_norm).fillna(df['RESTRICTION_HIERARCHY_ID'])

    if 'RESTRICTION_MEMBER_ID' in df.columns:
        df['RESTRICTION_MEMBER_ID'] = df['RESTRICTION_MEMBER_ID'].astype(str).map(member_map_norm).fillna(df['RESTRICTION_MEMBER_ID'])

    # Rename fields
    df = df.rename(columns={
        "ORDINATE_ID": "AXIS_ORDINATE_ID",
        "DIMENSION_ID": "VARIABLE_ID",
        "RESTRICTION_HIERARCHY_ID": "MEMBER_HIERARCHY_ID",
        "RESTRICTION_MEMBER_ID": "STARTING_MEMBER_ID",
        "RESTRICTION_MEMBER_INCLUDED": "IS_STARTING_MEMBER_INCLUDED"
    })

    # TRANSFORM ATY ORDINATE ITEMS: Replace dimension variable with field variables
    aty_transformation_stats = {
        "aty_items_found": 0,
        "aty_items_transformed": 0,
        "aty_items_not_in_metrics_map": 0
    }

    if metrics_map:
        # Identify ATY dimension items
        aty_mask = (df['VARIABLE_ID'] == 'EBA_ATY')
        aty_transformation_stats["aty_items_found"] = aty_mask.sum()

        if aty_mask.any():
            # VECTORIZED: Extract member codes from member_id
            # Example: EBA_AT_EBA_ei207 → ei207
            aty_df = df.loc[aty_mask].copy()

            # Vectorized extraction: split on '_EBA_' and take the last part
            aty_df['_member_code'] = aty_df['MEMBER_ID'].astype(str).str.split('_EBA_').str[-1]
            # Handle invalid splits (those that didn't have _EBA_)
            aty_df.loc[~aty_df['MEMBER_ID'].astype(str).str.contains('_EBA_', na=False), '_member_code'] = None

            # Vectorized mapping: get field variable from metrics_map
            aty_df['_field_variable'] = aty_df['_member_code'].map(metrics_map)

            # Mask for successful transformations
            transform_mask = aty_df['_field_variable'].notna()
            aty_transformation_stats["aty_items_transformed"] = transform_mask.sum()
            aty_transformation_stats["aty_items_not_in_metrics_map"] = (~transform_mask).sum()

            if transform_mask.any():
                # Apply variable transformations
                df.loc[aty_df[transform_mask].index, 'VARIABLE_ID'] = aty_df.loc[transform_mask, '_field_variable'].values

                # Vectorized: Extract domain from hierarchy and set member_id to x0
                transform_indices = aty_df[transform_mask].index
                hierarchy_values = df.loc[transform_indices, 'MEMBER_HIERARCHY_ID'].astype(str)

                # Extract domain using vectorized regex
                domain_extracted = hierarchy_values.str.extract(r'(EBA_[A-Z]+)', expand=False)
                x0_member_ids = domain_extracted + '_x0'

                # Set member_id based on whether domain was extracted
                has_valid_hierarchy = (
                    df.loc[transform_indices, 'MEMBER_HIERARCHY_ID'].notna() &
                    (df.loc[transform_indices, 'MEMBER_HIERARCHY_ID'] != '') &
                    domain_extracted.notna()
                )

                df.loc[transform_indices, 'MEMBER_ID'] = np.where(
                    has_valid_hierarchy.values,
                    x0_member_ids.values,
                    None
                )

    # Convert IS_STARTING_MEMBER_INCLUDED to bool
    if 'IS_STARTING_MEMBER_INCLUDED' in df.columns:
        df['IS_STARTING_MEMBER_INCLUDED'] = (
            df['IS_STARTING_MEMBER_INCLUDED']
            .astype(str)
            .str.lower()
            .isin(['true', '1', 'yes'])
        )
    else:
        df['IS_STARTING_MEMBER_INCLUDED'] = False

    df['MEMBER_HIERARCHY_VALID_FROM'] = ""

    # Set IS_STARTING_MEMBER_INCLUDED to False where STARTING_MEMBER_ID is empty
    if 'STARTING_MEMBER_ID' in df.columns:
        empty_mask = df['STARTING_MEMBER_ID'].isin(['', 'nan']) | df['STARTING_MEMBER_ID'].isna()
        df.loc[empty_mask, 'IS_STARTING_MEMBER_INCLUDED'] = False

    # Handle ID generation
    if start_index_after_last and 'ID' in df.columns and not df.empty:
        valid_ids = pd.to_numeric(df['ID'], errors='coerce').dropna()
        max_id = int(valid_ids.max()) if not valid_ids.empty else 0
        start_idx = max_id + 1
        df['ID'] = range(start_idx, start_idx + len(df))
    else:
        df['ID'] = range(len(df))

    # Identify ordinate items with null/empty/invalid MEMBER_ID (including nan_EBA_x999)
    null_member_mask = (
        df['MEMBER_ID'].isna() |
        df['MEMBER_ID'].isin(['', 'nan', 'None', 'NaN']) |
        df['MEMBER_ID'].str.contains('nan_EBA_x999', na=False)
    )
    variables_with_null_members = df.loc[null_member_mask, 'VARIABLE_ID'].unique().tolist()

    # Select final columns (order must match ORDINATE_ITEM model field order)
    df = df[[
        "ID",
        "AXIS_ORDINATE_ID",
        "VARIABLE_ID",
        "MEMBER_ID",
        "MEMBER_HIERARCHY_ID",
        "MEMBER_HIERARCHY_VALID_FROM",
        "STARTING_MEMBER_ID",
        "IS_STARTING_MEMBER_INCLUDED"
    ]]

    return df, {
        "variables_with_null_members": variables_with_null_members,
        "aty_transformation": aty_transformation_stats
    }


def update_ordinate_items_with_default_hierarchies(
    ordinate_items_df: pd.DataFrame,
    variable_to_domain_map: dict,
    domain_to_hierarchy_map: dict
) -> pd.DataFrame:
    """
    Update ordinate items that have null members with default hierarchy information.

    IMPORTANT: Only applies to enumerated domains. Natural domains (Float, String, Integer, etc.)
    should remain with NULL members as they don't have enumerated values.

    Args:
        ordinate_items_df: DataFrame of ordinate items
        variable_to_domain_map: Dict mapping variable_id to domain_id
        domain_to_hierarchy_map: Dict mapping domain_id to hierarchy_id

    Returns:
        Updated ordinate items DataFrame
    """
    import logging
    logger = logging.getLogger(__name__)

    # Define natural/scalar domains that should NOT have members/hierarchies
    NATURAL_DOMAINS = {
        'EBA_Float', 'EBA_String', 'EBA_Integer', 'EBA_Date', 'EBA_DateTime',
        'EBA_Boolean', 'EBA_Decimal', 'EBA_Double', 'EBA_Long',
        'EBA_Duration', 'EBA_Time', 'EBA_URI', 'EBA_XHTML'
    }

    # Identify rows with null/empty/invalid MEMBER_ID (including nan_EBA_x999)
    null_member_mask = (
        ordinate_items_df['MEMBER_ID'].isna() |
        ordinate_items_df['MEMBER_ID'].isin(['', 'nan', 'None', 'NaN']) |
        ordinate_items_df['MEMBER_ID'].str.contains('nan_EBA_x999', na=False)
    )

    if not null_member_mask.any():
        return ordinate_items_df

    # VECTORIZED: Map variables to domains
    null_items = ordinate_items_df.loc[null_member_mask].copy()
    null_items['_domain_id'] = null_items['VARIABLE_ID'].map(variable_to_domain_map)

    # Create masks for natural vs enumerated domains
    is_natural_domain = null_items['_domain_id'].isin(NATURAL_DOMAINS)
    has_hierarchy = null_items['_domain_id'].map(lambda x: x in domain_to_hierarchy_map if pd.notna(x) else False)
    is_enumerated = ~is_natural_domain & has_hierarchy & null_items['_domain_id'].notna()

    # Track domains for logging
    natural_domains_skipped = set(null_items.loc[is_natural_domain, '_domain_id'].dropna().unique())
    enumerated_domains_processed = set(null_items.loc[is_enumerated, '_domain_id'].dropna().unique())

    if is_enumerated.any():
        # Get enumerated items
        enum_indices = null_items[is_enumerated].index
        enum_domains = null_items.loc[is_enumerated, '_domain_id']

        # Vectorized: Compute hierarchy IDs and x0 member IDs
        hierarchy_ids = enum_domains.map(domain_to_hierarchy_map)
        x0_member_ids = enum_domains + '_x0'

        # Apply updates using vectorized operations
        ordinate_items_df.loc[enum_indices, 'MEMBER_ID'] = x0_member_ids.values
        ordinate_items_df.loc[enum_indices, 'MEMBER_HIERARCHY_ID'] = hierarchy_ids.values
        ordinate_items_df.loc[enum_indices, 'STARTING_MEMBER_ID'] = x0_member_ids.values
        ordinate_items_df.loc[enum_indices, 'IS_STARTING_MEMBER_INCLUDED'] = True

    # Log summary
    if natural_domains_skipped:
        logger.info(f"Skipped {len(natural_domains_skipped)} natural domains (kept NULL members): {sorted(natural_domains_skipped)}")
    if enumerated_domains_processed:
        logger.info(f"Created hierarchies for {len(enumerated_domains_processed)} enumerated domains: {sorted(enumerated_domains_processed)}")

    return ordinate_items_df
