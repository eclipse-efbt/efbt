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
import time
import gc
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

# Database column names for ordinate_item.csv (enables SQLite bulk import)
# SQLite .import --skip 1 matches columns by name, auto-generates missing 'id'
ORDINATE_ITEM_DB_COLUMNS = [
    "member_hierarchy_valid_from",
    "is_starting_member_included",
    "axis_ordinate_id_id",
    "variable_id_id",
    "member_id_id",
    "member_hierarchy_id_id",
    "starting_member_id_id"
]

# Mapping from DataFrame columns to database columns
ORDINATE_ITEM_COLUMN_RENAME = {
    "AXIS_ORDINATE_ID": "axis_ordinate_id_id",
    "VARIABLE_ID": "variable_id_id",
    "MEMBER_ID": "member_id_id",
    "MEMBER_HIERARCHY_ID": "member_hierarchy_id_id",
    "MEMBER_HIERARCHY_VALID_FROM": "member_hierarchy_valid_from",
    "STARTING_MEMBER_ID": "starting_member_id_id",
    "IS_STARTING_MEMBER_INCLUDED": "is_starting_member_included"
}


def prepare_ordinate_item_for_csv(df):
    """
    Rename columns to database names for SQLite bulk import compatibility.

    SQLite's .import --skip 1 matches columns by name and auto-generates 'id'.
    This function renames columns to database names for direct bulk import.

    Args:
        df: DataFrame with ordinate item data

    Returns:
        DataFrame with database column names for CSV export
    """
    logger.debug(f"prepare_ordinate_item_for_csv: input shape={df.shape}")

    if df.empty:
        # IMPORTANT: Even if empty, return with correct column names for CSV header
        empty_result = pd.DataFrame(columns=ORDINATE_ITEM_DB_COLUMNS)
        logger.info("DataFrame is empty - returning empty frame with correct database column names")
        return empty_result

    # Work on a copy to avoid modifying original DataFrame
    df = df.copy()

    # Explicitly drop ID column if present - SQLite will auto-generate
    if 'ID' in df.columns:
        df = df.drop(columns=['ID'])
        logger.info("Dropped ID column")

    # Rename columns to database names
    df = df.rename(columns=ORDINATE_ITEM_COLUMN_RENAME)
    logger.info(f"After rename, columns: {list(df.columns)}")

    # Ensure all required columns exist (fill missing with empty string)
    for col in ORDINATE_ITEM_DB_COLUMNS:
        if col not in df.columns:
            df[col] = ''

    # Replace NaN/None with empty string to ensure proper CSV formatting
    # IMPORTANT: Preserve NULL for FK columns (ending in _id_id) to avoid FK violations
    # Non-FK columns get empty string, FK columns remain NaN (will be exported with na_rep='')
    fk_columns = ['member_id_id', 'member_hierarchy_id_id', 'starting_member_id_id']
    non_fk_columns = [col for col in df.columns if col not in fk_columns]
    df[non_fk_columns] = df[non_fk_columns].fillna('')
    # FK columns: Replace empty/whitespace strings and 'None' string with NaN (exports as empty with na_rep='')
    for fk_col in fk_columns:
        if fk_col in df.columns:
            # For non-null values, convert to string, strip whitespace, then replace invalid values with NaN
            mask = df[fk_col].notna()
            if mask.any():
                # Convert to string and strip whitespace for actual values
                df.loc[mask, fk_col] = df.loc[mask, fk_col].astype(str).str.strip()
                # Replace empty strings, 'None', 'nan' with NaN
                df[fk_col] = df[fk_col].replace(['', 'None', 'nan', 'NaN'], np.nan)

    # Select only the required columns in the correct order
    result = df[ORDINATE_ITEM_DB_COLUMNS]
    logger.debug(f"prepare_ordinate_item_for_csv: output shape={result.shape}")

    return result


def identify_all_z_axis_tables(tables_df, axes_df=None, config_path=None):
    """
    Identify all tables that have an axis with orientation="Z" (Z-axis).

    Optimized to load from config file if available, falling back to axis scanning.

    Args:
        tables_df: DataFrame containing table definitions
        axes_df: DataFrame containing axis definitions (optional if config_path works)
        config_path: Path to z_axis_config.json (optional, uses default if not provided)

    Returns:
        DataFrame with tables that have Z-axis, including their Z-axis IDs (column: Z_AXIS_ID)
    """
    from . import table_deduplication_utils

    # Try loading from config first (fast path)
    result = table_deduplication_utils.load_z_axis_tables_from_config(tables_df, config_path)
    if result is not None:
        return result

    # Fallback: scan axes_df (original behavior)
    if axes_df is None:
        raise ValueError("axes_df is required when config file is not available")

    logger.info("Using axis scanning fallback to identify Z-axis tables")
    return table_deduplication_utils.identify_z_axis_tables(tables_df, axes_df)


def process_all_tables(tables_df, axes_df, ordinates_df, cells_df, cell_positions_df,
                      ordinate_cat_df, members_df, dimensions_df, output_directory=None,
                      min_table_version=0.0, batch_size=50, enable_batching=True,
                      hierarchy_nodes_df=None, use_bulk_mode=True):
    """
    Process all tables and duplicate those with Z-axis for each member in the domain.
    Memory-optimized with version filtering, batch processing, and incremental CSV writing.

    Args:
        tables_df: DataFrame with tables
        axes_df: DataFrame with axes
        ordinates_df: DataFrame with ordinates
        cells_df: DataFrame with cells
        cell_positions_df: DataFrame with cell positions
        ordinate_cat_df: DataFrame with ordinate categorisations
        members_df: DataFrame with members
        dimensions_df: DataFrame with dimensions
        output_directory: Path to write CSV files incrementally (if None, returns DataFrames)
        min_table_version: Minimum table version to process (default: 0.0 = all versions; set to 3.0+ to filter old versions)
        batch_size: Number of tables to process per batch (default: 50)
        enable_batching: Enable batch processing of tables (default: True, set False for one-at-a-time)
        hierarchy_nodes_df: DataFrame with hierarchy nodes (optional, used for optimized member lookup)
        use_bulk_mode: Use vectorized bulk duplication (default: True, 3-5x faster)

    Returns:
        If output_directory is None: Tuple of updated dataframes (tables, axes, ordinates, cells, cell_positions, ordinate_cats)
        If output_directory is provided: Dictionary with summary statistics
    """
    logger.info("=" * 80)
    logger.info("Starting Z-axis table duplication process (BULK-OPTIMIZED)" if use_bulk_mode else "Starting Z-axis table duplication process")
    logger.info(f"Config: min_version={min_table_version}, batch_size={batch_size if enable_batching else 1}, "
               f"batching={'enabled' if enable_batching else 'disabled'}, bulk_mode={use_bulk_mode}, output_dir={'CSV' if output_directory else 'Memory'}")
    logger.info("=" * 80)

    # Identify tables with Z-axis
    z_axis_tables = identify_all_z_axis_tables(tables_df, axes_df)

    from . import table_deduplication_utils

    if z_axis_tables.empty:
        logger.info("No tables with Z-axis found.")
        if output_directory:
            # Write original dataframes to CSV and return
            logger.info("Writing original dataframes to CSV (no duplication needed)...")
            table_deduplication_utils.write_all_csvs_to_directory(tables_df, axes_df, ordinates_df, cells_df,
                                        cell_positions_df, ordinate_cat_df, output_directory)
            return {'tables_processed': 0, 'tables_created': 0, 'message': 'No Z-axis tables found'}
        return (tables_df, axes_df, ordinates_df, cells_df, cell_positions_df, ordinate_cat_df)

    # Version filtering already done upstream in tables.py - no need to filter again here
    logger.info(f"Processing {len(z_axis_tables)} tables with Z-axis")

    # Pre-compute axis-to-members mapping (optimization: avoid redundant lookups)
    # Use hierarchy-based lookup if hierarchy_nodes_df is provided (more efficient)
    if hierarchy_nodes_df is not None and not hierarchy_nodes_df.empty:
        logger.info("Using optimized hierarchy-based member lookup")
        axis_members_map = table_deduplication_utils.precompute_duplication_info_from_ordinate_items(
            z_axis_tables, ordinates_df, ordinate_cat_df, hierarchy_nodes_df, members_df
        )
    else:
        logger.info("Using domain-based member lookup (fallback)")
        axis_members_map = table_deduplication_utils.precompute_axis_members_mapping(
            z_axis_tables, ordinates_df, ordinate_cat_df, dimensions_df, members_df
        )

    # Always use CSV incremental mode for memory efficiency
    if not output_directory:
        raise ValueError("output_directory is required for table duplication (in-memory mode removed for memory efficiency)")

    # Use bulk mode for 3-5x faster processing
    if use_bulk_mode:
        return _process_with_csv_output_bulk(
            z_axis_tables, axis_members_map, tables_df, axes_df, ordinates_df,
            cells_df, cell_positions_df, ordinate_cat_df, dimensions_df,
            output_directory, batch_size, enable_batching
        )
    else:
        # Legacy per-member processing (kept for compatibility)
        next_ids = {
            'cell': int(cells_df['CELL_ID'].str.replace('EBA_', '', regex=False).astype(int).max() + 1) if not cells_df.empty else 1,
            'cell_position': int(cell_positions_df['ID'].max() + 1) if not cell_positions_df.empty else 0,
            'ordinate_cat': int(ordinate_cat_df['ID'].max() + 1) if not ordinate_cat_df.empty else 0
        }
        return _process_with_csv_output(
            z_axis_tables, axis_members_map, tables_df, axes_df, ordinates_df,
            cells_df, cell_positions_df, ordinate_cat_df, dimensions_df, next_ids,
            output_directory, batch_size, enable_batching
        )


def _process_with_csv_output(z_axis_tables, axis_members_map, tables_df, axes_df, ordinates_df,
                            cells_df, cell_positions_df, ordinate_cat_df, dimensions_df, next_ids,
                            output_directory, batch_size, enable_batching):
    """Memory-optimized processing with incremental CSV writing using subgraph utilities."""
    import os
    import gc
    from . import table_deduplication_utils

    logger.info(f"Using CSV incremental mode (per-table subgraph processing)")

    # Prepare output file paths
    output_files = {
        'tables': os.path.join(output_directory, 'table.csv'),
        'axes': os.path.join(output_directory, 'axis.csv'),
        'ordinates': os.path.join(output_directory, 'axis_ordinate.csv'),
        'cells': os.path.join(output_directory, 'table_cell.csv'),
        'cell_positions': os.path.join(output_directory, 'cell_position.csv'),
        'ordinate_cats': os.path.join(output_directory, 'ordinate_item.csv')
    }

    # Step 1: Write non-Z-axis tables and non-Z-axis cells (Z-axis cells will be duplicated)
    tables_to_remove = set(z_axis_tables['TABLE_ID'].unique())
    logger.info(f"Writing {len(tables_df) - len(tables_to_remove)} non-Z-axis tables to CSV...")

    tables_df_filtered = tables_df[~tables_df['TABLE_ID'].isin(tables_to_remove)]
    tables_df_filtered.to_csv(output_files['tables'], index=False)

    axes_df_filtered = axes_df[~axes_df['TABLE_ID'].isin(tables_to_remove)]
    axes_df_filtered.to_csv(output_files['axes'], index=False)

    original_axis_ids = axes_df[axes_df['TABLE_ID'].isin(tables_to_remove)]['AXIS_ID'].unique()
    ordinates_df_filtered = ordinates_df[~ordinates_df['AXIS_ID'].isin(original_axis_ids)]
    ordinates_df_filtered.to_csv(output_files['ordinates'], index=False)

    # Write only non-Z-axis cells - Z-axis cells will be duplicated and appended
    cells_df_filtered = cells_df[~cells_df['TABLE_ID'].isin(tables_to_remove)]
    cells_df_filtered.to_csv(output_files['cells'], index=False)
    logger.info(f"Written {len(cells_df_filtered)} non-Z-axis cells (Z-axis cells will be duplicated)")

    # Filter cell_positions for non-Z-axis tables only (Z-axis positions will be added during duplication)
    original_cell_ids = cells_df[cells_df['TABLE_ID'].isin(tables_to_remove)]['CELL_ID'].unique()
    cell_positions_df_filtered = cell_positions_df[~cell_positions_df['CELL_ID'].isin(original_cell_ids)]
    # Drop ID column - SQLite will auto-generate (IDs are duplicated during table duplication)
    if 'ID' in cell_positions_df_filtered.columns:
        cell_positions_df_filtered = cell_positions_df_filtered.drop(columns=['ID'])
    cell_positions_df_filtered.to_csv(output_files['cell_positions'], index=False)

    original_ordinate_ids = ordinates_df[ordinates_df['AXIS_ID'].isin(original_axis_ids)]['AXIS_ORDINATE_ID'].unique()
    ordinate_cat_df_filtered = ordinate_cat_df[~ordinate_cat_df['AXIS_ORDINATE_ID'].isin(original_ordinate_ids)]

    prepared_df = prepare_ordinate_item_for_csv(ordinate_cat_df_filtered)
    prepared_df.to_csv(output_files['ordinate_cats'], index=False, na_rep='')

    # Step 2: Process Z-axis tables one at a time (subgraph processing)
    total_tables = len(z_axis_tables)

    # Calculate total duplicates to create
    total_duplicates = sum(len(axis_members_map.get(str(row['Z_AXIS_ID']), []))
                          for _, row in z_axis_tables.iterrows())

    stats = {
        'tables_processed': 0,
        'tables_created': 0,
        'axes_created': 0,
        'ordinates_created': 0,
        'cells_created': 0,
        'cell_positions_created': 0,
        'ordinate_cats_created': 0
    }

    logger.info("=" * 80)
    logger.info(f"STARTING TABLE DEDUPLICATION")
    logger.info(f"  Z-axis tables to process: {total_tables}")
    logger.info(f"  Total table duplicates to create: {total_duplicates}")
    logger.info("=" * 80)

    start_time = time.time()

    # Determine batch size
    effective_batch_size = batch_size if enable_batching else 1

    # Split tables into batches
    table_list = list(z_axis_tables.iterrows())
    table_batches = [table_list[i:i + effective_batch_size]
                    for i in range(0, len(table_list), effective_batch_size)]

    logger.info(f"Processing {len(table_batches)} batches (batch_size={effective_batch_size})")

    for batch_idx, batch in enumerate(table_batches):
        # Batch accumulation buffers (NOTE: batch_cells now included - cells ARE duplicated)
        batch_tables = []
        batch_axes = []
        batch_ordinates = []
        batch_cells = []
        batch_cell_positions = []
        batch_ordinate_cats = []

        for table_idx_in_batch, (idx, table_row) in enumerate(batch):
            table_idx = batch_idx * effective_batch_size + table_idx_in_batch
            table_id = str(table_row['TABLE_ID'])
            z_axis_id = str(table_row['Z_AXIS_ID'])

            # Check if duplication should be skipped for this Z-axis domain (EBA_GA, EBA_CU)
            should_skip, skip_reason = table_deduplication_utils.should_skip_duplication(
                z_axis_id, ordinates_df, ordinate_cat_df, dimensions_df
            )

            if should_skip:
                logger.info(f"Table {table_idx + 1}/{total_tables} ({table_id}): Skipping duplication - "
                           f"Z-axis domain is {skip_reason} (too many members)")
                # Add original table to batch buffers (no duplication)
                batch_tables.append(pd.DataFrame([table_row]))

                # Also add all related entities (axes, ordinates, cells, positions, ordinate_cats)
                table_axes = axes_df[axes_df['TABLE_ID'] == table_id]
                if not table_axes.empty:
                    batch_axes.append(table_axes)

                    axis_ids = table_axes['AXIS_ID'].unique()
                    table_ordinates = ordinates_df[ordinates_df['AXIS_ID'].isin(axis_ids)]
                    if not table_ordinates.empty:
                        batch_ordinates.append(table_ordinates)

                        ordinate_ids = table_ordinates['AXIS_ORDINATE_ID'].unique()
                        table_ordinate_cats = ordinate_cat_df[ordinate_cat_df['AXIS_ORDINATE_ID'].isin(ordinate_ids)]
                        if not table_ordinate_cats.empty:
                            batch_ordinate_cats.append(table_ordinate_cats)

                # Get cells and cell positions for this table
                table_cells = cells_df[cells_df['TABLE_ID'] == table_id]
                if not table_cells.empty:
                    batch_cells.append(table_cells)
                    cell_ids = table_cells['CELL_ID'].unique()
                    table_cell_positions = cell_positions_df[cell_positions_df['CELL_ID'].isin(cell_ids)]
                    if not table_cell_positions.empty:
                        # Drop ID column - SQLite will auto-generate (avoids duplicate IDs)
                        if 'ID' in table_cell_positions.columns:
                            table_cell_positions = table_cell_positions.drop(columns=['ID'])
                        batch_cell_positions.append(table_cell_positions)

                stats['tables_processed'] += 1
                continue

            # Get Z-axis members for this table
            members = axis_members_map.get(z_axis_id)
            if members is None or members.empty:
                logger.warning(f"Table {table_idx + 1}/{total_tables} ({table_id}): No members found. Skipping.")
                continue

            logger.info(f"Table {table_idx + 1}/{total_tables} ({table_id}): Processing {len(members)} Z-axis duplicates")

            # Get all axis IDs for this table (for metadata extraction)
            table_axes = axes_df[axes_df['TABLE_ID'] == table_id]
            axis_ids = table_axes['AXIS_ID'].unique().tolist()

            # Extract required metadata
            metadata = table_deduplication_utils.extract_required_metadata(
                table_id, axis_ids, ordinates_df, ordinate_cat_df
            )
            logger.debug(f"  Metadata: {len(metadata['variables'])} variables, "
                        f"{len(metadata['members'])} members, "
                        f"{len(metadata['hierarchies'])} hierarchies")

            # Process each Z-axis member
            for member_idx, member_row in members.iterrows():
                member_id = str(member_row['MEMBER_ID']).replace(' ', '_').replace('.', '_')
                member_name = str(member_row['NAME'])

                # Duplicate table
                new_table = table_deduplication_utils.duplicate_table_with_member(table_row, member_id, member_name)

                # Duplicate axes
                new_axes, axis_id_mapping = table_deduplication_utils.duplicate_axes_with_member(
                    axes_df, table_id, new_table['TABLE_ID'], member_id, member_name
                )

                # Duplicate ordinates
                original_axis_ids = axis_id_mapping.keys()
                new_ordinates, ordinate_id_mapping = table_deduplication_utils.duplicate_ordinates_with_member(
                    ordinates_df, original_axis_ids, axis_id_mapping, member_id, member_name
                )

                # Duplicate cells
                new_cells, cell_id_mapping = table_deduplication_utils.duplicate_cells_with_member(
                    cells_df, table_id, new_table['TABLE_ID'], member_id, member_name
                )

                # Duplicate cell positions (update both AXIS_ORDINATE_ID and CELL_ID)
                # Get cell IDs for this table to filter positions
                table_cells = cells_df[cells_df['TABLE_ID'] == table_id]
                original_cell_ids = table_cells['CELL_ID'].unique().tolist()
                new_cell_positions = table_deduplication_utils.duplicate_cell_positions_with_member(
                    cell_positions_df, original_cell_ids, ordinate_id_mapping, cell_id_mapping
                )

                # Duplicate ordinate categorisations (hierarchy exploded)
                original_ordinate_ids = ordinate_id_mapping.keys()
                new_ordinate_cats = table_deduplication_utils.duplicate_ordinate_cats_with_member(
                    ordinate_cat_df, original_ordinate_ids, ordinate_id_mapping
                )

                # Add to batch buffers (instead of writing immediately)
                # NOTE: batch_cells now included - cells ARE duplicated
                batch_tables.append(pd.DataFrame([new_table]))
                if not new_axes.empty:
                    batch_axes.append(new_axes)
                if not new_ordinates.empty:
                    batch_ordinates.append(new_ordinates)
                if not new_cells.empty:
                    batch_cells.append(new_cells)
                if not new_cell_positions.empty:
                    batch_cell_positions.append(new_cell_positions)
                if not new_ordinate_cats.empty:
                    batch_ordinate_cats.append(new_ordinate_cats)

                # Update stats
                stats['tables_created'] += 1
                stats['axes_created'] += len(new_axes)
                stats['ordinates_created'] += len(new_ordinates)
                stats['cells_created'] += len(new_cells)
                stats['cell_positions_created'] += len(new_cell_positions)
                stats['ordinate_cats_created'] += len(new_ordinate_cats)

                # Clear member-level objects to free memory immediately
                del new_table, new_axes, new_ordinates, new_cells
                del new_cell_positions, new_ordinate_cats
                del axis_id_mapping, ordinate_id_mapping, cell_id_mapping
                del original_axis_ids, original_cell_ids, original_ordinate_ids

            stats['tables_processed'] += 1

            # Per-table completion logging with timing
            elapsed = time.time() - start_time
            avg_time_per_table = elapsed / (table_idx + 1)
            remaining_tables = total_tables - (table_idx + 1)
            estimated_remaining = avg_time_per_table * remaining_tables

            logger.info(f"  ✓ Completed table {table_idx + 1}/{total_tables}: {table_id}")
            if 'members' in locals():
                logger.info(f"    Created {len(members)} table duplicates "
                           f"({stats['cell_positions_created'] - (stats.get('_last_positions', 0))} positions)")
            logger.info(f"    Elapsed: {elapsed:.1f}s | Avg: {avg_time_per_table:.2f}s/table | "
                       f"ETA: {estimated_remaining:.1f}s ({estimated_remaining/60:.1f}m)")

            # Track incremental stats for next iteration
            stats['_last_positions'] = stats['cell_positions_created']

            # Clear table-level objects to free memory
            if 'table_axes' in locals():
                del table_axes
            if 'axis_ids' in locals():
                del axis_ids
            if 'metadata' in locals():
                del metadata
            if 'members' in locals():
                del members

        # End of inner loop (all tables in batch processed)
        # Write entire batch to CSV files (write and immediately delete to free memory)
        # NOTE: batch_cells now included - cells ARE duplicated
        logger.info(f"Writing batch {batch_idx + 1}/{len(table_batches)} to CSV...")
        if batch_tables:
            temp_df = pd.concat(batch_tables, ignore_index=True)
            temp_df.to_csv(output_files['tables'], mode='a', header=False, index=False)
            del temp_df
        if batch_axes:
            temp_df = pd.concat(batch_axes, ignore_index=True)
            temp_df.to_csv(output_files['axes'], mode='a', header=False, index=False)
            del temp_df
        if batch_ordinates:
            temp_df = pd.concat(batch_ordinates, ignore_index=True)
            temp_df.to_csv(output_files['ordinates'], mode='a', header=False, index=False)
            del temp_df
        if batch_cells:
            temp_df = pd.concat(batch_cells, ignore_index=True)
            temp_df.to_csv(output_files['cells'], mode='a', header=False, index=False)
            del temp_df
        if batch_cell_positions:
            temp_df = pd.concat(batch_cell_positions, ignore_index=True)
            temp_df.to_csv(output_files['cell_positions'], mode='a', header=False, index=False)
            del temp_df
        if batch_ordinate_cats:
            temp_df = pd.concat(batch_ordinate_cats, ignore_index=True)
            prepare_ordinate_item_for_csv(temp_df).to_csv(output_files['ordinate_cats'], mode='a', header=False, index=False, na_rep='')
            del temp_df

        # Clear batch buffers and force garbage collection
        del batch_tables, batch_axes, batch_ordinates, batch_cells
        del batch_cell_positions, batch_ordinate_cats
        gc.collect()

        # Log memory usage if available
        try:
            import psutil
            process = psutil.Process()
            mem_mb = process.memory_info().rss / 1024 / 1024
            logger.info(f"✓ Batch {batch_idx + 1}/{len(table_batches)} complete | Memory: {mem_mb:.1f} MB")
        except ImportError:
            logger.info(f"✓ Batch {batch_idx + 1}/{len(table_batches)} complete")

    total_elapsed = time.time() - start_time

    logger.info("=" * 80)
    logger.info("Z-axis table duplication complete (CSV INCREMENTAL MODE)")
    logger.info(f"Total time: {total_elapsed:.1f}s ({total_elapsed/60:.1f}m)")
    logger.info(f"Tables processed: {stats['tables_processed']}")
    logger.info(f"Tables created: {stats['tables_created']}")
    logger.info(f"Axes created: {stats['axes_created']}")
    logger.info(f"Ordinates created: {stats['ordinates_created']}")
    logger.info(f"Cells created: {stats['cells_created']}")
    logger.info(f"Cell positions created: {stats['cell_positions_created']}")
    logger.info(f"Ordinate categorisations created: {stats['ordinate_cats_created']}")
    logger.info(f"Average time per table: {total_elapsed/max(stats['tables_processed'], 1):.2f}s")
    logger.info(f"Output files written to: {output_directory}")
    logger.info("=" * 80)

    return stats


def _process_with_csv_output_bulk(z_axis_tables, axis_members_map, tables_df, axes_df, ordinates_df,
                                   cells_df, cell_positions_df, ordinate_cat_df, dimensions_df,
                                   output_directory, batch_size, enable_batching):
    """
    BULK-OPTIMIZED processing with incremental CSV writing.

    Instead of processing one member at a time, this function:
    1. Pre-caches all table subgraphs
    2. Duplicates ALL members for a table in one vectorized operation
    3. Writes batches to CSV incrementally

    Expected 3-5x speedup over per-member processing.
    """
    from . import table_deduplication_utils

    logger.info("Using BULK duplication mode (vectorized operations)")

    # Prepare output file paths
    output_files = {
        'tables': os.path.join(output_directory, 'table.csv'),
        'axes': os.path.join(output_directory, 'axis.csv'),
        'ordinates': os.path.join(output_directory, 'axis_ordinate.csv'),
        'cells': os.path.join(output_directory, 'table_cell.csv'),
        'cell_positions': os.path.join(output_directory, 'cell_position.csv'),
        'ordinate_cats': os.path.join(output_directory, 'ordinate_item.csv')
    }

    # Step 1: Write non-Z-axis tables and non-Z-axis cells (Z-axis cells will be duplicated)
    tables_to_remove = set(z_axis_tables['TABLE_ID'].unique())
    logger.info(f"Writing {len(tables_df) - len(tables_to_remove)} non-Z-axis tables to CSV...")

    tables_df_filtered = tables_df[~tables_df['TABLE_ID'].isin(tables_to_remove)]
    tables_df_filtered.to_csv(output_files['tables'], index=False)

    axes_df_filtered = axes_df[~axes_df['TABLE_ID'].isin(tables_to_remove)]
    axes_df_filtered.to_csv(output_files['axes'], index=False)

    original_axis_ids = axes_df[axes_df['TABLE_ID'].isin(tables_to_remove)]['AXIS_ID'].unique()
    ordinates_df_filtered = ordinates_df[~ordinates_df['AXIS_ID'].isin(original_axis_ids)]
    ordinates_df_filtered.to_csv(output_files['ordinates'], index=False)

    # Write only non-Z-axis cells - Z-axis cells will be duplicated and appended
    cells_df_filtered = cells_df[~cells_df['TABLE_ID'].isin(tables_to_remove)]
    cells_df_filtered.to_csv(output_files['cells'], index=False)
    logger.info(f"Written {len(cells_df_filtered)} non-Z-axis cells (Z-axis cells will be duplicated)")

    # Filter cell_positions for non-Z-axis tables only (Z-axis positions will be added during duplication)
    original_cell_ids = cells_df[cells_df['TABLE_ID'].isin(tables_to_remove)]['CELL_ID'].unique()
    cell_positions_df_filtered = cell_positions_df[~cell_positions_df['CELL_ID'].isin(original_cell_ids)]
    # Drop ID column - SQLite will auto-generate (IDs are duplicated during table duplication)
    if 'ID' in cell_positions_df_filtered.columns:
        cell_positions_df_filtered = cell_positions_df_filtered.drop(columns=['ID'])
    cell_positions_df_filtered.to_csv(output_files['cell_positions'], index=False)

    original_ordinate_ids = ordinates_df[ordinates_df['AXIS_ID'].isin(original_axis_ids)]['AXIS_ORDINATE_ID'].unique()
    ordinate_cat_df_filtered = ordinate_cat_df[~ordinate_cat_df['AXIS_ORDINATE_ID'].isin(original_ordinate_ids)]

    prepared_df = prepare_ordinate_item_for_csv(ordinate_cat_df_filtered)
    prepared_df.to_csv(output_files['ordinate_cats'], index=False, na_rep='')

    # Step 2: Build table subgraph cache (pre-filter all DataFrames once)
    logger.info("Building table subgraph cache...")
    cache_start = time.time()
    table_cache = table_deduplication_utils.build_table_subgraph_cache(
        z_axis_tables, axes_df, ordinates_df, cells_df, cell_positions_df, ordinate_cat_df
    )
    logger.info(f"Cache built in {time.time() - cache_start:.2f}s")

    # Step 3: Process Z-axis tables with bulk duplication
    total_tables = len(z_axis_tables)
    total_duplicates = sum(len(axis_members_map.get(str(row['Z_AXIS_ID']), []))
                          for _, row in z_axis_tables.iterrows())

    stats = {
        'tables_processed': 0,
        'tables_created': 0,
        'axes_created': 0,
        'ordinates_created': 0,
        'cells_created': 0,
        'cell_positions_created': 0,
        'ordinate_cats_created': 0
    }

    logger.info("=" * 80)
    logger.info(f"STARTING BULK TABLE DEDUPLICATION")
    logger.info(f"  Z-axis tables to process: {total_tables}")
    logger.info(f"  Total table duplicates to create: {total_duplicates}")
    logger.info("=" * 80)

    start_time = time.time()

    # Determine batch size
    effective_batch_size = batch_size if enable_batching else 1

    # Split tables into batches
    table_list = list(z_axis_tables.iterrows())
    table_batches = [table_list[i:i + effective_batch_size]
                    for i in range(0, len(table_list), effective_batch_size)]

    logger.info(f"Processing {len(table_batches)} batches (batch_size={effective_batch_size})")

    for batch_idx, batch in enumerate(table_batches):
        batch_start = time.time()

        # Batch accumulation buffers (NOTE: batch_cells now included - cells ARE duplicated)
        batch_tables = []
        batch_axes = []
        batch_ordinates = []
        batch_cells = []
        batch_cell_positions = []
        batch_ordinate_cats = []

        for table_idx_in_batch, (idx, table_row) in enumerate(batch):
            table_idx = batch_idx * effective_batch_size + table_idx_in_batch
            table_id = str(table_row['TABLE_ID'])
            z_axis_id = str(table_row['Z_AXIS_ID'])

            # Check if duplication should be skipped for this Z-axis domain (EBA_GA, EBA_CU)
            should_skip, skip_reason = table_deduplication_utils.should_skip_duplication(
                z_axis_id, ordinates_df, ordinate_cat_df, dimensions_df
            )

            if should_skip:
                # Add original table to batch buffers (no duplication)
                cache_entry = table_cache.get(table_id, {})
                if cache_entry:
                    batch_tables.append(pd.DataFrame([table_row]))
                    if not cache_entry.get('axes', pd.DataFrame()).empty:
                        batch_axes.append(cache_entry['axes'])
                    if not cache_entry.get('ordinates', pd.DataFrame()).empty:
                        batch_ordinates.append(cache_entry['ordinates'])
                    if not cache_entry.get('cells', pd.DataFrame()).empty:
                        batch_cells.append(cache_entry['cells'])
                    if not cache_entry.get('cell_positions', pd.DataFrame()).empty:
                        batch_cell_positions.append(cache_entry['cell_positions'])
                    if not cache_entry.get('ordinate_cats', pd.DataFrame()).empty:
                        batch_ordinate_cats.append(cache_entry['ordinate_cats'])
                stats['tables_processed'] += 1
                continue

            # Get Z-axis members for this table
            members = axis_members_map.get(z_axis_id)
            if members is None or (hasattr(members, 'empty') and members.empty) or len(members) == 0:
                logger.warning(f"Table {table_idx + 1}/{total_tables} ({table_id}): No members found. Skipping.")
                continue

            # Convert members to DataFrame if it's a list
            if isinstance(members, list):
                members_df = pd.DataFrame(members)
            else:
                members_df = members

            # Get cached table subgraph
            cache_entry = table_cache.get(table_id)
            if not cache_entry:
                logger.warning(f"Table {table_idx + 1}/{total_tables} ({table_id}): Not in cache. Skipping.")
                continue

            # BULK DUPLICATE: All members for this table at once
            result = table_deduplication_utils.duplicate_table_subgraph_bulk(
                cache_entry, members_df, table_id
            )

            # Add to batch buffers (NOTE: cells ARE duplicated along with other entities)
            if not result['tables'].empty:
                batch_tables.append(result['tables'])
            if not result['axes'].empty:
                batch_axes.append(result['axes'])
            if not result['ordinates'].empty:
                batch_ordinates.append(result['ordinates'])
            if not result['cells'].empty:
                batch_cells.append(result['cells'])
            if not result['cell_positions'].empty:
                batch_cell_positions.append(result['cell_positions'])
            if not result['ordinate_cats'].empty:
                batch_ordinate_cats.append(result['ordinate_cats'])

            # Update stats
            stats['tables_processed'] += 1
            stats['tables_created'] += len(result['tables'])
            stats['axes_created'] += len(result['axes'])
            stats['ordinates_created'] += len(result['ordinates'])
            stats['cells_created'] += len(result['cells'])
            stats['cell_positions_created'] += len(result['cell_positions'])
            stats['ordinate_cats_created'] += len(result['ordinate_cats'])

        # End of batch - write to CSV (NOTE: cells ARE written - duplicated)
        if batch_tables:
            temp_df = pd.concat(batch_tables, ignore_index=True)
            temp_df.to_csv(output_files['tables'], mode='a', header=False, index=False)
            del temp_df
        if batch_axes:
            temp_df = pd.concat(batch_axes, ignore_index=True)
            temp_df.to_csv(output_files['axes'], mode='a', header=False, index=False)
            del temp_df
        if batch_ordinates:
            temp_df = pd.concat(batch_ordinates, ignore_index=True)
            temp_df.to_csv(output_files['ordinates'], mode='a', header=False, index=False)
            del temp_df
        if batch_cells:
            temp_df = pd.concat(batch_cells, ignore_index=True)
            temp_df.to_csv(output_files['cells'], mode='a', header=False, index=False)
            del temp_df
        if batch_cell_positions:
            temp_df = pd.concat(batch_cell_positions, ignore_index=True)
            temp_df.to_csv(output_files['cell_positions'], mode='a', header=False, index=False)
            del temp_df
        if batch_ordinate_cats:
            temp_df = pd.concat(batch_ordinate_cats, ignore_index=True)
            prepare_ordinate_item_for_csv(temp_df).to_csv(output_files['ordinate_cats'], mode='a', header=False, index=False, na_rep='')
            del temp_df

        # Clear batch buffers and force garbage collection
        del batch_tables, batch_axes, batch_ordinates, batch_cells
        del batch_cell_positions, batch_ordinate_cats
        gc.collect()

        # Log batch completion with timing
        batch_elapsed = time.time() - batch_start
        total_elapsed = time.time() - start_time
        tables_done = (batch_idx + 1) * effective_batch_size
        tables_done = min(tables_done, total_tables)
        avg_time = total_elapsed / tables_done if tables_done > 0 else 0
        remaining = avg_time * (total_tables - tables_done)

        logger.info(f"Batch {batch_idx + 1}/{len(table_batches)} done in {batch_elapsed:.1f}s | "
                   f"Total: {total_elapsed:.1f}s | ETA: {remaining:.1f}s")

    total_elapsed = time.time() - start_time

    logger.info("=" * 80)
    logger.info("Z-axis table duplication complete (BULK MODE)")
    logger.info(f"Total time: {total_elapsed:.1f}s ({total_elapsed/60:.1f}m)")
    logger.info(f"Tables processed: {stats['tables_processed']}")
    logger.info(f"Tables created: {stats['tables_created']}")
    logger.info(f"Axes created: {stats['axes_created']}")
    logger.info(f"Ordinates created: {stats['ordinates_created']}")
    logger.info(f"Cells created: {stats['cells_created']}")
    logger.info(f"Cell positions created: {stats['cell_positions_created']}")
    logger.info(f"Ordinate categorisations created: {stats['ordinate_cats_created']}")
    logger.info(f"Average time per table: {total_elapsed/max(stats['tables_processed'], 1):.2f}s")
    logger.info(f"Output files written to: {output_directory}")
    logger.info("=" * 80)

    return stats
