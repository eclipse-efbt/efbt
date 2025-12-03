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

"""Helper function to write all dataframes to CSV files."""

import os


def write_all_csvs_to_directory(tables_df, axes_df, ordinates_df, cells_df,
                                 cell_positions_df, ordinate_cat_df, output_directory):
    """
    Helper function to write all dataframes to CSV files.

    Args:
        tables_df: DataFrame of tables
        axes_df: DataFrame of axes
        ordinates_df: DataFrame of ordinates
        cells_df: DataFrame of cells
        cell_positions_df: DataFrame of cell positions
        ordinate_cat_df: DataFrame of ordinate categorisations
        output_directory: Directory path to write CSV files to
    """
    from ..table_duplication import prepare_ordinate_item_for_csv

    tables_df.to_csv(os.path.join(output_directory, 'table.csv'), index=False)
    axes_df.to_csv(os.path.join(output_directory, 'axis.csv'), index=False)
    ordinates_df.to_csv(os.path.join(output_directory, 'axis_ordinate.csv'), index=False)
    cells_df.to_csv(os.path.join(output_directory, 'table_cell.csv'), index=False)

    # Drop ID column from cell_positions (SQLite will auto-generate)
    cell_positions_to_write = cell_positions_df.copy()
    if 'ID' in cell_positions_to_write.columns:
        cell_positions_to_write = cell_positions_to_write.drop(columns=['ID'])
    cell_positions_to_write.to_csv(os.path.join(output_directory, 'cell_position.csv'), index=False)

    # Use prepare_ordinate_item_for_csv to transform column names for database compatibility
    prepare_ordinate_item_for_csv(ordinate_cat_df).to_csv(os.path.join(output_directory, 'ordinate_item.csv'), index=False, na_rep='')
