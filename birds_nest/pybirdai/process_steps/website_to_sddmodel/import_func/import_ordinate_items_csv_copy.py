# coding=UTF-8
# Copyright (c) 2024 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Neil Mackenzie - initial API and implementation
#

"""Import ordinate items using CSV copy with backup/restore."""

from pybirdai.models.bird_meta_data_model import ORDINATE_ITEM
from pybirdai.process_steps.website_to_sddmodel.import_func.csv_copy_importer import create_instances_from_csv_copy


def import_ordinate_items_csv_copy(context):
    """
    Import ordinate items using database-native CSV import.

    Args:
        context: SDDContext containing file paths
    """
    create_instances_from_csv_copy(context, ORDINATE_ITEM)
