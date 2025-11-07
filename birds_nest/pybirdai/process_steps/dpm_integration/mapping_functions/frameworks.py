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
from pybirdai.process_steps.dpm_integration.mapping_functions.utils import clean_spaces_df


def map_frameworks(path=os.path.join("target", "ReportingFramework.csv")):
    """Map frameworks from ReportingFramework.csv to the target format"""
    df = pd.read_csv(path, dtype=str)

    framework_columns = [
        "MAINTENANCE_AGENCY_ID", "FRAMEWORK_ID", "NAME", "CODE", "DESCRIPTION",
        "FRAMEWORK_TYPE", "REPORTING_POPULATION", "OTHER_LINKS", "ORDER", "FRAMEWORK_STATUS"
    ]

    # Create ID mapping before transforming
    framework_id_mapping = dict(zip(
        df['FrameworkID'].astype(str),
        "EBA_" + df['FrameworkCode'].astype(str)
    ))

    # Build transformed DataFrame
    df = df.assign(
        MAINTENANCE_AGENCY_ID="EBA",
        FRAMEWORK_ID="EBA_" + df['FrameworkCode'].astype(str),
        CODE=df['FrameworkCode'].astype(str),
        NAME=df['FrameworkLabel'].astype(str),
        FRAMEWORK_STATUS="PUBLISHED"
    )

    # Add missing columns as empty strings
    for col in framework_columns:
        if col not in df.columns:
            df[col] = ""

    # Select and clean
    df = df[framework_columns]
    df = clean_spaces_df(df)

    return df, framework_id_mapping
