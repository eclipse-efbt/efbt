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


# Mapping of framework codes to friendly display names
FRAMEWORK_FRIENDLY_NAMES = {
    'FINREP': 'Financial Reporting',
    'COREP': 'Common Reporting',
    'AE': 'Asset Encumbrance',
    'FP': 'Funding Plans',
    'SBP': 'Supervisory Benchmarking Portfolios',
    'REM': 'Remuneration',
    'RES': 'Resolution',
    'PAY': 'Payments',
    'FINREPCOVID19': 'Financial Reporting of COVID19',
    'IF': 'Investment Firms',
    'GSII': 'Global Systemic and Important Institutions',
    'MREL': 'MREL and TLAC',
    'IMPRAC': 'Impracticability of Contractual Recognition of Bail-in',
    'ESG': 'ESG',
    'IPU': 'Intermediate Parent Undertaking',
    'PILLAR3': 'Pillar 3 Disclosures',
    'IRRBB': 'Interest Rate Risk in the Banking Book',
    'DORA': 'Digital Operational Resilience',
    'FC': 'FICO',
    'MICA': 'MICA',
}


def map_frameworks(path=None, frameworks=None, base_path="target"):
    """
    Map frameworks from ReportingFramework.csv to the target format.

    Args:
        path: Path to ReportingFramework.csv (deprecated, use base_path instead)
        frameworks: List of framework codes to filter (e.g., ['FINREP', 'COREP']).
                   If None, all frameworks are imported.
        base_path: Base directory containing CSV files (default: "target")
    """
    if path is None:
        path = os.path.join(base_path, "ReportingFramework.csv")
    df = pd.read_csv(path, dtype=str)

    if frameworks:
        df = df[df['FrameworkCode'].isin(frameworks)]

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
    # Use friendly names from mapping dictionary, fallback to FrameworkLabel if not found
    df = df.assign(
        MAINTENANCE_AGENCY_ID="EBA",
        FRAMEWORK_ID="EBA_" + df['FrameworkCode'].astype(str),
        CODE=df['FrameworkCode'].astype(str),
        NAME=df['FrameworkCode'].map(FRAMEWORK_FRIENDLY_NAMES).fillna(df['FrameworkLabel']).astype(str),
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
