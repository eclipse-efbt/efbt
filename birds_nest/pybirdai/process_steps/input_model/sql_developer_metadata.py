# coding=UTF-8
# Copyright (c) 2025 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
"""Helpers for reading SQL Developer IL metadata."""

import csv
import os
from collections import Counter
from functools import lru_cache


@lru_cache(maxsize=8)
def _load_sql_developer_related_domains(file_directory):
    """Load SQL Developer domains and the related domains used by each column."""
    domains_by_id = {}
    related_domains_by_column = {}

    il_directory = os.path.join(file_directory, "il")
    columns_path = os.path.join(il_directory, "DM_Columns.csv")
    domains_path = os.path.join(il_directory, "DM_Domains.csv")

    if os.path.exists(domains_path):
        with open(domains_path, encoding="utf-8-sig") as csvfile:
            for row in csv.DictReader(csvfile):
                domain_id = (row.get("Domain_ID") or "").strip()
                if domain_id:
                    domains_by_id[domain_id] = {
                        "description": (row.get("Domain_Name") or "").strip(),
                        "synonym": (row.get("Synonyms") or "").strip(),
                    }

    if os.path.exists(columns_path):
        with open(columns_path, encoding="utf-8-sig") as csvfile:
            for row in csv.DictReader(csvfile):
                column_name = (row.get("Column_Name") or "").strip()
                domain_id = (row.get("Domain_ID") or "").strip()
                related_domain = domains_by_id.get(domain_id)
                if not column_name or not related_domain or not related_domain["description"]:
                    continue

                related_domains_by_column.setdefault(column_name, Counter())[
                    (domain_id, related_domain["description"], related_domain["synonym"])
                ] += 1

    return domains_by_id, related_domains_by_column


def _ordered_sql_developer_domains(variable_id, related_domains):
    """Order related SQL Developer domains by preference for a variable."""
    ordered_domain_ids = []
    preferred_synonyms = [f"{variable_id}_INPT", variable_id]

    for preferred_synonym in preferred_synonyms:
        for (domain_id, _, synonym), _ in related_domains.items():
            if synonym == preferred_synonym and domain_id not in ordered_domain_ids:
                ordered_domain_ids.append(domain_id)

    for (domain_id, _, _), _ in related_domains.most_common():
        if domain_id not in ordered_domain_ids:
            ordered_domain_ids.append(domain_id)

    return ordered_domain_ids


@lru_cache(maxsize=8)
def load_sql_developer_variable_descriptions(file_directory):
    """Load SQL Developer variable descriptions keyed by variable ID."""
    descriptions = {}
    domains_by_id, related_domains_by_column = _load_sql_developer_related_domains(file_directory)

    for variable_id, related_domains in related_domains_by_column.items():
        preferred_description = None
        for domain_id in _ordered_sql_developer_domains(variable_id, related_domains):
            preferred_description = domains_by_id.get(domain_id, {}).get("description")
            if preferred_description:
                break

        if preferred_description:
            descriptions[variable_id] = preferred_description

    return descriptions


@lru_cache(maxsize=8)
def load_sql_developer_member_descriptions(file_directory):
    """Load SQL Developer member descriptions keyed by variable/domain and code."""
    descriptions = {}
    _, related_domains_by_column = _load_sql_developer_related_domains(file_directory)

    il_directory = os.path.join(file_directory, "il")
    domain_avt_path = os.path.join(il_directory, "DM_Domain_AVT.csv")
    descriptions_by_domain_id = {}

    if os.path.exists(domain_avt_path):
        with open(domain_avt_path, encoding="utf-8-sig") as csvfile:
            for row in csv.DictReader(csvfile):
                domain_id = (row.get("Domain_ID") or "").strip()
                value = (row.get("Value") or "").strip()
                description = (row.get("Short_Description") or "").strip()
                if domain_id and value and description:
                    descriptions_by_domain_id.setdefault(domain_id, {})[value] = description

    for variable_id, related_domains in related_domains_by_column.items():
        ordered_domain_ids = _ordered_sql_developer_domains(variable_id, related_domains)
        member_descriptions = {}
        for domain_id in ordered_domain_ids:
            for value, description in descriptions_by_domain_id.get(domain_id, {}).items():
                member_descriptions.setdefault(value, description)
        if member_descriptions:
            descriptions[variable_id] = member_descriptions

    return descriptions
