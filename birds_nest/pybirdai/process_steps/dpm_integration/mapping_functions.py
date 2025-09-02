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
import pandas as pd
import re
import os
from collections import defaultdict

def pascal_to_upper_snake(name):
    """Convert PascalCase to UPPER_SNAKE_CASE"""
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).upper()

def clean_spaces(df):
    for col in df:
        try:
            df[col] = df[col].str.replace("\n"," ").replace('''"''',"\'")
        except:
            pass
    return df


def map_frameworks(path=os.path.join("target", "ReportingFramework.csv")):
    frameworks = pd.read_csv(path)
    framework_columns = [
        "MAINTENANCE_AGENCY_ID","FRAMEWORK_ID","NAME","CODE","DESCRIPTION","FRAMEWORK_TYPE","REPORTING_POPULATION","OTHER_LINKS","ORDER","FRAMEWORK_STATUS"

    ]
    frameworks["MAINTENANCE_AGENCY_ID"] = "EBA"
    frameworks["FRAMEWORK_ID"] = "EBA" + "_" + frameworks.FrameworkCode
    frameworks["CODE"] = frameworks.FrameworkCode
    frameworks["NAME"] = frameworks.FrameworkLabel
    frameworks["FRAMEWORK_STATUS"] = "PUBLISHED"
    for col in framework_columns:
        if col not in frameworks:
            frameworks[col] = ""
    frameworks.drop(axis=1, labels=["ConceptID"], inplace=True)
    frameworks = frameworks.set_index("FrameworkID").drop(
        axis=1, labels=["FrameworkCode", "FrameworkLabel"]
    )

    framework_id_mapping = frameworks["FRAMEWORK_ID"].to_dict()

    frameworks = frameworks.loc[
        :,
        framework_columns
    ]

    return frameworks, framework_id_mapping


def map_domains(path=os.path.join("target", "Domain.csv")):
    domains = pd.read_csv(path)

    # Transform column names to UPPER_SNAKE_CASE
    column_mapping = {col: pascal_to_upper_snake(col) for col in domains.columns}
    domains = domains.rename(columns=column_mapping)

    # Set maintenance agency ID and create new domain ID
    domains["MAINTENANCE_AGENCY_ID"] = "EBA"
    domains["NEW_DOMAIN_ID"] = "EBA_" + domains["DOMAIN_CODE"]

    # Create ID mapping for return
    id_mapping = dict(zip(domains["DOMAIN_ID"].copy(), domains["NEW_DOMAIN_ID"].copy()))

    domains.drop(axis=1,labels=["DOMAIN_ID"],inplace=True)

    domains.rename(columns={
        "NEW_DOMAIN_ID":"DOMAIN_ID",
        "DOMAIN_CODE":"CODE",
        "DOMAIN_LABEL":"NAME",
        "DOMAIN_DESCRIPTION":"DESCRIPTION",
        "DATA_TYPE_ID":"DATA_TYPE",
    },inplace=True)

    domains = clean_spaces(domains)

    domains["FACET_ID"] = False
    domains["IS_REFERENCE"] = False
    domains["IS_ENUMERATED"] = False

    domains = domains.loc[
        :,
        [
            "MAINTENANCE_AGENCY_ID","DOMAIN_ID","NAME","IS_ENUMERATED","DESCRIPTION","DATA_TYPE","CODE","FACET_ID","IS_REFERENCE"
        ]
    ]

    return domains, id_mapping


def map_members(path=os.path.join("target", "Member.csv"), domain_id_map: dict = {}):
    members = pd.read_csv(path)

    # Transform column names to UPPER_SNAKE_CASE
    column_mapping = {col: pascal_to_upper_snake(col) for col in members.columns}
    members = members.rename(columns=column_mapping)

    # Set maintenance agency ID and create new member ID
    members["MAINTENANCE_AGENCY_ID"] = "EBA"
    members["NEW_MEMBER_ID"] = members["DOMAIN_ID"].apply(domain_id_map.get) + "_EBA_" + members["MEMBER_CODE"]
    members["DOMAIN_ID"] = members["DOMAIN_ID"].apply(domain_id_map.get)

    # Create ID mapping for return
    id_mapping = dict(zip(members["MEMBER_ID"], members["NEW_MEMBER_ID"]))

    members.drop(
        axis=1, labels=["MEMBER_ID"],inplace=True
    )
    members.rename(columns={
        "NEW_MEMBER_ID":"MEMBER_ID",
        "MEMBER_CODE":"CODE",
        "MEMBER_LABEL":"NAME",
        "DOMAIN_ID":"DOMAIN_ID",
        "MEMBER_DESCRIPTION":"DESCRIPTION",
    },inplace=True)

    members = members[~members.MEMBER_ID.isna()].copy()

    members = members.loc[
        :,
        ["MAINTENANCE_AGENCY_ID","MEMBER_ID","CODE","NAME","DOMAIN_ID","DESCRIPTION"]
    ]

    members = clean_spaces(members)

    return members, id_mapping


def map_dimensions(path=os.path.join("target", "Dimension.csv"), domain_id_map: dict = {}):
    dimensions = pd.read_csv(path)

    # Transform column names to UPPER_SNAKE_CASE
    column_mapping = {col: pascal_to_upper_snake(col) for col in dimensions.columns}
    dimensions = dimensions.rename(columns=column_mapping)

    # Set maintenance agency ID and create new dimension ID
    dimensions["MAINTENANCE_AGENCY_ID"] = "EBA"
    dimensions["NEW_DIMENSION_ID"] = "EBA_" + dimensions["DIMENSION_CODE"]
    dimensions["DOMAIN_ID"] = dimensions["DOMAIN_ID"].apply(domain_id_map.get)

    # Create ID mapping for return
    id_mapping = dict(zip(dimensions["DIMENSION_ID"], dimensions["NEW_DIMENSION_ID"]))

    dimensions.rename(columns={
        "MAINTENANCE_AGENCY_ID": "MAINTENANCE_AGENCY_ID",
        "NEW_DIMENSION_ID": "VARIABLE_ID",
        "DIMENSION_CODE": "CODE",
        "DIMENSION_LABEL": "NAME",
        "DOMAIN_ID": "DOMAIN_ID",
        "DIMENSION_DESCRIPTION": "DESCRIPTION"
    },inplace=True)

    dimensions["PRIMARY_CONCEPT"] = ""
    dimensions["IS_DECOMPOSED"] = False

    dimensions = dimensions.loc[:,
        ["MAINTENANCE_AGENCY_ID","VARIABLE_ID","CODE","NAME","DOMAIN_ID","DESCRIPTION","PRIMARY_CONCEPT","IS_DECOMPOSED","IS_IMPLIED_IF_NOT_EXPLICITLY_MODELLED"]
    ]
    dimensions["IS_IMPLIED_IF_NOT_EXPLICITLY_MODELLED"] = dimensions["IS_IMPLIED_IF_NOT_EXPLICITLY_MODELLED"].astype(bool)

    dimensions = clean_spaces(dimensions)

    return dimensions, id_mapping

def load_template_to_framework_mapping(base_path="target"):
    template_group = pd.read_csv(os.path.join(base_path, "TemplateGroup.csv"))[
        ["FrameworkID", "TemplateGroupID"]
    ].copy()
    template_group_template = pd.read_csv(
        os.path.join(base_path, "TemplateGroupTemplate.csv")
    )[["TemplateID", "TemplateGroupID"]].copy()
    merged = pd.merge(template_group, template_group_template, on="TemplateGroupID")[
        ["TemplateID", "FrameworkID"]
    ]
    column_mapping = {col: pascal_to_upper_snake(col) for col in merged.columns}
    merged = merged.rename(columns=column_mapping)
    result = merged.set_index("TEMPLATE_ID").to_dict()
    return result["FRAMEWORK_ID"]

def load_taxonomy_version_to_table_mapping(base_path="target"):
    taxonomy_to_table_version = pd.read_csv(os.path.join(base_path, "TaxonomyTableVersion.csv"))[
        ["TaxonomyID", "TableVID"]
    ].copy()
    taxonomy_to_package_version = pd.read_csv(os.path.join(base_path, "Taxonomy.csv"))[
        ["TaxonomyID", "DpmPackageCode"]
    ].copy()
    merged = pd.merge(taxonomy_to_table_version, taxonomy_to_package_version, on="TaxonomyID")[
        ["TableVID", "DpmPackageCode"]
    ]
    column_mapping = {col: pascal_to_upper_snake(col) for col in merged.columns}
    merged = merged.rename(columns=column_mapping)
    result = merged.set_index("TABLE_VID").to_dict()
    return result["DPM_PACKAGE_CODE"]

def map_tables(path=os.path.join("target", "Table.csv"), framework_id_map: dict = {}):
    tables = pd.read_csv(path).drop(axis=1,labels=["ConceptID"])
    # Get directory and create proper path for TableVersion.csv
    path_dir = os.path.dirname(path)
    tables_versions_path = os.path.join(path_dir, "TableVersion.csv")
    tables_versions = pd.read_csv(tables_versions_path).drop(axis=1,labels=["ConceptID"])
    tables = pd.merge(tables, tables_versions, on="TableID")
    template_to_framework_mapping = load_template_to_framework_mapping()
    table_to_taxonomy_mapping = load_taxonomy_version_to_table_mapping()

    # Transform column names to UPPER_SNAKE_CASE
    column_mapping = {col: pascal_to_upper_snake(col) for col in tables.columns}
    tables = tables.rename(columns=column_mapping)

    # Set maintenance agency ID and create new table ID
    tables["MAINTENANCE_AGENCY_ID"] = "EBA"
    tables["NEW_TABLE_ID"] = (
        tables["TEMPLATE_ID"].apply(
            lambda x: framework_id_map.get(template_to_framework_mapping.get(x, x), x)
        )
        + "_EBA_"
        + tables["ORIGINAL_TABLE_CODE"].str.split().str.join("_")
        + "_"
        + tables["TABLE_VID"].apply(table_to_taxonomy_mapping.get)
    ).str.replace(".","_")

    tables.drop(columns=["TABLE_ID"], inplace=True)

    # Create ID mapping for return
    id_mapping = dict(zip(tables["TABLE_VID"], tables["NEW_TABLE_ID"]))

    tables.rename(columns={
        "NEW_TABLE_ID":"TABLE_ID",
        "ORIGINAL_TABLE_LABEL":"DESCRIPTION",
        "FROM_DATE":"VALID_FROM",
        "TO_DATE":"VALID_TO"
    },inplace=True)

    tables["NAME"] = tables["ORIGINAL_TABLE_CODE"].copy()
    tables["CODE"] = tables["ORIGINAL_TABLE_CODE"].str.split().str.join("_")
    tables["VERSION"] = tables["TEMPLATE_ID"].apply(
        lambda x: framework_id_map.get(template_to_framework_mapping.get(x, x), x)
    ).str.replace("EBA_","") + " " + tables["TABLE_VID"].apply(table_to_taxonomy_mapping.get)

    tables = tables.loc[
        :,
        [
            "TABLE_ID","NAME","CODE","DESCRIPTION","MAINTENANCE_AGENCY_ID","VERSION","VALID_FROM","VALID_TO"

        ]
    ]

    return tables, id_mapping


def map_axis(path=os.path.join("target", "Axis.csv"), table_map:dict = {}):
    orientation_id_map = {"X":"1","Y":"2","Z":"3","0":"0"}
    axes = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in axes.columns}
    axes = axes.rename(columns=column_mapping)
    axes["MAINTENANCE_AGENCY_ID"] = "EBA"
    axes["NEW_AXIS_ID"] = axes["TABLE_VID"].apply(table_map.get) + "_" + axes["AXIS_ORIENTATION"].apply(lambda x : orientation_id_map.get(x,""))

    id_mapping = dict(zip(axes["AXIS_ID"], axes["NEW_AXIS_ID"]))

    axes.drop(
        axis=1,
        labels=["AXIS_ID"],inplace=True
    )

    axes.rename(
        columns = {
            "NEW_AXIS_ID":"AXIS_ID",
            "AXIS_LABEL":"NAME",
            "AXIS_ORIENTATION":"ORIENTATION"
        },inplace=True
    )

    axes["CODE"] = axes["AXIS_ID"].str.rsplit("_").apply(lambda x : x[-4:-2] + [x[-1]]).str.join("_")

    axes["TABLE_ID"] = axes["TABLE_VID"].apply(table_map.get)

    axes["ORDER"] = axes["ORIENTATION"].apply(lambda x : orientation_id_map.get(x,""))

    axes["DESCRIPTION"] = ""

    axes["IS_OPEN_AXIS"] = axes["IS_OPEN_AXIS"].astype(bool)

    axes = axes.loc[
        :,
        [
            "AXIS_ID","CODE","ORIENTATION","ORDER","NAME","DESCRIPTION","TABLE_ID","IS_OPEN_AXIS"
        ]
    ]



    return axes, id_mapping


def map_axis_ordinate(path=os.path.join("target", "AxisOrdinate.csv"),axis_map:dict = {}):
    types = defaultdict(lambda: str, OrdinateID="int", OrdinateCode="str", AxisID="int")
    ordinates = pd.read_csv(path, dtype=types)
    column_mapping = {col: pascal_to_upper_snake(col) for col in ordinates.columns}
    ordinates = ordinates.rename(columns=column_mapping)
    ordinates["MAINTENANCE_AGENCY_ID"] = "EBA"
    ordinates["NEW_ORDINATE_ID"] = ordinates["AXIS_ID"].apply(axis_map.get) + "_" + ordinates["ORDINATE_CODE"].str.strip()

    id_mapping = dict(zip(ordinates["ORDINATE_ID"].copy(), ordinates["NEW_ORDINATE_ID"].copy()))


    ordinates.drop(
        axis=1,
        labels=["ORDINATE_ID"],
        inplace=True
    )

    ordinates.rename(
        columns={
            "NEW_ORDINATE_ID":"AXIS_ORDINATE_ID",
            "ORDINATE_CODE":"CODE",
            "PARENT_ORDINATE_ID":"PARENT_AXIS_ORDINATE_ID",
            "ORDINATE_LABEL":"NAME",
            "ORDINATE_CODE":"CODE"
        },
        inplace=True
    )

    ordinates["PARENT_AXIS_ORDINATE_ID"] = ordinates["PARENT_AXIS_ORDINATE_ID"].apply(id_mapping.get)

    ordinates["DESCRIPTION"] = ""

    ordinates["AXIS_ID"] = ordinates["AXIS_ID"].apply(axis_map.get)

    ordinates["PATH"] = ordinates["PATH"].apply(lambda x : ".".join(
        list(map(lambda _: id_mapping.get(int(_),_) if _ else _,x.split(".")))
    ))

    ordinates = ordinates.loc[
        :,
        [
            "AXIS_ORDINATE_ID","IS_ABSTRACT_HEADER","CODE","ORDER","LEVEL","PATH","AXIS_ID","PARENT_AXIS_ORDINATE_ID","NAME","DESCRIPTION"
        ]
    ]

    return ordinates, id_mapping


def map_table_cell(path=os.path.join("target", "TableCell.csv"), table_map:dict = {}, dp_map:dict = {}):
    cells = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in cells.columns}
    cells = cells.rename(columns=column_mapping)
    cells["MAINTENANCE_AGENCY_ID"] = "EBA"
    cells["NEW_CELL_ID"] = "EBA_" + cells["CELL_ID"].astype(str)
    cells["TABLE_VID"] = cells["TABLE_VID"].apply(table_map.get)

    cells["IS_SHADED"] = cells["IS_SHADED"].astype(bool)

    if not dp_map:
        cells["DATA_POINT_VID"] = ""
    if dp_map:
        cells["DATA_POINT_VID"] = cells["DATA_POINT_VID"].astype(str)

        cells.loc[:,"DATA_POINT_VID"] = cells["DATA_POINT_VID"].str.replace(".0","").replace("nan","").apply(lambda x : dp_map.get(x,x))

    id_mapping = dict(zip(cells["CELL_ID"], cells["NEW_CELL_ID"]))

    cells.drop(
        axis=1,
        labels=["CELL_ID"],
        inplace=True
    )

    cells.rename(
        columns={
            "NEW_CELL_ID":"CELL_ID",
            "TABLE_VID":"TABLE_ID",
            "DATA_POINT_VID":"TABLE_CELL_COMBINATION_ID"
        },inplace=True
    )

    cells["SYSTEM_DATA_CODE"] = ""
    cells["NAME"] = cells["CELL_ID"]

    cells = cells.loc[
        :,
        [
            "CELL_ID","IS_SHADED","TABLE_CELL_COMBINATION_ID","SYSTEM_DATA_CODE","NAME","TABLE_ID"
        ]
    ]

    return cells, id_mapping


def map_cell_position(path=os.path.join("target", "CellPosition.csv"),cell_map:dict={},ordinate_map:dict={},start_index_after_last:bool=False):
    data = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in data.columns}
    data = data.rename(columns=column_mapping)
    data["CELL_ID"] = data["CELL_ID"].apply(cell_map.get)
    data["ORDINATE_ID"] = data["ORDINATE_ID"].apply(ordinate_map.get)
    
    if start_index_after_last and "ID" in data.columns and not data.empty:
        start_idx = data["ID"].max() + 1 if pd.notnull(data["ID"].max()) else 0
        data.reset_index(drop=True, inplace=True)
        data["ID"] = range(start_idx, start_idx + len(data))
    else:
        if "ID" in data.columns:
            data.drop(columns=["ID"], inplace=True)
        data.reset_index(inplace=True)
        data.rename(columns={"index": "ID"},inplace=True)
    
    return data, {}

def map_datapoint_version(path=os.path.join("target", "DataPointVersion.csv"),context_map:dict={},context_data:pd.DataFrame=pd.DataFrame(),dimension_map:dict={},member_map:dict={}):
    types = defaultdict(lambda: str, ContextID="str")
    dpv = pd.read_csv(path,dtype=types)
    column_mapping = {col: pascal_to_upper_snake(col) for col in dpv.columns}
    dpv = dpv.rename(columns=column_mapping)
    dpv["MAINTENANCE_AGENCY_ID"] = "EBA"
    dpv["NEW_DATA_POINT_VID"] = "EBA_" + dpv["DATA_POINT_VID"].astype(str)
    dp_items = pd.merge(dpv[["NEW_DATA_POINT_VID","CONTEXT_ID"]].copy(),context_data,on="CONTEXT_ID").drop(axis=1,labels=["CONTEXT_ID"])
    id_mapping = dict(zip(dpv["DATA_POINT_VID"], dpv["NEW_DATA_POINT_VID"]))

    def is_number(char):
        if char in "0123456789":
            return True
        return False

    def compute_code(string):
        new_key = ""
        value = ""
        previous_char = ""
        key_value = dict()
        is_new_key = True
        is_new_value = False
        for idx,char in enumerate(string):
            if previous_char.isnumeric() and char.isalpha():
                new_key = ""
                is_new_key = True
                is_new_value = False

            if char.isnumeric() and previous_char.isalpha():
                is_new_value = True
                is_new_key = False

            if is_new_key:
                new_key += char

            if is_new_value:
                if "EBA_"+new_key not in key_value:
                    key_value["EBA_"+new_key] = ""
                key_value["EBA_"+new_key] += char

            previous_char = char

        return "|".join(f"{key}({member_map.get(int(value))})" for key, value in key_value.items())

    dpv.rename(columns={
        "NEW_DATA_POINT_VID":"COMBINATION_ID",
        "DATA_POINT_VID":"CODE",
        "FROM_DATE":"VALID_FROM",
        "TO_DATE":"VALID_TO",
        "CATEGORISATION_KEY":"NAME"
    },
        inplace=True)

    dpv["NAME"] = dpv["NAME"].apply(compute_code)
    dpv["VERSION"] = ""

    dp_items.rename(columns={
        "NEW_DATA_POINT_VID":"COMBINATION_ID",
        "DIMENSION_ID": "VARIABLE_ID",
        "MEMBER_ID": "MEMBER_ID"
    },inplace=True)

    dp_items["VARIABLE_SET"] = ""
    dp_items["SUBDOMAIN_ID"] = ""

    dp_items = dp_items.loc[
        :,
        ["COMBINATION_ID","VARIABLE_ID","MEMBER_ID","VARIABLE_SET","SUBDOMAIN_ID"]
]

    dpv = dpv.loc[
        :,
        ["COMBINATION_ID","CODE","NAME","MAINTENANCE_AGENCY_ID","VERSION","VALID_FROM","VALID_TO"]
]

    return (dpv,dp_items), id_mapping

def map_context_definition(path=os.path.join("target", "ContextDefinition.csv"),dimension_map:dict={},member_map:dict={}):
    types = defaultdict(lambda: str, ContextID="str")
    data = pd.read_csv(path,dtype=types)
    column_mapping = {col: pascal_to_upper_snake(col) for col in data.columns}
    data = data.rename(columns=column_mapping)
    data["MAINTENANCE_AGENCY_ID"] = "EBA"
    data["DIMENSION_ID"] = data["DIMENSION_ID"].astype(int).apply(dimension_map.get)
    data["MEMBER_ID"] = data["MEMBER_ID"].astype(int).apply(member_map.get)

    return data, {}

def map_hierarchy(path=os.path.join("target", "Hierarchy.csv"),domain_id_map:dict={}):
    hierarchies = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in hierarchies.columns}
    hierarchies = hierarchies.rename(columns=column_mapping)
    hierarchies["MAINTENANCE_AGENCY_ID"] = "EBA"
    hierarchies["NEW_HIERARCHY_ID"] = "EBA_" + hierarchies["HIERARCHY_CODE"]
    hierarchies["DOMAIN_ID"] = hierarchies["DOMAIN_ID"].apply(domain_id_map.get)

    # generate id map
    id_mapping = dict(zip(hierarchies["HIERARCHY_ID"], hierarchies["NEW_HIERARCHY_ID"]))

    hierarchies.rename(
        columns={
            "NEW_HIERARCHY_ID":"MEMBER_HIERARCHY_ID",
            "HIERARCHY_CODE":"CODE","HIERARCHY_LABEL":"NAME",
            "HIERARCHY_DESCRIPTION":"DESCRIPTION"
        },inplace=True
    )

    hierarchies["IS_MAIN_HIERARCHY"] = False

    hierarchies = hierarchies.loc[
        :,
        [
        "MAINTENANCE_AGENCY_ID","MEMBER_HIERARCHY_ID","CODE","DOMAIN_ID","NAME","DESCRIPTION","IS_MAIN_HIERARCHY"
        ]
    ]



    return hierarchies, id_mapping


def map_hierarchy_node(path=os.path.join("target", "HierarchyNode.csv"), hierarchy_map:dict={}, member_map:dict={}):
    data = pd.read_csv(path)
    data["ParentMemberID"] = data["ParentMemberID"].fillna(0).astype(int)
    data["MemberID"] = data["MemberID"].fillna(0).astype(int)
    column_mapping = {col: pascal_to_upper_snake(col) for col in data.columns}
    data = data.rename(columns=column_mapping)
    data["MEMBER_ID"] = data["MEMBER_ID"].apply(member_map.get)
    data["PARENT_MEMBER_ID"] = data["PARENT_MEMBER_ID"].apply(member_map.get)
    data["PARENT_HIERARCHY_ID"] = data["PARENT_HIERARCHY_ID"].apply(hierarchy_map.get)
    data["HIERARCHY_ID"] = data["HIERARCHY_ID"].apply(hierarchy_map.get)
    data["MAINTENANCE_AGENCY_ID"] = "EBA"

    data.rename(columns={
        "HIERARCHY_ID":"MEMBER_HIERARCHY_ID",
        "COMPARISON_OPERATOR":"COMPARATOR",
        "UNARY_OPERATOR":"OPERATOR"
    },inplace=True)

    data["VALID_FROM"] = "1900-01-01"
    data["VALID_TO"] = "9999-12-31"

    data.COMPARATOR = data.COMPARATOR.str.strip()
    data.OPERATOR = data.OPERATOR.str.strip()

    data.loc[
        data["COMPARATOR"].isna() & data["OPERATOR"].isna(),
        "COMPARATOR"
    ] = ">="

    data = data.loc[
        :,
        [
            "MEMBER_HIERARCHY_ID","MEMBER_ID","LEVEL","PARENT_MEMBER_ID","COMPARATOR","OPERATOR","VALID_FROM","VALID_TO"
        ]
    ]

    return data, {}

def traceback_restrictions(path=os.path.join("target", "OpenMemberRestriction.csv")):
    restriction_df = pd.read_csv(path)
    cols = ("Restriction" + restriction_df.columns).tolist()
    cols[0] = "RestrictionID"
    restriction_df.columns = cols
    return restriction_df

def map_ordinate_categorisation(path=os.path.join("target", "OrdinateCategorisation.csv"), member_map:dict={}, dimension_map:dict={}, ordinate_map:dict={}, hierarchy_map:dict={}, start_index_after_last:bool=False):
    data = pd.read_csv(path)
    restrictions = traceback_restrictions()
    data = pd.merge(data,restrictions,on="RestrictionID"
        ,how="left"
    )
    column_mapping = {col: pascal_to_upper_snake(col) for col in data.columns}
    data = data.rename(columns=column_mapping)
    data["MAINTENANCE_AGENCY_ID"] = "EBA"
    data["MEMBER_ID"] = data["MEMBER_ID"].apply(lambda x : member_map.get(x,x))
    data["DIMENSION_ID"] = data["DIMENSION_ID"].apply(dimension_map.get)
    data["ORDINATE_ID"] = data["ORDINATE_ID"].apply(ordinate_map.get)
    data["RESTRICTION_HIERARCHY_ID"] = data["RESTRICTION_HIERARCHY_ID"].apply(lambda x : hierarchy_map.get(x,x))
    data["RESTRICTION_MEMBER_ID"] = data["RESTRICTION_MEMBER_ID"].apply(lambda x : member_map.get(x,x))



    data.rename(columns={
        "ORDINATE_ID":"AXIS_ORDINATE_ID",
        "DIMENSION_ID":"VARIABLE_ID",
        "MEMBER_ID":"MEMBER_ID",
        "RESTRICTION_HIERARCHY_ID":"MEMBER_HIERARCHY_ID",
        "RESTRICTION_MEMBER_ID":"STARTING_MEMBER_ID",
        "RESTRICTION_MEMBER_INCLUDED":"IS_STARTING_MEMBER_INCLUDED"
    },inplace=True)



    # to be fixed in future with restriction parsing
    data["IS_STARTING_MEMBER_INCLUDED"] = data["IS_STARTING_MEMBER_INCLUDED"].astype(bool)
    data["MEMBER_HIERARCHY_VALID_FROM"] = ""
    data.loc[data.STARTING_MEMBER_ID.isna(),"IS_STARTING_MEMBER_INCLUDED"] = False

    if start_index_after_last and "ID" in data.columns and not data.empty:
        start_idx = data["ID"].max() + 1 if pd.notnull(data["ID"].max()) else 0
        data.reset_index(drop=True, inplace=True)
        data["ID"] = range(start_idx, start_idx + len(data))
    else:
        if "ID" in data.columns:
            data.drop(columns=["ID"], inplace=True)
        data.reset_index(inplace=True)
        data.rename(columns={"index": "ID"},inplace=True)

    data = data.loc[
        :,
        [
            "ID",
            "MEMBER_HIERARCHY_VALID_FROM",
            "IS_STARTING_MEMBER_INCLUDED",
            "AXIS_ORDINATE_ID",
            "VARIABLE_ID",
            "MEMBER_ID",
            "MEMBER_HIERARCHY_ID",
            "STARTING_MEMBER_ID"
        ]
    ]

    return data, {}
