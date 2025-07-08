import pandas as pd
import re
import os
from collections import defaultdict

def pascal_to_upper_snake(name):
    """Convert PascalCase to UPPER_SNAKE_CASE"""
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).upper()


def map_frameworks(path="target/ReportingFramework.csv"):
    frameworks = pd.read_csv(path)
    framework_columns = "MAINTENANCE_AGENCY_ID,FRAMEWORK_ID,NAME,CODE,DESCRIPTION,FRAMEWORK_TYPE,REPORTING_POPULATION,OTHER_LINKS,ORDER,FRAMEWORK_STATUS".split(
        ","
    )
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
    return frameworks, frameworks["FRAMEWORK_ID"].to_dict()


def map_domains(path="target/Domain.csv"):
    domains = pd.read_csv(path)

    # Transform column names to UPPER_SNAKE_CASE
    column_mapping = {col: pascal_to_upper_snake(col) for col in domains.columns}
    domains = domains.rename(columns=column_mapping)

    # Set maintenance agency ID and create new domain ID
    domains["MAINTENANCE_AGENCY_ID"] = "EBA"
    domains["NEW_DOMAIN_ID"] = "EBA_" + domains["DOMAIN_CODE"]

    # Create ID mapping for return
    id_mapping = dict(zip(domains["DOMAIN_ID"], domains["NEW_DOMAIN_ID"]))

    return domains, id_mapping


def map_members(path="target/Member.csv", domain_id_map: dict = {}):
    members = pd.read_csv(path)

    # Transform column names to UPPER_SNAKE_CASE
    column_mapping = {col: pascal_to_upper_snake(col) for col in members.columns}
    members = members.rename(columns=column_mapping)

    # Set maintenance agency ID and create new member ID
    members["MAINTENANCE_AGENCY_ID"] = "EBA"
    members["NEW_MEMBER_ID"] = "EBA_" + members["MEMBER_CODE"]
    members["DOMAIN_ID"] = members["DOMAIN_ID"].apply(domain_id_map.get)

    # Create ID mapping for return
    id_mapping = dict(zip(members["MEMBER_ID"], members["NEW_MEMBER_ID"]))

    return members, id_mapping


def map_dimensions(path="target/Dimension.csv", domain_id_map: dict = {}):
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

def map_tables(path="target/Table.csv", framework_id_map: dict = {}):
    tables = pd.read_csv(path).drop(axis=1,labels=["ConceptID"])
    tables_versions = pd.read_csv(path.replace("Table.csv","TableVersion.csv")).drop(axis=1,labels=["ConceptID"])
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
    )

    # Create ID mapping for return
    id_mapping = dict(zip(tables["TABLE_VID"], tables["NEW_TABLE_ID"]))

    return tables, id_mapping


def map_axis(path="target/Axis.csv", table_map:dict = {}):
    orientation_id_map = {"X":"1","Y":"2","Z":"3","0":"0"}
    axes = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in axes.columns}
    axes = axes.rename(columns=column_mapping)
    axes["MAINTENANCE_AGENCY_ID"] = "EBA"
    axes["NEW_AXIS_ID"] = axes["TABLE_VID"].apply(table_map.get) + "_" + axes["AXIS_ORIENTATION"].apply(lambda x : orientation_id_map.get(x,""))
    id_mapping = dict(zip(axes["AXIS_ID"], axes["NEW_AXIS_ID"]))
    return axes, id_mapping


def map_axis_ordinate(path="target/AxisOrdinate.csv",axis_map:dict = {}):
    types = defaultdict(lambda: str, OrdinateID="int", OrdinateCode="str", AxisID="int")
    ordinates = pd.read_csv(path, dtype=types)
    column_mapping = {col: pascal_to_upper_snake(col) for col in ordinates.columns}
    ordinates = ordinates.rename(columns=column_mapping)
    ordinates["MAINTENANCE_AGENCY_ID"] = "EBA"
    ordinates["NEW_ORDINATE_ID"] = ordinates["AXIS_ID"].apply(axis_map.get) + "_" + ordinates["ORDINATE_CODE"].str.strip()
    id_mapping = dict(zip(ordinates["ORDINATE_ID"], ordinates["NEW_ORDINATE_ID"]))
    return ordinates, id_mapping


def map_table_cell(path="target/TableCell.csv", table_map:dict = {}, dp_map:dict = {}):
    cells = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in cells.columns}
    cells = cells.rename(columns=column_mapping)
    cells["MAINTENANCE_AGENCY_ID"] = "EBA"
    cells["NEW_CELL_ID"] = "EBA_" + cells["CELL_ID"].astype(str)
    cells["TABLE_VID"] = cells["TABLE_VID"].apply(table_map.get)
    if dp_map:
        cells["DATAPOINT_VID"] = cells["DATAPOINT_VID"].apply(dp_map.get)
    id_mapping = dict(zip(cells["CELL_ID"], cells["NEW_CELL_ID"]))
    return cells, id_mapping


def map_cell_position(path="target/CellPosition.csv",cell_map:dict={},ordinate_map:dict={}):
    data = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in data.columns}
    data = data.rename(columns=column_mapping)
    data["MAINTENANCE_AGENCY_ID"] = "EBA"
    data["CELL_ID"] = data["CELL_ID"].apply(cell_map.get)
    data["ORDINATE_ID"] = data["ORDINATE_ID"].apply(ordinate_map.get)
    return data, {}

def map_datapoint_version(path="target/DataPointVersion.csv",context_map:dict={},context_data:pd.DataFrame=pd.DataFrame()):
    types = defaultdict(lambda: str, ContextID="str")
    dpv = pd.read_csv(path,dtype=types)
    column_mapping = {col: pascal_to_upper_snake(col) for col in dpv.columns}
    dpv = dpv.rename(columns=column_mapping)
    dpv["MAINTENANCE_AGENCY_ID"] = "EBA"
    dpv["NEW_DATA_POINT_VID"] = "EBA_" + dpv["DATA_POINT_VID"].astype(str)
    dp_items = pd.merge(dpv[["NEW_DATA_POINT_VID","CONTEXT_ID"]].copy(),context_data,on="CONTEXT_ID").drop(axis=1,labels=["CONTEXT_ID"])
    id_mapping = dict(zip(dpv["DATA_POINT_VID"], dpv["NEW_DATA_POINT_VID"]))
    return (dpv,dp_items), id_mapping

def map_context_definition(path="target/ContextDefinition.csv",dimension_map:dict={},member_map:dict={}):
    types = defaultdict(lambda: str, ContextID="str")
    data = pd.read_csv(path,dtype=types)
    column_mapping = {col: pascal_to_upper_snake(col) for col in data.columns}
    data = data.rename(columns=column_mapping)
    data["MAINTENANCE_AGENCY_ID"] = "EBA"
    data["DIMENSION_ID"] = data["DIMENSION_ID"].astype(int).apply(dimension_map.get)
    data["MEMBER_ID"] = data["MEMBER_ID"].astype(int).apply(member_map.get)
    return data, {}

def map_hierarchy(path="target/Hierarchy.csv",domain_id_map:dict={}):
    hierarchies = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in hierarchies.columns}
    hierarchies = hierarchies.rename(columns=column_mapping)
    hierarchies["MAINTENANCE_AGENCY_ID"] = "EBA"
    hierarchies["NEW_HIERARCHY_ID"] = "EBA_" + hierarchies["HIERARCHY_CODE"]
    hierarchies["DOMAIN_ID"] = hierarchies["DOMAIN_ID"].apply(domain_id_map.get)
    id_mapping = dict(zip(hierarchies["HIERARCHY_ID"], hierarchies["NEW_HIERARCHY_ID"]))
    return hierarchies, id_mapping


def map_hierarchy_node(path="target/HierarchyNode.csv", hierarchy_map:dict={}, member_map:dict={}):
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
    return data, {}

"""
----------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------
NOT IMPLEMENTED / REVIEWED YET
----------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------
"""

def map_metrics_for_datapoint():

    def map_metrics(path="target/Metric.csv"):
        metrics = pd.read_csv(path)

        # Transform column names to UPPER_SNAKE_CASE
        column_mapping = {col: pascal_to_upper_snake(col) for col in metrics.columns}
        metrics = metrics.rename(columns=column_mapping)

        # Set maintenance agency ID and create new metric ID
        metrics["MAINTENANCE_AGENCY_ID"] = "EBA"
        metrics["NEW_METRIC_ID"] = "EBA_" + metrics["METRIC_CODE"]

        # Create ID mapping for return
        id_mapping = dict(zip(metrics["METRIC_ID"], metrics["NEW_METRIC_ID"]))

        return metrics, id_mapping

    def map_metric_variable(path="target/MetricVariable.csv"):
        data = pd.read_csv(path)
        column_mapping = {col: pascal_to_upper_snake(col) for col in data.columns}
        data = data.rename(columns=column_mapping)
        data["MAINTENANCE_AGENCY_ID"] = "EBA"
        return data, {}

    def map_expression(path="target/Expression.csv"):
        expressions = pd.read_csv(path)
        column_mapping = {col: pascal_to_upper_snake(col) for col in expressions.columns}
        expressions = expressions.rename(columns=column_mapping)
        expressions["MAINTENANCE_AGENCY_ID"] = "EBA"
        expressions["NEW_EXPRESSION_ID"] = "EBA_EXPR_" + expressions[
            "EXPRESSION_ID"
        ].astype(str)
        id_mapping = dict(
            zip(expressions["EXPRESSION_ID"], expressions["NEW_EXPRESSION_ID"])
        )
        return expressions, id_mapping

def map_ordinate_variable(path="target/OrdinateVariable.csv",ordinate_map:dict = {}):
    data = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in data.columns}
    data = data.rename(columns=column_mapping)
    data["MAINTENANCE_AGENCY_ID"] = "EBA"
    return data, {}

def map_ordinate_categorisation(path="target/OrdinateCategorisation.csv"):
    data = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in data.columns}
    data = data.rename(columns=column_mapping)
    data["MAINTENANCE_AGENCY_ID"] = "EBA"
    return data, {}


def map_reference_categorisation(path="target/ReferenceCategorisation.csv"):
    data = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in data.columns}
    data = data.rename(columns=column_mapping)
    data["MAINTENANCE_AGENCY_ID"] = "EBA"
    return data, {}




def map_sum_of_many_ordinates(path="target/SumOfManyOrdinates.csv"):
    data = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in data.columns}
    data = data.rename(columns=column_mapping)
    data["MAINTENANCE_AGENCY_ID"] = "EBA"
    return data, {}





def map_variable_of_expression(path="target/VariableOfExpression.csv"):
    data = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in data.columns}
    data = data.rename(columns=column_mapping)
    data["MAINTENANCE_AGENCY_ID"] = "EBA"
    return data, {}


def map_sum_over_open_axis(path="target/SumOverOpenAxis.csv"):
    data = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in data.columns}
    data = data.rename(columns=column_mapping)
    data["MAINTENANCE_AGENCY_ID"] = "EBA"
    return data, {}


def map_expression_scope(path="target/ExpressionScope.csv"):
    data = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in data.columns}
    data = data.rename(columns=column_mapping)
    data["MAINTENANCE_AGENCY_ID"] = "EBA"
    return data, {}








def map_specific_cell_variable(path="target/SpecificCellVariable.csv"):
    data = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in data.columns}
    data = data.rename(columns=column_mapping)
    data["MAINTENANCE_AGENCY_ID"] = "EBA"
    return data, {}


def map_datapoint_version_variable(path="target/DataPointVersionVariable.csv"):
    data = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in data.columns}
    data = data.rename(columns=column_mapping)
    data["MAINTENANCE_AGENCY_ID"] = "EBA"
    return data, {}






def map_datapoints(path="target/DataPoint.csv"):
    datapoints = pd.read_csv(path)

    # Transform column names to UPPER_SNAKE_CASE
    column_mapping = {col: pascal_to_upper_snake(col) for col in datapoints.columns}
    datapoints = datapoints.rename(columns=column_mapping)

    # Set maintenance agency ID and create new datapoint ID
    datapoints["MAINTENANCE_AGENCY_ID"] = "EBA"
    datapoints["NEW_DATA_POINT_ID"] = "EBA_" + datapoints["DATA_POINT_ID"].astype(str)

    # Create ID mapping for return
    id_mapping = dict(zip(datapoints["DATA_POINT_ID"], datapoints["NEW_DATA_POINT_ID"]))

    return datapoints, id_mapping


def map_axis_ordinate_trace(path="target/AxisOrdinateTrace.csv"):
    data = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in data.columns}
    data = data.rename(columns=column_mapping)
    data["MAINTENANCE_AGENCY_ID"] = "EBA"
    return data, {}





def map_module(path="target/Module.csv"):
    modules = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in modules.columns}
    modules = modules.rename(columns=column_mapping)
    modules["MAINTENANCE_AGENCY_ID"] = "EBA"
    modules["NEW_MODULE_ID"] = "EBA_" + modules["MODULE_CODE"]
    id_mapping = dict(zip(modules["MODULE_ID"], modules["NEW_MODULE_ID"]))
    return modules, id_mapping


def map_module_table_version(path="target/ModuleTableVersion.csv"):
    data = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in data.columns}
    data = data.rename(columns=column_mapping)
    data["MAINTENANCE_AGENCY_ID"] = "EBA"
    return data, {}


def map_module_table_or_group(path="target/ModuleTableOrGroup.csv"):
    data = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in data.columns}
    data = data.rename(columns=column_mapping)
    data["MAINTENANCE_AGENCY_ID"] = "EBA"
    return data, {}


def map_module_datapoint_restriction(path="target/ModuleDataPointRestriction.csv"):
    data = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in data.columns}
    data = data.rename(columns=column_mapping)
    data["MAINTENANCE_AGENCY_ID"] = "EBA"
    return data, {}


def map_module_dimension_implied_value(path="target/ModuleDimensionImpliedValue.csv"):
    data = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in data.columns}
    data = data.rename(columns=column_mapping)
    data["MAINTENANCE_AGENCY_ID"] = "EBA"
    return data, {}


def map_validation_rule(path="target/ValidationRule.csv"):
    validations = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in validations.columns}
    validations = validations.rename(columns=column_mapping)
    validations["MAINTENANCE_AGENCY_ID"] = "EBA"
    validations["NEW_VALIDATION_ID"] = "EBA_VAL_" + validations["VALIDATION_ID"].astype(
        str
    )
    id_mapping = dict(
        zip(validations["VALIDATION_ID"], validations["NEW_VALIDATION_ID"])
    )
    return validations, id_mapping


def map_validation_rule_set(path="target/ValidationRuleSet.csv"):
    data = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in data.columns}
    data = data.rename(columns=column_mapping)
    data["MAINTENANCE_AGENCY_ID"] = "EBA"
    return data, {}


def map_validation_scope(path="target/ValidationScope.csv"):
    data = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in data.columns}
    data = data.rename(columns=column_mapping)
    data["MAINTENANCE_AGENCY_ID"] = "EBA"
    return data, {}


def map_pre_condition(path="target/PreCondition.csv"):
    data = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in data.columns}
    data = data.rename(columns=column_mapping)
    data["MAINTENANCE_AGENCY_ID"] = "EBA"
    return data, {}





def map_hierarchy_scope(path="target/HierarchyScope.csv"):
    data = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in data.columns}
    data = data.rename(columns=column_mapping)
    data["MAINTENANCE_AGENCY_ID"] = "EBA"
    return data, {}


def map_hierarchy_validation_rule(path="target/HierarchyValidationRule.csv"):
    data = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in data.columns}
    data = data.rename(columns=column_mapping)
    data["MAINTENANCE_AGENCY_ID"] = "EBA"
    return data, {}


def map_references(path="target/References.csv"):
    references = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in references.columns}
    references = references.rename(columns=column_mapping)
    references["MAINTENANCE_AGENCY_ID"] = "EBA"
    references["NEW_REFERENCE_ID"] = "EBA_REF_" + references["REFERENCE_ID"].astype(str)
    id_mapping = dict(zip(references["REFERENCE_ID"], references["NEW_REFERENCE_ID"]))
    return references, id_mapping


def map_reference_sources(path="target/ReferenceSources.csv"):
    data = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in data.columns}
    data = data.rename(columns=column_mapping)
    data["MAINTENANCE_AGENCY_ID"] = "EBA"
    return data, {}


def map_reference_period_offset(path="target/ReferencePeriodOffset.csv"):
    periods = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in periods.columns}
    periods = periods.rename(columns=column_mapping)
    periods["MAINTENANCE_AGENCY_ID"] = "EBA"
    periods["NEW_PERIOD_OFFSET_ID"] = "EBA_PER_" + periods["PERIOD_OFFSET_ID"].astype(
        str
    )
    id_mapping = dict(zip(periods["PERIOD_OFFSET_ID"], periods["NEW_PERIOD_OFFSET_ID"]))
    return periods, id_mapping


def map_concept_reference(path="target/ConceptReference.csv"):
    data = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in data.columns}
    data = data.rename(columns=column_mapping)
    data["MAINTENANCE_AGENCY_ID"] = "EBA"
    return data, {}


def map_cell_reference(path="target/CellReference.csv"):
    data = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in data.columns}
    data = data.rename(columns=column_mapping)
    data["MAINTENANCE_AGENCY_ID"] = "EBA"
    return data, {}


def map_instance_level_concept(path="target/InstanceLevelConcept.csv"):
    concepts = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in concepts.columns}
    concepts = concepts.rename(columns=column_mapping)
    concepts["MAINTENANCE_AGENCY_ID"] = "EBA"
    concepts["NEW_INSTANCE_LEVEL_CONCEPT_ID"] = "EBA_ILC_" + concepts[
        "INSTANCE_LEVEL_CONCEPT_ID"
    ].astype(str)
    id_mapping = dict(
        zip(
            concepts["INSTANCE_LEVEL_CONCEPT_ID"],
            concepts["NEW_INSTANCE_LEVEL_CONCEPT_ID"],
        )
    )
    return concepts, id_mapping


def map_instance_level_datapoint(path="target/InstanceLevelDataPoint.csv"):
    data = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in data.columns}
    data = data.rename(columns=column_mapping)
    data["MAINTENANCE_AGENCY_ID"] = "EBA"
    return data, {}


def map_instance_level_dimension(path="target/InstanceLevelDimension.csv"):
    data = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in data.columns}
    data = data.rename(columns=column_mapping)
    data["MAINTENANCE_AGENCY_ID"] = "EBA"
    return data, {}





def map_mv_cell_location(path="target/mvCellLocation.csv"):
    data = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in data.columns}
    data = data.rename(columns=column_mapping)
    data["MAINTENANCE_AGENCY_ID"] = "EBA"
    return data, {}


# Remaining utility and configuration tables
def map_taxonomy(path="target/Taxonomy.csv"):
    taxonomies = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in taxonomies.columns}
    taxonomies = taxonomies.rename(columns=column_mapping)
    taxonomies["MAINTENANCE_AGENCY_ID"] = "EBA"
    taxonomies["NEW_TAXONOMY_ID"] = "EBA_" + taxonomies["TAXONOMY_CODE"]
    id_mapping = dict(zip(taxonomies["TAXONOMY_ID"], taxonomies["NEW_TAXONOMY_ID"]))
    return taxonomies, id_mapping


def map_taxonomy_history(path="target/TaxonomyHistory.csv"):
    data = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in data.columns}
    data = data.rename(columns=column_mapping)
    data["MAINTENANCE_AGENCY_ID"] = "EBA"
    return data, {}


def map_taxonomy_table_version(path="target/TaxonomyTableVersion.csv"):
    data = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in data.columns}
    data = data.rename(columns=column_mapping)
    data["MAINTENANCE_AGENCY_ID"] = "EBA"
    return data, {}









def map_datapoint_version_transition_link(
    path="target/DataPointVersionTransitionLink.csv",
):
    data = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in data.columns}
    data = data.rename(columns=column_mapping)
    data["MAINTENANCE_AGENCY_ID"] = "EBA"
    return data, {}


def map_datapoint_version_transition(path="target/DatapointVersionTransition.csv"):
    transitions = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in transitions.columns}
    transitions = transitions.rename(columns=column_mapping)
    transitions["MAINTENANCE_AGENCY_ID"] = "EBA"
    transitions["NEW_TRANSITION_ID"] = "EBA_TRANS_" + transitions[
        "TRANSITION_ID"
    ].astype(str)
    id_mapping = dict(
        zip(transitions["TRANSITION_ID"], transitions["NEW_TRANSITION_ID"])
    )
    return transitions, id_mapping


# Lookup and configuration tables
def map_data_type(path="target/DataType.csv"):
    data_types = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in data_types.columns}
    data_types = data_types.rename(columns=column_mapping)
    data_types["MAINTENANCE_AGENCY_ID"] = "EBA"
    data_types["NEW_DATA_TYPE_ID"] = "EBA_" + data_types["DATA_TYPE_CODE"]
    id_mapping = dict(zip(data_types["DATA_TYPE_ID"], data_types["NEW_DATA_TYPE_ID"]))
    return data_types, id_mapping


def map_flow_type(path="target/FlowType.csv"):
    flow_types = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in flow_types.columns}
    flow_types = flow_types.rename(columns=column_mapping)
    flow_types["MAINTENANCE_AGENCY_ID"] = "EBA"
    flow_types["NEW_FLOW_TYPE_ID"] = "EBA_" + flow_types["FLOW_TYPE_CODE"]
    id_mapping = dict(zip(flow_types["FLOW_TYPE_ID"], flow_types["NEW_FLOW_TYPE_ID"]))
    return flow_types, id_mapping


def map_balance_type(path="target/BalanceType.csv"):
    balance_types = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in balance_types.columns}
    balance_types = balance_types.rename(columns=column_mapping)
    balance_types["MAINTENANCE_AGENCY_ID"] = "EBA"
    balance_types["NEW_BALANCE_TYPE_ID"] = "EBA_" + balance_types["BALANCE_TYPE_CODE"]
    id_mapping = dict(
        zip(balance_types["BALANCE_TYPE_ID"], balance_types["NEW_BALANCE_TYPE_ID"])
    )
    return balance_types, id_mapping


def map_data_sign(path="target/DataSign.csv"):
    data = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in data.columns}
    data = data.rename(columns=column_mapping)
    data["MAINTENANCE_AGENCY_ID"] = "EBA"
    return data, {}


def map_additivity_code(path="target/AdditivityCode.csv"):
    data = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in data.columns}
    data = data.rename(columns=column_mapping)
    data["MAINTENANCE_AGENCY_ID"] = "EBA"
    return data, {}


def map_language(path="target/Language.csv"):
    languages = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in languages.columns}
    languages = languages.rename(columns=column_mapping)
    languages["MAINTENANCE_AGENCY_ID"] = "EBA"
    languages["NEW_LANGUAGE_ID"] = "EBA_" + languages["ISO_CODE"]
    id_mapping = dict(zip(languages["LANGUAGE_ID"], languages["NEW_LANGUAGE_ID"]))
    return languages, id_mapping


def map_owner(path="target/Owner.csv"):
    owners = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in owners.columns}
    owners = owners.rename(columns=column_mapping)
    owners["MAINTENANCE_AGENCY_ID"] = "EBA"
    owners["NEW_OWNER_ID"] = "EBA_" + owners["OWNER_PREFIX"]
    id_mapping = dict(zip(owners["OWNER_ID"], owners["NEW_OWNER_ID"]))
    return owners, id_mapping


def map_concept_translation(path="target/ConceptTranslation.csv"):
    data = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in data.columns}
    data = data.rename(columns=column_mapping)
    data["MAINTENANCE_AGENCY_ID"] = "EBA"
    return data, {}


def map_string_list(path="target/StringList.csv"):
    string_lists = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in string_lists.columns}
    string_lists = string_lists.rename(columns=column_mapping)
    string_lists["MAINTENANCE_AGENCY_ID"] = "EBA"
    string_lists["NEW_STRING_LIST_ID"] = "EBA_" + string_lists["STRING_LIST_CODE"]
    id_mapping = dict(
        zip(string_lists["STRING_LIST_ID"], string_lists["NEW_STRING_LIST_ID"])
    )
    return string_lists, id_mapping


def map_string_list_values(path="target/StringListValues.csv"):
    data = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in data.columns}
    data = data.rename(columns=column_mapping)
    data["MAINTENANCE_AGENCY_ID"] = "EBA"
    return data, {}


# Group and template tables
def map_table_group(path="target/TableGroup.csv"):
    table_groups = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in table_groups.columns}
    table_groups = table_groups.rename(columns=column_mapping)
    table_groups["MAINTENANCE_AGENCY_ID"] = "EBA"
    table_groups["NEW_TABLE_GROUP_ID"] = "EBA_" + table_groups["TABLE_GROUP_CODE"]
    id_mapping = dict(
        zip(table_groups["TABLE_GROUP_ID"], table_groups["NEW_TABLE_GROUP_ID"])
    )
    return table_groups, id_mapping


def map_template_group(path="target/TemplateGroup.csv"):
    template_groups = pd.read_csv(path)
    column_mapping = {
        col: pascal_to_upper_snake(col) for col in template_groups.columns
    }
    template_groups = template_groups.rename(columns=column_mapping)
    template_groups["MAINTENANCE_AGENCY_ID"] = "EBA"
    template_groups["NEW_TEMPLATE_GROUP_ID"] = (
        "EBA_" + template_groups["TEMPLATE_GROUP_CODE"]
    )
    id_mapping = dict(
        zip(
            template_groups["TEMPLATE_GROUP_ID"],
            template_groups["NEW_TEMPLATE_GROUP_ID"],
        )
    )
    return template_groups, id_mapping


def map_template_group_template(path="target/TemplateGroupTemplate.csv"):
    data = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in data.columns}
    data = data.rename(columns=column_mapping)
    data["MAINTENANCE_AGENCY_ID"] = "EBA"
    return data, {}


def map_table_group_templates(path="target/TableGroupTemplates.csv"):
    data = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in data.columns}
    data = data.rename(columns=column_mapping)
    data["MAINTENANCE_AGENCY_ID"] = "EBA"
    return data, {}


def map_conceptual_module(path="target/ConceptualModule.csv"):
    modules = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in modules.columns}
    modules = modules.rename(columns=column_mapping)
    modules["MAINTENANCE_AGENCY_ID"] = "EBA"
    modules["NEW_CONCEPTUAL_MODULE_ID"] = "EBA_" + modules["CONCEPTUAL_MODULE_CODE"]
    id_mapping = dict(
        zip(modules["CONCEPTUAL_MODULE_ID"], modules["NEW_CONCEPTUAL_MODULE_ID"])
    )
    return modules, id_mapping


def map_conceptual_module_template(path="target/ConceptualModuleTemplate.csv"):
    data = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in data.columns}
    data = data.rename(columns=column_mapping)
    data["MAINTENANCE_AGENCY_ID"] = "EBA"
    return data, {}


# Restriction and coordinate tables
def map_open_axis_value_restriction(path="target/OpenAxisValueRestriction.csv"):
    data = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in data.columns}
    data = data.rename(columns=column_mapping)
    data["MAINTENANCE_AGENCY_ID"] = "EBA"
    return data, {}


def map_open_member_restriction(path="target/OpenMemberRestriction.csv"):
    restrictions = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in restrictions.columns}
    restrictions = restrictions.rename(columns=column_mapping)
    restrictions["MAINTENANCE_AGENCY_ID"] = "EBA"
    restrictions["NEW_RESTRICTION_ID"] = "EBA_RESTR_" + restrictions[
        "RESTRICTION_ID"
    ].astype(str)
    id_mapping = dict(
        zip(restrictions["RESTRICTION_ID"], restrictions["NEW_RESTRICTION_ID"])
    )
    return restrictions, id_mapping


def map_dimensional_coordinate(path="target/DimensionalCoordinate.csv"):
    data = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in data.columns}
    data = data.rename(columns=column_mapping)
    data["MAINTENANCE_AGENCY_ID"] = "EBA"
    return data, {}


# Database utility tables
def map_dpm_package(path="target/DpmPackage.csv"):
    data = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in data.columns}
    data = data.rename(columns=column_mapping)
    data["MAINTENANCE_AGENCY_ID"] = "EBA"
    return data, {}


def map_aa_database_version_history(path="target/aaDatabaseVersionHistory.csv"):
    data = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in data.columns}
    data = data.rename(columns=column_mapping)
    data["MAINTENANCE_AGENCY_ID"] = "EBA"
    return data, {}


def map_zz_table_changes(path="target/zzTableChanges.csv"):
    data = pd.read_csv(path)
    column_mapping = {col: pascal_to_upper_snake(col) for col in data.columns}
    data = data.rename(columns=column_mapping)
    data["MAINTENANCE_AGENCY_ID"] = "EBA"
    return data, {}
