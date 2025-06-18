MAPPING = {
    # ==================== AXIS & ORDINATE MAPPINGS ====================
    'AxisOrdinateTrace': {
        'target_table': 'axis_ordinate',
        'column_mappings': {
            'OrdinateID': 'AXIS_ORDINATE_ID',
            'OldOrdinateID': 'PARENT_AXIS_ORDINATE_ID'
        },
        'additional_columns': {
            'IS_ABSTRACT_HEADER': lambda row: row.get('IsAbstractHeader', 0) if row.get('IsAbstractHeader') is not None else 0,
            'CODE': lambda row: row.get('OrdinateCode'),
            'NAME': lambda row: row.get('OrdinateLabel')
        }
    },

    'AxisOrdinate': {
        'target_table': 'axis_ordinate',
        'column_mappings': {
            'OrdinateID': 'AXIS_ORDINATE_ID',
            'AxisID': 'AXIS_ID',
            'IsAbstractHeader': 'IS_ABSTRACT_HEADER',
            'OrdinateCode': 'CODE',
            'OrdinateLabel': 'NAME',
            'Order': 'ORDER',
            'Level': 'LEVEL',
            'Path': 'PATH',
            'ParentOrdinateID': 'PARENT_AXIS_ORDINATE_ID'
        },
        'additional_columns': {
            'DESCRIPTION': lambda row: row.get('Description') if row.get('Description') is not None else row.get('OrdinateLabel')
        }
    },

    'Axis': {
        'target_table': 'axis',
        'column_mappings': {
            'AxisID': 'AXIS_ID',
            'AxisLabel': 'NAME',
            'AxisOrientation': 'ORIENTATION',
            'AxisOrder': 'ORDER',
            'IsOpenAxis': 'IS_OPEN_AXIS',
            'TableVID': 'TABLE_ID'
        },
        'additional_columns': {
            'CODE': lambda row: f"AXIS_{row.get('AxisID')}",
            'DESCRIPTION': lambda row: row.get('Description') if row.get('Description') is not None else row.get('AxisLabel')
        }
    },

    'OrdinateItem': {
        'target_table': 'ordinate_item',
        'column_mappings': {
            'OrdinateID': 'AXIS_ORDINATE_ID',
            'VariableID': 'VARIABLE_ID',
            'MemberID': 'MEMBER_ID'
        },
        'additional_columns': {
            'MEMBER_HIERARCHY_ID': lambda row: row.get('MEMBER_HIERARCHY_ID'),
            'MEMBER_HIERARCHY_VALID_FROM': lambda row: row.get('VALID_FROM'),
            'STARTING_MEMBER_ID': lambda row: None,
            'IS_STARTING_MEMBER_INCLUDED': lambda row: 0
        }
    },

    # ==================== MEMBER & HIERARCHY MAPPINGS ====================
    'Member': {
        'target_table': 'member',
        'column_mappings': {
            'MemberID': 'MEMBER_ID',
            'MemberCode': 'CODE',
            'MemberLabel': 'NAME',
            'DomainID': 'DOMAIN_ID',
            'MemberDescription': 'DESCRIPTION'
        },
        'additional_columns': {
            'MAINTENANCE_AGENCY_ID': lambda row: "NODE"  # Default agency
        }
    },

    'Hierarchy': {
        'target_table': 'member_hierarchy',
        'column_mappings': {
            'HierarchyID': 'MEMBER_HIERARCHY_ID',
            'HierarchyCode': 'CODE',
            'HierarchyLabel': 'NAME',
            'DomainID': 'DOMAIN_ID',
            'HierarchyDescription': 'DESCRIPTION'
        },
        'additional_columns': {
            'MAINTENANCE_AGENCY_ID': lambda row: "NODE",
            'IS_MAIN_HIERARCHY': lambda row: 1 if row.get('HierarchyCode') and 'MAIN' in row.get('HierarchyCode', '') else 0
        }
    },

    'HierarchyNode': {
        'target_table': 'member_hierarchy_node',
        'column_mappings': {
            'HierarchyID': 'MEMBER_HIERARCHY_ID',
            'MemberID': 'MEMBER_ID',
            'Level': 'LEVEL',
            'ParentMemberID': 'PARENT_MEMBER_ID',
            'ComparisonOperator': 'COMPARATOR',
            'UnaryOperator': 'OPERATOR'
        },
        'additional_columns': {
            'VALID_FROM': lambda row: 'CURRENT_DATE',
            'VALID_TO': lambda row: None
        }
    },

    # ==================== DOMAIN & SUBDOMAIN MAPPINGS ====================
    'Domain': {
        'target_table': 'domain',
        'column_mappings': {
            'DomainID': 'DOMAIN_ID',
            'DomainCode': 'CODE',
            'DomainLabel': 'NAME',
            'IsTypedDomain': 'IS_ENUMERATED',
            'DomainDescription': 'DESCRIPTION',
            # DataTypeID': 'DATA_TYPE'
        },
        'additional_columns': {
            'MAINTENANCE_AGENCY_ID': lambda row: "NODE",
            'FACET_ID': lambda row: None,
            'IS_REFERENCE': lambda row: 1 if row.get('IsExternalRefData') == 1 else 0
        }
    },

    'SubCategory': {
        'target_table': 'subdomain',
        'column_mappings': {
            'SubCategoryID': 'SUBDOMAIN_ID',
            'Code': 'CODE',
            'Name': 'NAME',
            'Description': 'DESCRIPTION',
            'CategoryID': 'DOMAIN_ID'
        },
        'additional_columns': {
            'MAINTENANCE_AGENCY_ID': lambda row: "NODE",
            'IS_LISTED': lambda row: 1,
            'FACET_ID': lambda row: None,
            'IS_NATURAL': lambda row: 1
        }
    },

    # ==================== VARIABLE MAPPINGS ====================
    'Variable': {
        'target_table': 'variable',
        'column_mappings': {
            'VariableID': 'VARIABLE_ID'
        },
        'additional_columns': {
            'MAINTENANCE_AGENCY_ID': lambda row: "NODE",
            'CODE': lambda row: f"VAR_{row.get('VariableID')}",
            'NAME': lambda row: f"Variable {row.get('VariableID')}",
            'DOMAIN_ID': lambda row: None,
            'DESCRIPTION': lambda row: None,
            'PRIMARY_CONCEPT': lambda row: None,
            'IS_DECOMPOSED': lambda row: 0
        }
    },

    'VariableVersion': {
        'target_table': 'variable',
        'column_mappings': {
            'VariableVID': 'VARIABLE_ID',
            'Code': 'CODE',
            'Name': 'NAME',
            'PropertyID': 'DOMAIN_ID'
        },
        'additional_columns': {
            'MAINTENANCE_AGENCY_ID': lambda row: "NODE",
            'DESCRIPTION': lambda row: row.get('Description') if row.get('Description') is not None else row.get('Name'),
            'PRIMARY_CONCEPT': lambda row: None,
            'IS_DECOMPOSED': lambda row: 1 if row.get('IsMultiValued') == 1 else 0
        }
    },

    # ==================== FRAMEWORK MAPPINGS ====================
    'Framework': {
        'target_table': 'framework',
        'column_mappings': {
            'FrameworkID': 'FRAMEWORK_ID',
            'Code': 'CODE',
            'Name': 'NAME',
            'Description': 'DESCRIPTION'
        },
        'additional_columns': {
            'MAINTENANCE_AGENCY_ID': lambda row: "NODE",
            'FRAMEWORK_TYPE': lambda row: "REGULATORY",
            'REPORTING_POPULATION': lambda row: None,
            'OTHER_LINKS': lambda row: None,
            'ORDER': lambda row: row.get('FrameworkID'),
            'FRAMEWORK_STATUS': lambda row: "ACTIVE"
        }
    },

    'ReportingFramework': {
        'target_table': 'framework',
        'column_mappings': {
            'FrameworkID': 'FRAMEWORK_ID',
            'FrameworkCode': 'CODE',
            'FrameworkLabel': 'NAME'
        },
        'additional_columns': {
            'MAINTENANCE_AGENCY_ID': lambda row: "NODE",
            'DESCRIPTION': lambda row: row.get('Description') if row.get('Description') is not None else row.get('FrameworkLabel'),
            'FRAMEWORK_TYPE': lambda row: "REPORTING",
            'REPORTING_POPULATION': lambda row: None,
            'OTHER_LINKS': lambda row: None,
            'ORDER': lambda row: row.get('FrameworkID'),
            'FRAMEWORK_STATUS': lambda row: "ACTIVE"
        }
    },

    # ==================== CUBE MAPPINGS ====================
    'Module': {
        'target_table': 'cube',
        'column_mappings': {
            'ModuleID': 'CUBE_ID',
            'FrameworkID': 'FRAMEWORK_ID'
        },
        'additional_columns': {
            'MAINTENANCE_AGENCY_ID': lambda row: "NODE",
            'NAME': lambda row: f"Module {row.get('ModuleID')}",
            'CODE': lambda row: f"MOD_{row.get('ModuleID')}",
            'CUBE_STRUCTURE_ID': lambda row: None,
            'CUBE_TYPE': lambda row: "REPORTING",
            'IS_ALLOWED': lambda row: 1,
            'VALID_FROM': lambda row: 'CURRENT_DATE',
            'VALID_TO': lambda row: None,
            'VERSION': lambda row: "1.0",
            'DESCRIPTION': lambda row: None,
            'PUBLISHED': lambda row: 0,
            'DATASET_URL': lambda row: None,
            'FILTERS': lambda row: None,
            'DI_EXPORT': lambda row: 0
        }
    },

    'ModuleVersion': {
        'target_table': 'cube',
        'column_mappings': {
            'ModuleVID': 'CUBE_ID',
            'ModuleID': 'CUBE_ID',
            'Code': 'CODE',
            'Name': 'NAME',
            'Description': 'DESCRIPTION',
            'VersionNumber': 'VERSION'
        },
        'additional_columns': {
            'MAINTENANCE_AGENCY_ID': lambda row: "NODE",
            'FRAMEWORK_ID': lambda row: row.get('FRAMEWORK_ID'),
            'CUBE_STRUCTURE_ID': lambda row: row.get('CUBE_STRUCTURE_ID'),
            'CUBE_TYPE': lambda row: "REPORTING",
            'IS_ALLOWED': lambda row: 1 if row.get('IsReported') == 1 else 0,
            'VALID_FROM': lambda row: row.get('FromReferenceDate'),
            'VALID_TO': lambda row: row.get('ToReferenceDate'),
            'PUBLISHED': lambda row: 1,
            'DATASET_URL': lambda row: None,
            'FILTERS': lambda row: None,
            'DI_EXPORT': lambda row: 0
        }
    },

    # ==================== TABLE MAPPINGS ====================
    'Table': {
        'target_table': 'table',
        'column_mappings': {
            'TableID': 'TABLE_ID'
        },
        'additional_columns': {
            'NAME': lambda row: f"Table {row.get('TableID')}",
            'CODE': lambda row: f"TBL_{row.get('TableID')}",
            'DESCRIPTION': lambda row: None,
            'MAINTENANCE_AGENCY_ID': lambda row: "NODE",
            'VERSION': lambda row: "1.0",
            'VALID_FROM': lambda row: 'CURRENT_DATE',
            'VALID_TO': lambda row: None
        }
    },

    'TableVersion': {
        'target_table': 'table',
        'column_mappings': {
            'TableVID': 'TABLE_ID',
            'Code': 'CODE',
            'Name': 'NAME',
            'Description': 'DESCRIPTION'
        },
        'additional_columns': {
            'MAINTENANCE_AGENCY_ID': lambda row: "NODE",
            'VERSION': lambda row: "1.0",
            'VALID_FROM': lambda row: row.get('StartReleaseID'),
            'VALID_TO': lambda row: row.get('EndReleaseID')
        }
    },

    # ==================== CELL MAPPINGS ====================
    'Cell': {
        'target_table': 'table_cell',
        'column_mappings': {
            'CellID': 'CELL_ID',
            'TableID': 'TABLE_ID'
        },
        'additional_columns': {
            'IS_SHADED': lambda row: 0,
            'TABLE_CELL_COMBINATION_ID': lambda row: None,
            'SYSTEM_DATA_CODE': lambda row: f"CELL_{row.get('CellID')}",
            'NAME': lambda row: f"Cell {row.get('CellID')}"
        }
    },

    'TableCell': {
        'target_table': 'table_cell',
        'column_mappings': {
            'CellID': 'CELL_ID',
            'TableVID': 'TABLE_ID',
            'IsShaded': 'IS_SHADED',
            'CellCode': 'SYSTEM_DATA_CODE'
        },
        'additional_columns': {
            'TABLE_CELL_COMBINATION_ID': lambda row: row.get('DataPointVID'),
            'NAME': lambda row: row.get('Name') if row.get('Name') is not None else row.get('CellCode')
        }
    },

    'CellPosition': {
        'target_table': 'cell_position',
        'column_mappings': {
            'CellID': 'CELL_ID',
            'OrdinateID': 'AXIS_ORDINATE_ID'
        }
    },

    # ==================== COMBINATION MAPPINGS ====================
    'DataPointVersion': {
        'target_table': 'combination',
        'column_mappings': {
            'DataPointVID': 'COMBINATION_ID',
            'MetricID': 'METRIC'
        },
        'additional_columns': {
            'CODE': lambda row: f"DP_{row.get('DataPointVID')}",
            'NAME': lambda row: f"DataPoint {row.get('DataPointVID')}",
            'MAINTENANCE_AGENCY_ID': lambda row: "NODE",
            'VERSION': lambda row: "1.0",
            'VALID_FROM': lambda row: row.get('FromDate'),
            'VALID_TO': lambda row: row.get('ToDate')
        }
    },

    'CombinationItem': {
        'target_table': 'combination_item',
        'column_mappings': {
            'DataPointVID': 'COMBINATION_ID',
            'VariableID': 'VARIABLE_ID',
            'MemberID': 'MEMBER_ID'
        },
        'additional_columns': {
            'SUBDOMAIN_ID': lambda row: None,
            'VARIABLE_SET_ID': lambda row: None,
            'MEMBER_HIERARCHY': lambda row: None
        }
    },

    # ==================== MAPPING DEFINITIONS ====================
    'ValidationRule': {
        'target_table': 'mapping_definition',
        'column_mappings': {
            'ValidationId': 'MAPPING_ID',
            'ValidationCode': 'CODE',
            'ExpressionID': 'ALGORITHM'
        },
        'additional_columns': {
            'MAINTENANCE_AGENCY_ID': lambda row: "NODE",
            'NAME': lambda row: row.get('NarrativeExplanation') if row.get('NarrativeExplanation') is not None else row.get('ValidationCode'),
            'MAPPING_TYPE': lambda row: "VALIDATION",
            'MEMBER_MAPPING_ID': lambda row: None,
            'VARIABLE_MAPPING_ID': lambda row: None
        }
    },

    # ==================== MAINTENANCE AGENCY ====================
    'Organisation': {
        'target_table': 'maintenance_agency',
        'column_mappings': {
            'OrgID': 'MAINTENANCE_AGENCY_ID',
            'Acronym': 'CODE',
            'Name': 'NAME'
        },
        'additional_columns': {
            'DESCRIPTION': lambda row: row.get('Description') if row.get('Description') is not None else row.get('Name')
        }
    },

    # ==================== CUBE STRUCTURE MAPPINGS ====================
    'Template': {
        'target_table': 'cube_structure',
        'column_mappings': {
            'TemplateID': 'CUBE_STRUCTURE_ID',
            'TemplateCode': 'CODE',
            'TemplateLabel': 'NAME'
        },
        'additional_columns': {
            'MAINTENANCE_AGENCY_ID': lambda row: "NODE",
            'DESCRIPTION': lambda row: row.get('Description') if row.get('Description') is not None else row.get('TemplateLabel'),
            'VALID_FROM': lambda row: 'CURRENT_DATE',
            'VALID_TO': lambda row: None,
            'VERSION': lambda row: "1.0"
        }
    },

    # ==================== VARIABLE SET MAPPINGS ====================
    'Category': {
        'target_table': 'variable_set',
        'column_mappings': {
            'CategoryID': 'VARIABLE_SET_ID',
            'Code': 'CODE',
            'Name': 'NAME',
            'Description': 'DESCRIPTION'
        },
        'additional_columns': {
            'MAINTENANCE_AGENCY_ID': lambda row: "NODE"
        }
    }
}

PRIORITY_ORDER = [
    # Base entities first (no dependencies)
    'Organisation.csv',           # MAINTENANCE_AGENCY
    'Domain.csv',                # DOMAIN
    'Category.csv',              # VARIABLE_SET
    'Framework.csv',             # FRAMEWORK
    'ReportingFramework.csv',    # FRAMEWORK
    'DataType.csv',              # Used by DOMAIN

    # Entities with minimal dependencies
    'Member.csv',                # MEMBER (depends on DOMAIN, MAINTENANCE_AGENCY)
    'Variable.csv',              # VARIABLE
    'VariableVersion.csv',       # VARIABLE
    'SubCategory.csv',           # SUBDOMAIN (depends on DOMAIN)
    'Hierarchy.csv',             # MEMBER_HIERARCHY (depends on DOMAIN)
    'Template.csv',              # CUBE_STRUCTURE

    # More complex entities
    'HierarchyNode.csv',         # MEMBER_HIERARCHY_NODE (depends on HIERARCHY, MEMBER)
    'Module.csv',                # CUBE (depends on FRAMEWORK)
    'ModuleVersion.csv',         # CUBE (depends on FRAMEWORK)
    'Table.csv',                 # TABLE
    'TableVersion.csv',          # TABLE
    'Axis.csv',                  # AXIS (depends on TABLE)

    # Complex mapping entities
    'AxisOrdinate.csv',          # AXIS_ORDINATE (depends on AXIS)
    'AxisOrdinateTrace.csv',     # AXIS_ORDINATE
    'OrdinateItem.csv',          # ORDINATE_ITEM (depends on AXIS_ORDINATE, VARIABLE, MEMBER)
    'Cell.csv',                  # TABLE_CELL (depends on TABLE)
    'TableCell.csv',             # TABLE_CELL (depends on TABLE)
    'CellPosition.csv',          # CELL_POSITION (depends on TABLE_CELL, AXIS_ORDINATE)

    # Data points and combinations
    'DataPointVersion.csv',      # COMBINATION
    'CombinationItem.csv',       # COMBINATION_ITEM (depends on COMBINATION)
    'ValidationRule.csv',        # MAPPING_DEFINITION
]


# Input model tables for reference
INPUT_TABLES = [
    'SumOfManyOrdinate', 'OrdinateVariable', 'OrdinateCategorisation', 'AxisOrdinateTrace',
    'ReferenceCategorisation', 'Template', 'ModuleTableVersion', 'Operation', 'PropertyCategory',
    'OperationScopeComposition', 'Release', 'ReportingFramework', 'OperatorArgument',
    'ContextComposition', 'DataType', 'VariableOfExpression', 'SubCategory', 'TableGroup',
    'ModuleParameter', 'ContextOfDataPoint', 'Aux_CellMapping', 'ConceptualModuleTemplate',
    'Aux_CellStatus', 'Axis', 'DataPointVersionTransitionLink', 'TableAssociation',
    'SubCategoryItem', 'OpenAxisValueRestriction', 'ChangeLog', 'HierarchyScope',
    'VariableGeneration', 'OpenMemberRestriction', 'SubCategoryVersion', 'TaxonomyHistory',
    'ConceptualModule', 'Framework', 'TaxonomyTableVersion', 'UserRole', 'HierarchyValidationRule',
    'zzTableChange', 'Table', 'DPMCla', 'DataSign', 'Property', 'ValidationRuleSet', 'Module',
    'OperationCodePrefix', 'Reference', 'Header', 'SumOverOpenAxi', 'TemplateGroup', 'Context',
    'OperationVersion', 'DataPointVersion', 'AdditivityCode', 'Operator', 'VarGeneration_Detail',
    'ExpressionScope', 'Cell', 'ModuleTableOrGroup', 'Item', 'Language', 'Hierarchy', 'Concept',
    'DocumentVersion', 'ConceptTranslation', 'SubdivisionType', 'StringListValue', 'StringList',
    'PreCondition', 'DPMAttribute', 'Subdivision', 'ModelViolation', 'InstanceLevelDimension',
    'DpmPackage', 'Organisation', 'ModuleDataPointRestriction', 'DatapointVersionTransition',
    'ContextDefinition', 'aaDatabaseVersionHistory', 'OperationVersionData', 'ItemCategory',
    'Metri', 'TableGroupComposition', 'TableVersion', 'BalanceType', 'CompoundKey',
    'TableVersionCell', 'VarGeneration_Summary', 'ModuleVersion', 'SpecificCellVariable',
    'ConceptRelation', 'OperationScope', 'ValidationRule', 'Member', 'VariableVersion',
    'Dimension', 'User', 'Reference', 'KeyHeaderMapping', 'Role', 'ReferenceSource',
    'OperandReference', 'Owner', 'Variable', 'DataPoint', 'KeyComposition', 'TableVersionHeader',
    'InstanceLevelConcept', 'VariableCalculation', 'AxisOrdinate', 'Document', 'mvCellLocation',
    'CellPosition', 'ConceptReference', 'TableGroupTemplate', 'RelatedConcept',
    'DataPointVersionVariable', 'Category', 'Taxonomy', 'OperandReferenceLocation', 'Domain',
    'SuperCategoryComposition', 'DimensionalCoordinate', 'Expression', 'Translation',
    'MetricVariable', 'CellReference', 'ModuleVersionComposition', 'ModuleDimensionImpliedValue',
    'TemplateGroupTemplate', 'FlowType', 'TableCell', 'ValidationScope', 'ReferencePeriodOffset',
    'InstanceLevelDataPoint', 'HeaderVersion', 'CompoundItemContext', 'OperationNode',
    'HierarchyNode'
]

# Output model tables for reference
OUTPUT_TABLES = [
    'member_link', 'member_hierarchy_node', 'cube_structure_item_link', 'member_mapping_item',
    'facet_collection', 'axi', 'subdomain', 'axis_ordinate', 'cube_structure', 'framework',
    'member_hierarchy', 'table', 'mapping_to_cube', 'combination', 'cube_link',
    'combination_item', 'cube_structure_item', 'cube_to_combination', 'variable_mapping_item',
    'maintenance_agency', 'member', 'member_mapping', 'variable', 'cube', 'ordinate_item',
    'table_cell', 'mapping_definition', 'domain', 'variable_set', 'variable_mapping',
    'subdomain_enumeration', 'cell_position', 'variable_set_enumeration'
]
