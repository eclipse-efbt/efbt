from transformation_utilities import *

# ==================== MAPPING DEFINITIONS ====================
MAPPING = {
    # ==================== AXIS & ORDINATE MAPPINGS ====================
    # 'AxisOrdinateTrace': {
    #     'target_table': 'axis_ordinate',
    #     'column_mappings': {
    #         'OrdinateID': 'AXIS_ORDINATE_ID',
    #         'OldOrdinateID': 'PARENT_AXIS_ORDINATE_ID'
    #     },
    #     'additional_columns': {
    #         'IS_ABSTRACT_HEADER': lambda row: row.get('IsAbstractHeader'),
    #         'CODE': lambda row: row.get('OrdinateCode'),
    #         'NAME': lambda row: row.get('OrdinateLabel')
    #     }
    # },

    'AxisOrdinate': {
        'target_table': 'axis_ordinate',
        'column_mappings': {
            'IsAbstractHeader': 'IS_ABSTRACT_HEADER',
            'OrdinateCode': 'CODE',
            'OrdinateLabel': 'NAME',
            'Order': 'ORDER',
            'Level': 'LEVEL',
            'Path': 'PATH'
        },
        'additional_columns': {
            'AXIS_ORDINATE_ID': lambda row: build_axis_ordinate_id(row),
            'AXIS_ID': lambda row: row.get("AxisID"),
            'PARENT_AXIS_ORDINATE_ID': lambda row: build_axis_ordinate_id({'OrdinateID': row.get('ParentOrdinateID'), 'AxisID': row.get('AxisID'), 'OrdinateCode': ''}) if row.get('ParentOrdinateID') else None,
            'DESCRIPTION': lambda row: row.get('Description') if row.get('Description') is not None else row.get('OrdinateLabel')
        }
    },

    'Axis': {
        'target_table': 'axis',
        'column_mappings': {
            'AxisLabel': 'NAME',
            'AxisOrientation': 'ORIENTATION',
            'AxisOrder': 'ORDER',
            'IsOpenAxis': 'IS_OPEN_AXIS',
            'TableVID': 'TABLE_ID'
        },
        'additional_columns': {
            'AXIS_ID': lambda row: row.get("AxisID"),
            'CODE': lambda row: f"AXIS_{row.get('AxisID', '')}" if row.get('AxisID') else f"AXIS_{row.get('AxisOrder', '1')}",
            'DESCRIPTION': lambda row: row.get('Description', row.get('AxisLabel'))
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
            'MemberCode': 'CODE',
            'MemberLabel': 'NAME',
            'DomainID': 'DOMAIN_ID',
            'MemberDescription': 'DESCRIPTION'
        },
        'additional_columns': {
            'MAINTENANCE_AGENCY_ID': lambda row: "EBA",
            'MEMBER_ID': lambda row: build_member_id(row)
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
            'MAINTENANCE_AGENCY_ID': lambda row: "EBA",
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
            'MEMBER_ID': lambda row: "_".join(["EBA",row.get('DomainID',""),row.get('MemberID',"")]),
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
            'MAINTENANCE_AGENCY_ID': lambda row: "EBA",
            'DOMAIN_ID': lambda row: "_".join(["EBA",row.get('DomainID',"")]),
            'FACET_ID': lambda row: None,
            'DESCRIPTION': lambda row: row.get('DomainDescription').replace("\n"," "),
            'IS_REFERENCE': lambda row: 1 if row.get('IsExternalRefData') == 1 else 0
        }
    },

    # ==================== VARIABLE MAPPINGS ====================
    'Variable': {
        'target_table': 'variable',
        'column_mappings': {
            'VariableID': 'VARIABLE_ID'
        },
        'additional_columns': {
            'VARIABLE_ID': lambda row: "EBA"+"_"+row.get('VariableID'),
            'MAINTENANCE_AGENCY_ID': lambda row: resolve_maintenance_agency(row, "variable"),
            'CODE': lambda row: f"VAR_{row.get('VariableID')}",
            'NAME': lambda row: f"Variable {row.get('VariableID')}",
            'DOMAIN_ID': lambda row: row.get('PropertyID') or row.get('DomainID'),
            'DESCRIPTION': lambda row: derive_description(row, fallback_prefix="Variable "),
            'PRIMARY_CONCEPT': lambda row: derive_primary_concept(row),
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
            'MAINTENANCE_AGENCY_ID': lambda row: resolve_maintenance_agency(row, "variable"),
            'DESCRIPTION': lambda row: derive_description(row, ['Description', 'Name'], "Variable "),
            'PRIMARY_CONCEPT': lambda row: derive_primary_concept(row),
            'IS_DECOMPOSED': lambda row: 1 if row.get('IsMultiValued') == 1 else 0
        }
    },

    # ==================== FRAMEWORK MAPPINGS ====================

    'ReportingFramework': {
        'target_table': 'framework',
        'column_mappings': {
            'FrameworkCode': 'CODE',
            'FrameworkLabel': 'NAME'
        },
        'additional_columns': {
            'FRAMEWORK_ID': lambda row: build_framework_id(row, framework_code=row.get('FrameworkCode')),
            'MAINTENANCE_AGENCY_ID': lambda row: "EBA",
            'DESCRIPTION': lambda row: derive_description(row, ['Description', 'FrameworkLabel'], "Framework "),
            'FRAMEWORK_TYPE': lambda row: "REPORTING",
            'REPORTING_POPULATION': lambda row: None,
            'OTHER_LINKS': lambda row: None,
            'ORDER': lambda row: row.get('FrameworkID'),
            'FRAMEWORK_STATUS': lambda row: "ACTIVE"
        }
    },

    # # ==================== CUBE MAPPINGS ====================
    # 'Module': {
    #     'target_table': 'cube',
    #     'column_mappings': {
    #     },
    #     'additional_columns': {
    #         'FRAMEWORK_ID': lambda row: build_framework_id_from_module(row),
    #         'CUBE_ID': lambda row: build_cube_id(row),
    #         'MAINTENANCE_AGENCY_ID': lambda row: resolve_maintenance_agency(row, "cube"),
    #         'NAME': lambda row: derive_description(row, ['Name', 'ModuleLabel'], f"Module {row.get('ModuleID', '')} "),
    #         'CODE': lambda row: build_module_code(row),
    #         'CUBE_STRUCTURE_ID': lambda row: build_cube_id(row),  # Should match CUBE_ID
    #         'CUBE_TYPE': lambda row: "D",  # Change to "D" to match technical export
    #         'IS_ALLOWED': lambda row: 1,
    #         'VALID_FROM': lambda row: derive_valid_dates(row, "cube")[0],
    #         'VALID_TO': lambda row: derive_valid_dates(row, "cube")[1],
    #         'VERSION': lambda row: lookup_version_from_module(row),
    #         'DESCRIPTION': lambda row: derive_description(row, ['Description', 'ModuleLabel'], "Reporting cube "),
    #         'PUBLISHED': lambda row: 1,  # Change to 1 to match technical export
    #         'DATASET_URL': lambda row: None,
    #         'FILTERS': lambda row: None,
    #         'DI_EXPORT': lambda row: 0
    #     }
    # },

    # 'ModuleVersion': {
    #     'target_table': 'cube',
    #     'column_mappings': {
    #         'Code': 'CODE',
    #         'Name': 'NAME',
    #         'Description': 'DESCRIPTION',
    #         'VersionNumber': 'VERSION'
    #     },
    #     'additional_columns': {
    #         'CUBE_ID': lambda row: build_cube_id(row),
    #         'MAINTENANCE_AGENCY_ID': lambda row: resolve_maintenance_agency(row, "cube"),
    #         'FRAMEWORK_ID': lambda row: row.get('FRAMEWORK_ID'),
    #         'CUBE_STRUCTURE_ID': lambda row: row.get('CUBE_STRUCTURE_ID') or derive_cube_structure_id(row),
    #         'CUBE_TYPE': lambda row: "REPORTING",
    #         'IS_ALLOWED': lambda row: 1 if row.get('IsReported') == 1 else 0,
    #         'VALID_FROM': lambda row: transform_taxonomy_date(row.get('FromReferenceDate')) or derive_valid_dates(row, "cube")[0],
    #         'VALID_TO': lambda row: transform_taxonomy_date(row.get('ToReferenceDate')) or derive_valid_dates(row, "cube")[1],
    #         'PUBLISHED': lambda row: 1,
    #         'DATASET_URL': lambda row: None,
    #         'FILTERS': lambda row: None,
    #         'DI_EXPORT': lambda row: 0
    #     }
    # },

    # ==================== TABLE MAPPINGS ====================
    # Note: Enhanced Table mapping is defined later with build_table_id()

    'TableVersion': {
        'target_table': 'table',
        'column_mappings': {
            'TableVID': 'TABLE_ID',
            'Code': 'CODE',
            'Name': 'NAME',
            'Description': 'DESCRIPTION'
        },
        'additional_columns': {
            'MAINTENANCE_AGENCY_ID': lambda row: "EBA",
            'VERSION': lambda row: "1.0",
            'VALID_FROM': lambda row: row.get('StartReleaseID'),
            'VALID_TO': lambda row: row.get('EndReleaseID')
        }
    },

    # ==================== CELL MAPPINGS ====================
    'Cell': {
        'target_table': 'table_cell',
        'column_mappings': {
            'TableID': 'TABLE_ID'
        },
        'additional_columns': {
            'CELL_ID': lambda row: build_cell_id(row),
            'IS_SHADED': lambda row: 0,
            'TABLE_CELL_COMBINATION_ID': lambda row: None,
            'SYSTEM_DATA_CODE': lambda row: f"CELL_{row.get('CellID')}",
            'NAME': lambda row: f"Cell {row.get('CellID')}"
        }
    },

    'TableCell': {
        'target_table': 'table_cell',
        'column_mappings': {
            'TableVID': 'TABLE_ID',
            'IsShaded': 'IS_SHADED',
            'CellCode': 'SYSTEM_DATA_CODE'
        },
        'additional_columns': {
            'CELL_ID': lambda row: build_cell_id(row),
            'TABLE_CELL_COMBINATION_ID': lambda row: row.get('DataPointVID'),
            'NAME': lambda row: row.get('Name') if row.get('Name') is not None else row.get('CellCode')
        }
    },

    'CellPosition': {
        'target_table': 'cell_position',
        'column_mappings': {
        },
        'additional_columns': {
            'CELL_ID': lambda row: build_cell_id(row),
            'AXIS_ORDINATE_ID': lambda row: build_axis_ordinate_id({'OrdinateID': row.get('OrdinateID'), 'AxisID': row.get('AxisID', ''), 'OrdinateCode': row.get('OrdinateCode', '')})
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
            'MAINTENANCE_AGENCY_ID': lambda row: "EBA",
            'VERSION': lambda row: "1.0",
            'VALID_FROM': lambda row: row.get('FromDate'),
            'VALID_TO': lambda row: row.get('ToDate')
        }
    },

    'ContextOfDataPoints': {
        'target_table': 'combination_item',
        'column_mappings': {
            'ContextID': 'COMBINATION_ID'
        },
        'additional_columns': {
            'VARIABLE_ID': lambda row: extract_variable_from_context(row) or 'EBA_UNK',
            'SUBDOMAIN_ID': lambda row: None,
            'VARIABLE_SET_ID': lambda row: None,
            'MEMBER_ID': lambda row: extract_member_from_context(row) or 'EBA_UNK_EBA_x0'
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
            'MAINTENANCE_AGENCY_ID': lambda row: "EBA",
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
            'MAINTENANCE_AGENCY_ID': lambda row: "EBA"
        }
    },


    # ==================== DIMENSION MAPPINGS ====================
    'Dimension': {
        'target_table': 'variable',
        'column_mappings': {
            'DimensionCode': 'CODE',
            'DimensionLabel': 'NAME',
            'DomainID': 'DOMAIN_ID',
            'DimensionDescription': 'DESCRIPTION'
        },
        'additional_columns': {
            'VARIABLE_ID': lambda row: build_variable_id(row),
            'MAINTENANCE_AGENCY_ID': lambda row: "EBA",
            'PRIMARY_CONCEPT': lambda row: None,
            'IS_DECOMPOSED': lambda row: 1 if row.get('DimensionXbrlCode') else 0
        }
    },

    # ==================== CONCEPT MAPPINGS ====================
    # 'Concept': {
    #     'target_table': 'maintenance_agency',
    #     'column_mappings': {
    #         'ConceptID': 'MAINTENANCE_AGENCY_ID'
    #     },
    #     'additional_columns': {
    #         'CODE': lambda row: transform_id(row.get('ConceptID'), 'CONCEPT_'),
    #         'NAME': lambda row: f"Concept {row.get('ConceptID')}",
    #         'DESCRIPTION': lambda row: f"Auto-generated concept {row.get('ConceptID')} created on {transform_date(row.get('CreationDate'))}"
    #     }
    # },

    # ==================== DATAPOINT MAPPINGS ====================
    # 'DataPoint': {
    #     'target_table': 'combination',
    #     'column_mappings': {
    #         'DataPointID': 'COMBINATION_ID'
    #     },
    #     'additional_columns': {
    #         'CODE': lambda row: transform_id(row.get('DataPointID'), 'DP_'),
    #         'NAME': lambda row: f"DataPoint {row.get('DataPointID')}",
    #         'MAINTENANCE_AGENCY_ID': lambda row: resolve_maintenance_agency(row, "datapoint"),
    #         'METRIC': lambda row: derive_metric_from_datapoint(row),
    #         'VERSION': lambda row: "1.0",
    #         'VALID_FROM': lambda row: transform_date(row.get('FromDate')) or derive_valid_dates(row, "datapoint")[0],
    #         'VALID_TO': lambda row: transform_date(row.get('ToDate')) or derive_valid_dates(row, "datapoint")[1]
    #     }
    # },

    # ==================== DATATYPE MAPPINGS ====================
    'DataType': {
        'target_table': 'domain',
        'column_mappings': {
            'DataTypeID': 'DOMAIN_ID',
            'DataTypeCode': 'CODE',
            'DataTypeLabel': 'NAME'
        },
        'additional_columns': {
            'MAINTENANCE_AGENCY_ID': lambda row: resolve_maintenance_agency(row, "domain"),
            'IS_ENUMERATED': lambda row: 0,
            'DESCRIPTION': lambda row: derive_description(row, ['DataTypeLabel', 'Description'], "Data type "),
            'DATA_TYPE': lambda row: row.get('DataTypeCode'),
            'FACET_ID': lambda row: row.get('FacetID'),
            'IS_REFERENCE': lambda row: 0
        }
    },

    # ==================== ENHANCED CUBE MAPPINGS ====================
    'Template': {
        'target_table': 'cube_structure',
        'column_mappings': {
            'TemplateCode': 'CODE',
            'TemplateLabel': 'NAME'
        },
        'additional_columns': {
            'CUBE_STRUCTURE_ID': lambda row: build_template_cube_id(row),
            'MAINTENANCE_AGENCY_ID': lambda row: lookup_owner_from_template(row),
            'DESCRIPTION': lambda row: row.get('Description') if row.get('Description') is not None else row.get('TemplateLabel'),
            'VALID_FROM': lambda row: '1900-01-01',
            'VALID_TO': lambda row: '9999-12-31',
            'VERSION': lambda row: "1.0"
        }
    },

    # ==================== CONCEPTUAL MODULE MAPPINGS ====================
    'ConceptualModule': {
        'target_table': 'framework',
        'column_mappings': {
            'ConceptualModuleID': 'FRAMEWORK_ID',
            'ConceptualModuleCode': 'CODE',
            'ConceptualModuleLabel': 'NAME'
        },
        'additional_columns': {
            'MAINTENANCE_AGENCY_ID': lambda row: "EBA",
            'DESCRIPTION': lambda row: row.get('ConceptualModuleLabel'),
            'FRAMEWORK_TYPE': lambda row: "CONCEPTUAL",
            'REPORTING_POPULATION': lambda row: None,
            'OTHER_LINKS': lambda row: None,
            'ORDER': lambda row: row.get('ConceptualModuleID'),
            'FRAMEWORK_STATUS': lambda row: "ACTIVE"
        }
    },

    # ==================== CONCEPTUAL MODULE TEMPLATE MAPPINGS ====================
    'ConceptualModuleTemplate': {
        'target_table': 'framework_subdomain',
        'column_mappings': {
            'ConceptualModuleID': 'FRAMEWORK_ID',
            'TemplateID': 'SUBDOMAIN_ID'
        },
        'additional_columns': {
            'MAINTENANCE_AGENCY_ID': lambda row: "EBA",
            'CODE': lambda row: f"CMT_{row.get('ConceptualModuleID')}_{row.get('TemplateID')}",
            'NAME': lambda row: f"Template {row.get('TemplateID')} in Module {row.get('ConceptualModuleID')}",
            'DESCRIPTION': lambda row: None,
            'DOMAIN_ID': lambda row: None,
            'IS_LISTED': lambda row: 1,
            'FACET_ID': lambda row: None,
            'IS_NATURAL': lambda row: 1
        }
    },

    # ==================== ENHANCED TABLE INFORMATION MAPPINGS ====================
    'Table': {
        'target_table': 'table',
        'column_mappings': {
            'OriginalTableCode': 'CODE',
            'OriginalTableLabel': 'NAME'
        },
        'additional_columns': {
            'TABLE_ID': lambda row: build_table_id(row),
            'MAINTENANCE_AGENCY_ID': lambda row: lookup_owner_prefix_from_table(row),
            'DESCRIPTION': lambda row: row.get('OriginalTableLabel'),
            'VERSION': lambda row: lookup_version_from_table(row),
            'VALID_FROM': lambda row: '1900-01-01',
            'VALID_TO': lambda row: '9999-12-31'
        }
    },

    # ==================== FRAMEWORK SUBDOMAIN MAPPINGS ====================
    # Skip subdomain for now - SubCategory.csv doesn't exist

    # ==================== CUBE TO TABLE MAPPINGS ====================
    'ModuleTableVersion': {
        'target_table': 'cube_to_table',
        'column_mappings': {
            'ModuleID': 'CUBE_ID',
            'TableVID': 'TABLE_ID'
        },
        'additional_columns': {
            'CUBE_ID': lambda row: build_cube_id_from_module(row),
            'TABLE_ID': lambda row: build_table_id_from_table_vid(row)
        }
    },

    # ==================== VARIABLE MAPPING ITEMS ====================
    'VariableOfExpression': {
        'target_table': 'variable_mapping_item',
        'column_mappings': {
            'VariableID': 'VARIABLE_ID',
            'ExpressionID': 'VARIABLE_MAPPING_ID'
        },
        'additional_columns': {
            'VARIABLE_SET_ID': lambda row: None,
            'MEMBER_ID': lambda row: None,
            'MEMBER_HIERARCHY_ID': lambda row: None,
            'SUBDOMAIN_ID': lambda row: None
        }
    },

    # ==================== OWNER/AGENCY MAPPINGS ====================
    'Owner': {
        'target_table': 'maintenance_agency',
        'column_mappings': {
            'OwnerName': 'NAME'
        },
        'additional_columns': {
            'MAINTENANCE_AGENCY_ID': lambda row: (row.get('OwnerPrefix') or 'NODE').upper(),
            'CODE': lambda row: (row.get('OwnerPrefix') or 'NODE').upper(),
            'DESCRIPTION': lambda row: row.get('OwnerCopyright') or row.get('OwnerName') or 'Default maintenance agency'
        }
    },

    # ==================== TAXONOMY-TABLE RELATIONSHIPS ====================
    'TaxonomyTableVersion': {
        'target_table': 'framework_table',
        'column_mappings': {
            'TaxonomyID': 'FRAMEWORK_ID',
            'TableVID': 'TABLE_ID',
            'TemplateID': 'TEMPLATE_ID'
        },
        'additional_columns': {
            'MAINTENANCE_AGENCY_ID': lambda row: 'NODE',
            'IS_SIMPLE_REUSE': lambda row: transform_boolean(row.get('IsSimpleReuse')),
            'TABLE_GROUP_ID': lambda row: row.get('TableGroupID')
        }
    },

    # ==================== VALIDATION MAPPINGS ====================
    'ValidationScope': {
        'target_table': 'validation_rule_scope',
        'column_mappings': {
            'ValidationID': 'VALIDATION_RULE_ID',
            'TableVID': 'TABLE_ID',
            'Alternative': 'ALTERNATIVE'
        },
        'additional_columns': {
            'MAINTENANCE_AGENCY_ID': lambda row: 'NODE',
            'IS_ACTIVE': lambda row: 1
        }
    },

    # ==================== REFERENCE MAPPINGS ====================
    # 'ConceptReference': {
    #     'target_table': 'concept_reference',
    #     'column_mappings': {
    #         'ConceptID': 'CONCEPT_ID',
    #         'ReferenceID': 'REFERENCE_ID'
    #     },
    #     'additional_columns': {
    #         'MAINTENANCE_AGENCY_ID': lambda row: 'NODE',
    #         'REFERENCE_TYPE': lambda row: 'CONCEPT'
    #     }
    # },

    # ==================== INSTANCE LEVEL MAPPINGS ====================
    'InstanceLevelDataPoint': {
        'target_table': 'instance_datapoint',
        'column_mappings': {
            'InstanceLevelDataPointID': 'INSTANCE_DATAPOINT_ID',
            'DataPointVID': 'DATAPOINT_VERSION_ID'
        },
        'additional_columns': {
            'MAINTENANCE_AGENCY_ID': lambda row: 'NODE',
            'IS_ACTIVE': lambda row: 1
        }
    },

    # ==================== CORE METADATA ENTITIES ====================

    # Metric mappings
    'Metric': {
        'target_table': 'variable',
        'column_mappings': {
            'MetricID': 'VARIABLE_ID',
            'DataTypeID': 'DOMAIN_ID'
        },
        'additional_columns': {
            'MAINTENANCE_AGENCY_ID': lambda row: "EBA",
            'CODE': lambda row: transform_id(row.get('MetricID'), 'METRIC_'),
            'NAME': lambda row: f"Metric {row.get('MetricID')}",
            'DESCRIPTION': lambda row: f"Metric with data type {row.get('DataTypeID')} and flow type {row.get('FlowTypeID')}",
            'PRIMARY_CONCEPT': lambda row: None,
            'IS_DECOMPOSED': lambda row: transform_boolean(row.get('Additivity'))
        }
    },

    # Language mappings
    'Language': {
        'target_table': 'maintenance_agency',
        'column_mappings': {
            'LanguageID': 'MAINTENANCE_AGENCY_ID',
            'IsoCode': 'CODE',
            'EnglishName': 'NAME'
        },
        'additional_columns': {
            'DESCRIPTION': lambda row: f"{row.get('LanguageName')} ({row.get('IsoCode')})"
        }
    },

    # Taxonomy mappings
    'Taxonomy': {
        'target_table': 'framework',
        'column_mappings': {
            'TaxonomyID': 'FRAMEWORK_ID',
            'TaxonomyCode': 'CODE',
            'TaxonomyLabel': 'NAME',
            'FrameworkID': 'FRAMEWORK_ID'
        },
        'additional_columns': {
            'MAINTENANCE_AGENCY_ID': lambda row: "EBA",
            'DESCRIPTION': lambda row: f"{row.get('TaxonomyLabel')} - {row.get('TechnicalStandard')} (Published: {transform_taxonomy_date(row.get('ActualPublicationDate'),row)})",
            'FRAMEWORK_TYPE': lambda row: "TAXONOMY",
            'REPORTING_POPULATION': lambda row: None,
            'OTHER_LINKS': lambda row: transform_version(row.get('DpmPackageCode')),
            'ORDER': lambda row: row.get('TaxonomyID'),
            'FRAMEWORK_STATUS': lambda row: "ACTIVE"
        }
    },

    # DPM Package mappings
    'DpmPackage': {
        'target_table': 'framework',
        'column_mappings': {
            'DpmPackageCode': 'CODE'
        },
        'additional_columns': {
            'MAINTENANCE_AGENCY_ID': lambda row: "EBA",
            'FRAMEWORK_ID': lambda row: transform_id(row.get('DpmPackageCode'), 'PKG_'),
            'NAME': lambda row: f"DPM Package {row.get('DpmPackageCode')}",
            'DESCRIPTION': lambda row: f"Data Point Model Package version {row.get('DpmPackageCode')}",
            'FRAMEWORK_TYPE': lambda row: "PACKAGE",
            'REPORTING_POPULATION': lambda row: None,
            'OTHER_LINKS': lambda row: None,
            'ORDER': lambda row: None,
            'FRAMEWORK_STATUS': lambda row: "ACTIVE"
        }
    },

    # ==================== DOMAIN TYPE MAPPINGS ====================

    # FlowType mappings
    'FlowType': {
        'target_table': 'domain',
        'column_mappings': {
            'FlowTypeID': 'DOMAIN_ID',
            'FlowTypeCode': 'CODE',
            'FlowTypeLabel': 'NAME'
        },
        'additional_columns': {
            'MAINTENANCE_AGENCY_ID': lambda row: "EBA",
            'IS_ENUMERATED': lambda row: 1,
            'DESCRIPTION': lambda row: row.get('FlowTypeLabel', f"Flow type {row.get('FlowTypeCode')}"),
            'DATA_TYPE': lambda row: "string",
            'FACET_ID': lambda row: None,
            'IS_REFERENCE': lambda row: 0
        }
    },

    # BalanceType mappings
    'BalanceType': {
        'target_table': 'domain',
        'column_mappings': {
            'BalanceTypeID': 'DOMAIN_ID',
            'BalanceTypeCode': 'CODE',
            'BalanceTypeLabel': 'NAME'
        },
        'additional_columns': {
            'MAINTENANCE_AGENCY_ID': lambda row: "EBA",
            'IS_ENUMERATED': lambda row: 1,
            'DESCRIPTION': lambda row: row.get('BalanceTypeLabel', f"Balance type {row.get('BalanceTypeCode')}"),
            'DATA_TYPE': lambda row: "string",
            'FACET_ID': lambda row: None,
            'IS_REFERENCE': lambda row: 0
        }
    },

    # DataSign mappings
    'DataSign': {
        'target_table': 'domain',
        'column_mappings': {
            'DataSign': 'CODE'
        },
        'additional_columns': {
            'MAINTENANCE_AGENCY_ID': lambda row: "EBA",
            'DOMAIN_ID': lambda row: transform_id(row.get('DataSign'), 'SIGN_'),
            'NAME': lambda row: f"Data Sign: {row.get('DataSign')}",
            'IS_ENUMERATED': lambda row: 1,
            'DESCRIPTION': lambda row: f"Data sign indicator: {row.get('DataSign')}",
            'DATA_TYPE': lambda row: "string",
            'FACET_ID': lambda row: None,
            'IS_REFERENCE': lambda row: 0
        }
    },

    # AdditivityCode mappings
    'AdditivityCode': {
        'target_table': 'domain',
        'column_mappings': {
            'Additivity': 'CODE',
            'Description': 'DESCRIPTION'
        },
        'additional_columns': {
            'MAINTENANCE_AGENCY_ID': lambda row: "EBA",
            'DOMAIN_ID': lambda row: transform_id(row.get('Additivity'), 'ADD_'),
            'NAME': lambda row: f"Additivity: {row.get('Additivity')}",
            'IS_ENUMERATED': lambda row: 1,
            'DATA_TYPE': lambda row: "string",
            'FACET_ID': lambda row: None,
            'IS_REFERENCE': lambda row: 0
        }
    },

    # ==================== TABLE/CUBE GROUPING MAPPINGS ====================

    # TableGroup mappings
    'TableGroup': {
        'target_table': 'cube_group',
        'column_mappings': {
            'TableGroupID': 'CUBE_GROUP_ID',
            'TableGroupCode': 'CODE',
            'TableGroupLabel': 'NAME',
            'TaxonomyID': 'FRAMEWORK_ID'
        },
        'additional_columns': {
            'MAINTENANCE_AGENCY_ID': lambda row: "EBA",
            'DESCRIPTION': lambda row: row.get('TableGroupLabel'),
            'ORDER': lambda row: row.get('Order', 0)
        }
    },

    # TableGroupTemplates mappings
    'TableGroupTemplates': {
        'target_table': 'cube_group_enumeration',
        'column_mappings': {
            'TableGroupID': 'CUBE_GROUP_ID',
            'TemplateID': 'CUBE_STRUCTURE_ID'
        },
        'additional_columns': {
            'MAINTENANCE_AGENCY_ID': lambda row: "EBA",
            'VALID_FROM': lambda row: transform_taxonomy_date(row.get('FromDate'),row) or '1900-01-01',
            'VALID_TO': lambda row: transform_taxonomy_date(row.get('ToDate'),row) or '9999-12-31',
            'ORDER': lambda row: 0
        }
    },

    # TemplateGroup mappings
    'TemplateGroup': {
        'target_table': 'cube_group',
        'column_mappings': {
            'TemplateGroupID': 'CUBE_GROUP_ID',
            'TemplateGroupCode': 'CODE',
            'TemplateGroupLabel': 'NAME',
            'FrameworkID': 'FRAMEWORK_ID'
        },
        'additional_columns': {
            'MAINTENANCE_AGENCY_ID': lambda row: "EBA",
            'DESCRIPTION': lambda row: row.get('TemplateGroupLabel'),
            'ORDER': lambda row: row.get('TemplateGroupID', 0)
        }
    },

    # TemplateGroupTemplate mappings
    'TemplateGroupTemplate': {
        'target_table': 'cube_group_enumeration',
        'column_mappings': {
            'TemplateGroupID': 'CUBE_GROUP_ID',
            'TemplateID': 'CUBE_STRUCTURE_ID'
        },
        'additional_columns': {
            'MAINTENANCE_AGENCY_ID': lambda row: "EBA",
            'VALID_FROM': lambda row: transform_taxonomy_date(row.get('FromDate'),row) or '1900-01-01',
            'VALID_TO': lambda row: transform_taxonomy_date(row.get('ToDate'),row) or '9999-12-31',
            'ORDER': lambda row: 0
        }
    },

    # ==================== EXPRESSION AND VALIDATION MAPPINGS ====================

    # Expression mappings
    'Expression': {
        'target_table': 'mapping_definition',
        'column_mappings': {
            'ExpressionID': 'MAPPING_ID',
            'ExpressionType': 'MAPPING_TYPE'
        },
        'additional_columns': {
            'MAINTENANCE_AGENCY_ID': lambda row: "EBA",
            'CODE': lambda row: transform_id(row.get('ExpressionID'), 'EXPR_'),
            'NAME': lambda row: f"Expression {row.get('ExpressionID')}",
            'ALGORITHM': lambda row: transform_expression(row.get('TableBasedFormula')),
            'MEMBER_MAPPING_ID': lambda row: None,
            'VARIABLE_MAPPING_ID': lambda row: None
        }
    },

    # ExpressionScope mappings
    'ExpressionScope': {
        'target_table': 'mapping_to_cube',
        'column_mappings': {
            'ExpressionID': 'MAPPING_ID',
            'ModuleID': 'CUBE_ID'
        },
        'additional_columns': {
            'MAINTENANCE_AGENCY_ID': lambda row: "EBA",
            'VALID_FROM': lambda row: '1900-01-01',
            'VALID_TO': lambda row: '9999-12-31'
        }
    },

    # ValidationRuleSet mappings
    'ValidationRuleSet': {
        'target_table': 'mapping_to_cube',
        'column_mappings': {
            'ValidationRuleId': 'MAPPING_ID',
            'ModuleID': 'CUBE_ID'
        },
        'additional_columns': {
            'MAINTENANCE_AGENCY_ID': lambda row: "EBA",
            'VALID_FROM': lambda row: '1900-01-01',
            'VALID_TO': lambda row: '9999-12-31'
        }
    },

    # HierarchyValidationRule mappings
    'HierarchyValidationRule': {
        'target_table': 'member_mapping',
        'column_mappings': {
            'HierarchyID': 'MEMBER_HIERARCHY_ID',
            'ValidationRuleId': 'MAPPING_ID'
        },
        'additional_columns': {
            'MAINTENANCE_AGENCY_ID': lambda row: "EBA",
            'CODE': lambda row: f"HVR_{row.get('HierarchyID')}_{row.get('ValidationRuleId')}",
            'NAME': lambda row: f"Hierarchy Validation Rule {row.get('ValidationRuleId')}",
            'MAPPING_TYPE': lambda row: "HIERARCHY_VALIDATION",
            'MEMBER_MAPPING_ID': lambda row: None,
            'VARIABLE_MAPPING_ID': lambda row: None
        }
    },

    # ==================== VARIABLE RELATIONSHIP MAPPINGS ====================

    # MetricVariable mappings
    'MetricVariable': {
        'target_table': 'variable_mapping_item',
        'column_mappings': {
            'ExpressionID': 'VARIABLE_MAPPING_ID',
            'MetricID': 'VARIABLE_ID'
        },
        'additional_columns': {
            'VARIABLE_SET_ID': lambda row: None,
            'MEMBER_ID': lambda row: None,
            'MEMBER_HIERARCHY_ID': lambda row: None,
            'SUBDOMAIN_ID': lambda row: None
        }
    },

    # OrdinateVariable mappings
    'OrdinateVariable': {
        'target_table': 'variable_mapping_item',
        'column_mappings': {
            'ExpressionID': 'VARIABLE_MAPPING_ID',
            'OrdinateID': 'VARIABLE_ID'
        },
        'additional_columns': {
            'VARIABLE_SET_ID': lambda row: None,
            'MEMBER_ID': lambda row: None,
            'MEMBER_HIERARCHY_ID': lambda row: None,
            'SUBDOMAIN_ID': lambda row: None
        }
    },

    # SpecificCellVariable mappings
    'SpecificCellVariable': {
        'target_table': 'variable_mapping_item',
        'column_mappings': {
            'CellID': 'VARIABLE_ID',
            'VariableID': 'VARIABLE_MAPPING_ID'
        },
        'additional_columns': {
            'VARIABLE_SET_ID': lambda row: None,
            'MEMBER_ID': lambda row: None,
            'MEMBER_HIERARCHY_ID': lambda row: None,
            'SUBDOMAIN_ID': lambda row: None
        }
    },

    # ==================== CONTEXT AND REFERENCE MAPPINGS ====================

    # ContextDefinition mappings
    'ContextDefinition': {
        'target_table': 'combination',
        'column_mappings': {
            'ContextID': 'COMBINATION_ID'
        },
        'additional_columns': {
            'CODE': lambda row: transform_id(row.get('ContextID'), 'CTX_'),
            'NAME': lambda row: f"Context {row.get('ContextID')}",
            'MAINTENANCE_AGENCY_ID': lambda row: "EBA",
            'METRIC': lambda row: None,
            'VERSION': lambda row: "1.0",
            'VALID_FROM': lambda row: '1900-01-01',
            'VALID_TO': lambda row: '9999-12-31'
        }
    },

    # ReferenceSources mappings
    'ReferenceSources': {
        'target_table': 'maintenance_agency',
        'column_mappings': {
            'ReferenceSourceID': 'MAINTENANCE_AGENCY_ID',
            'ReferenceSourceCode': 'CODE',
            'ReferenceSourceName': 'NAME'
        },
        'additional_columns': {
            'DESCRIPTION': lambda row: row.get('ReferenceSourceDescription', row.get('ReferenceSourceName'))
        }
    },

    # References mappings
    'References': {
        'target_table': 'member_mapping',
        'column_mappings': {
            'ReferenceID': 'MAPPING_ID',
            'ReferenceCode': 'CODE',
            'ReferenceLabel': 'NAME'
        },
        'additional_columns': {
            'MAINTENANCE_AGENCY_ID': lambda row: "EBA",
            'MAPPING_TYPE': lambda row: "REFERENCE",
            'MEMBER_HIERARCHY_ID': lambda row: None,
            'MEMBER_MAPPING_ID': lambda row: None,
            'VARIABLE_MAPPING_ID': lambda row: None
        }
    },

    # ==================== SPECIALIZED ENTITY MAPPINGS ====================

    # StringList mappings
    'StringList': {
        'target_table': 'facet_collection',
        'column_mappings': {
            'StringListID': 'FACET_COLLECTION_ID',
            'StringListCode': 'CODE'
        },
        'additional_columns': {
            'MAINTENANCE_AGENCY_ID': lambda row: "EBA",
            'NAME': lambda row: row.get('StringListCode'),
            'DESCRIPTION': lambda row: f"String list collection {row.get('StringListCode')}"
        }
    },

    # StringListValues mappings
    'StringListValues': {
        'target_table': 'facet_enumeration',
        'column_mappings': {
            'StringListID': 'FACET_COLLECTION_ID',
            'StringValue': 'CODE'
        },
        'additional_columns': {
            'MAINTENANCE_AGENCY_ID': lambda row: "EBA",
            'NAME': lambda row: row.get('StringValue'),
            'DESCRIPTION': lambda row: row.get('StringValue'),
            'ORDER': lambda row: 0
        }
    },

    # ModuleDataPointRestriction mappings
    'ModuleDataPointRestriction': {
        'target_table': 'cube_mapping',
        'column_mappings': {
            'ModuleID': 'CUBE_ID',
            'DataPointID': 'COMBINATION_ID'
        },
        'additional_columns': {
            'MAINTENANCE_AGENCY_ID': lambda row: "EBA",
            'CUBE_MAPPING_ID': lambda row: transform_id(f"{row.get('ModuleID')}_{row.get('DataPointID')}", 'MDPR_'),
            'MAPPING_TYPE': lambda row: "RESTRICTION",
            'VALID_FROM': lambda row: '1900-01-01',
            'VALID_TO': lambda row: '9999-12-31'
        }
    },

    # OpenAxisValueRestriction mappings
    'OpenAxisValueRestriction': {
        'target_table': 'cube_structure_mapping',
        'column_mappings': {
            'AxisID': 'CUBE_STRUCTURE_ID',
            'MemberID': 'MEMBER_ID'
        },
        'additional_columns': {
            'MAINTENANCE_AGENCY_ID': lambda row: "EBA",
            'CUBE_STRUCTURE_MAPPING_ID': lambda row: transform_id(f"{row.get('AxisID')}_{row.get('MemberID')}", 'OAVR_'),
            'MAPPING_TYPE': lambda row: "AXIS_RESTRICTION",
            'VALID_FROM': lambda row: '1900-01-01',
            'VALID_TO': lambda row: '9999-12-31'
        }
    },

    # OpenMemberRestriction mappings
    'OpenMemberRestriction': {
        'target_table': 'member_mapping',
        'column_mappings': {
            'DomainID': 'MEMBER_HIERARCHY_ID',
            'MemberID': 'MEMBER_ID'
        },
        'additional_columns': {
            'MAINTENANCE_AGENCY_ID': lambda row: "EBA",
            'MAPPING_ID': lambda row: transform_id(f"{row.get('DomainID')}_{row.get('MemberID')}", 'OMR_'),
            'CODE': lambda row: f"OMR_{row.get('DomainID')}_{row.get('MemberID')}",
            'NAME': lambda row: f"Open Member Restriction {row.get('MemberID')}",
            'MAPPING_TYPE': lambda row: "MEMBER_RESTRICTION",
            'MEMBER_MAPPING_ID': lambda row: None,
            'VARIABLE_MAPPING_ID': lambda row: None
        }
    }
}

PRIORITY_ORDER = [
    # ==================== BASE ENTITIES (NO DEPENDENCIES) ====================
    'Organisation.csv',          # MAINTENANCE_AGENCY
    'Owner.csv',                 # MAINTENANCE_AGENCY
    'Concept.csv',               # MAINTENANCE_AGENCY (for concepts)
    'Language.csv',              # MAINTENANCE_AGENCY (language support)
    'ReferenceSources.csv',      # MAINTENANCE_AGENCY (reference sources)

    # Domain and basic data types
    'Domain.csv',                # DOMAIN
    'DataType.csv',              # DOMAIN (data type domains)
    'FlowType.csv',              # DOMAIN (flow types)
    'BalanceType.csv',           # DOMAIN (balance types)
    'DataSign.csv',              # DOMAIN (data signs)
    'AdditivityCode.csv',        # DOMAIN (additivity codes)

    # Framework and categorization
    'Category.csv',              # VARIABLE_SET
    'Framework.csv',             # FRAMEWORK
    'ReportingFramework.csv',    # FRAMEWORK
    'ConceptualModule.csv',      # FRAMEWORK (conceptual modules)
    'Taxonomy.csv',              # FRAMEWORK (taxonomies)
    'DpmPackage.csv',            # FRAMEWORK (packages)

    # ==================== ENTITIES WITH MINIMAL DEPENDENCIES ====================
    'Member.csv',                # MEMBER (depends on DOMAIN, MAINTENANCE_AGENCY)
    'Variable.csv',              # VARIABLE
    'VariableVersion.csv',       # VARIABLE
    'Dimension.csv',             # VARIABLE (dimensions as variables)
    'Metric.csv',                # VARIABLE (metrics as variables)
    'SubCategory.csv',           # SUBDOMAIN (depends on DOMAIN)
    'Hierarchy.csv',             # MEMBER_HIERARCHY (depends on DOMAIN)
    'Template.csv',              # CUBE_STRUCTURE

    # Facet collections and enumerations
    'StringList.csv',            # FACET_COLLECTION
    'StringListValues.csv',      # FACET_ENUMERATION (depends on FACET_COLLECTION)

    # ==================== FRAMEWORK RELATIONSHIPS ====================
    'ConceptualModuleTemplate.csv', # FRAMEWORK_SUBDOMAIN (depends on FRAMEWORK, TEMPLATE)

    # ==================== GROUPING ENTITIES ====================
    'TableGroup.csv',            # CUBE_GROUP (depends on FRAMEWORK)
    'TemplateGroup.csv',         # CUBE_GROUP (depends on FRAMEWORK)
    'TableGroupTemplates.csv',   # CUBE_GROUP_ENUMERATION (depends on CUBE_GROUP, TEMPLATE)
    'TemplateGroupTemplate.csv', # CUBE_GROUP_ENUMERATION (depends on CUBE_GROUP, TEMPLATE)

    # ==================== COMPLEX ENTITIES ====================
    'HierarchyNode.csv',         # MEMBER_HIERARCHY_NODE (depends on HIERARCHY, MEMBER)
    'Module.csv',                # CUBE (depends on FRAMEWORK)
    'ModuleVersion.csv',         # CUBE (depends on FRAMEWORK)
    'Table.csv',                 # TABLE (enhanced with template info)
    'TableVersion.csv',          # TABLE
    'Axis.csv',                  # AXIS (depends on TABLE)


    # ==================== COMPLEX MAPPING ENTITIES ====================
    'AxisOrdinate.csv',          # AXIS_ORDINATE (depends on AXIS)
    'AxisOrdinateTrace.csv',     # AXIS_ORDINATE
    'OrdinateItem.csv',          # ORDINATE_ITEM (depends on AXIS_ORDINATE, VARIABLE, MEMBER)
    'Cell.csv',                  # TABLE_CELL (depends on TABLE)
    'TableCell.csv',             # TABLE_CELL (depends on TABLE)
    'CellPosition.csv',          # CELL_POSITION (depends on TABLE_CELL, AXIS_ORDINATE)

    # ==================== DATA POINTS AND COMBINATIONS ====================
    'DataPoint.csv',             # COMBINATION (base data points)
    'DataPointVersion.csv',      # COMBINATION
    'ContextDefinition.csv',     # COMBINATION (context definitions)
    'CombinationItem.csv',       # COMBINATION_ITEM (depends on COMBINATION)

    # ==================== SCOPE AND RELATIONSHIP MAPPINGS ====================

    'ContextOfDataPoints.csv',   # CUBE_TO_COMBINATION (depends on CUBE, COMBINATION)
    'HierarchyValidationRule.csv', # MEMBER_MAPPING (depends on HIERARCHY, VALIDATION)

    # ==================== RESTRICTION AND CONSTRAINT MAPPINGS ====================
    'ModuleDataPointRestriction.csv', # CUBE_MAPPING (depends on CUBE, COMBINATION)
    'OpenAxisValueRestriction.csv',   # CUBE_STRUCTURE_MAPPING (depends on AXIS, MEMBER)
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
    'ExpressionScope', 'Cell', 'ModuleTableOrGroup', 'Item', 'Language', 'Hierarchy',
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
     'member_hierarchy_node',
    'facet_collection', 'axis', 'subdomain', 'axis_ordinate', 'cube_structure', 'framework',
    'member_hierarchy', 'table', 'combination',
    'combination_item', 'cube_structure_item', 'cube_to_combination',
    'maintenance_agency', 'member', 'variable', 'cube', 'ordinate_item',
    'table_cell',  'domain', 'variable_set',
    'subdomain_enumeration', 'cell_position', 'variable_set_enumeration'
]
