
from django.db import models

		
class SDDModule(models.Model):


    dependencies = models.ForeignKey("SDDModule", models.SET_NULL,blank=True,null=True,) 

    theDescription = models.CharField("theDescription",max_length=255,default=None, blank=True, null=True)   

    license = models.CharField("license",max_length=255,default=None, blank=True, null=True)   

    name = models.CharField("name",max_length=255, primary_key=True)   

    version = models.CharField("version",max_length=255,default=None, blank=True, null=True)   

	
		
class DOMAIN(models.Model):


    code = models.CharField("code",max_length=255,default=None, blank=True, null=True)   

    FACET_VALUE_TYPE = {"BigInteger":"BigInteger",
"Boolean":"Boolean",
"DateTime":"DateTime",
"Day_MonthDay_Month":"DayMonthDayMonth",
"Decimal":"Decimal",
"Double":"Double",
"Duration":"Duration",
"Float":"Float",
"GregorianDay":"GregorianDay",
"GregorianMonth":"GregorianMonth",
"GregorianYear":"GregorianYear",
"Integer":"Integer",
"Long":"Long",
"Short":"Short",
"String":"String",
"Time":"Time",
"URI":"URI",
} 
    data_type = models.CharField("data_type",max_length=255, choices=FACET_VALUE_TYPE,default=None, blank=True, null=True)   

    description = models.CharField("description",max_length=255,default=None, blank=True, null=True)   

    domain_id = models.CharField("domain_id",max_length=255,default=None, blank=True, null=True)   

    facet_id = models.ForeignKey("FACET_COLLECTION", models.SET_NULL,blank=True,null=True,) 

    is_enumerated = models.BooleanField("is_enumerated",default=None, blank=True, null=True)   

    is_reference = models.BooleanField("is_reference",default=None, blank=True, null=True)   

    maintenance_agency_id = models.ForeignKey("MAINTENANCE_AGENCY", models.SET_NULL,blank=True,null=True,) 

    name = models.CharField("name",max_length=255, primary_key=True)   

    displayName = models.CharField("displayName",max_length=255,default=None, blank=True, null=True)   

	
		
class FACET_COLLECTION(models.Model):


    code = models.CharField("code",max_length=255,default=None, blank=True, null=True)   

    facet_id = models.CharField("facet_id",max_length=255, primary_key=True)   

    FACET_VALUE_TYPE = {"BigInteger":"BigInteger",
"Boolean":"Boolean",
"DateTime":"DateTime",
"Day_MonthDay_Month":"DayMonthDayMonth",
"Decimal":"Decimal",
"Double":"Double",
"Duration":"Duration",
"Float":"Float",
"GregorianDay":"GregorianDay",
"GregorianMonth":"GregorianMonth",
"GregorianYear":"GregorianYear",
"Integer":"Integer",
"Long":"Long",
"Short":"Short",
"String":"String",
"Time":"Time",
"URI":"URI",
} 
    facet_value_type = models.CharField("facet_value_type",max_length=255, choices=FACET_VALUE_TYPE,default=None, blank=True, null=True)   

    maintenance_agency_id = models.ForeignKey("MAINTENANCE_AGENCY", models.SET_NULL,blank=True,null=True,) 

    name = models.CharField("name",max_length=255,default=None, blank=True, null=True)   

	
		
class FACET_ENUMERATION(models.Model):


    facet_id = models.ForeignKey("FACET_COLLECTION", models.SET_NULL,blank=True,null=True,) 

    facet_type = models.ForeignKey("facet_type", models.SET_NULL,blank=True,null=True,) 

    observation_value = models.CharField("observation_value",max_length=255,default=None, blank=True, null=True)   

	
		
class facet_type(models.Model):


    decimals = models.BigIntegerField("decimals",default=None, blank=True, null=True)   

    endTime = models.CharField("endTime",max_length=255,default=None, blank=True, null=True)   

    endValue = models.BigIntegerField("endValue",default=None, blank=True, null=True)   

    interval = models.CharField("interval",max_length=255,default=None, blank=True, null=True)   

    isSequence = models.CharField("isSequence",max_length=255,default=None, blank=True, null=True)   

    maxLength = models.BigIntegerField("maxLength",default=None, blank=True, null=True)   

    maxValue = models.BigIntegerField("maxValue",default=None, blank=True, null=True)   

    minLength = models.BigIntegerField("minLength",default=None, blank=True, null=True)   

    minValue = models.BigIntegerField("minValue",default=None, blank=True, null=True)   

    pattern = models.CharField("pattern",max_length=255,default=None, blank=True, null=True)   

    startTime = models.CharField("startTime",max_length=255,default=None, blank=True, null=True)   

    startValue = models.BigIntegerField("startValue",default=None, blank=True, null=True)   

    timeInterval = models.CharField("timeInterval",max_length=255,default=None, blank=True, null=True)   

	
		
class MAINTENANCE_AGENCY(models.Model):


    code = models.CharField("code",max_length=255,default=None, blank=True, null=True)   

    maintenance_agency_id = models.CharField("maintenance_agency_id",max_length=255,default=None, blank=True, null=True)   

    name = models.CharField("name",max_length=255, primary_key=True)   

	
		
class MEMBER(models.Model):


    code = models.CharField("code",max_length=255,default=None, blank=True, null=True)   

    description = models.CharField("description",max_length=255,default=None, blank=True, null=True)   

    domain_id = models.ForeignKey("DOMAIN", models.SET_NULL,blank=True,null=True,) 

    maintenance_agency_id = models.ForeignKey("MAINTENANCE_AGENCY", models.SET_NULL,blank=True,null=True,) 

    member_id = models.CharField("member_id",max_length=255,default=None, blank=True, null=True)   

    name = models.CharField("name",max_length=255, primary_key=True)   

    displayName = models.CharField("displayName",max_length=255,default=None, blank=True, null=True)   

	
		
class MEMBER_HIERARCHY(models.Model):


    code = models.CharField("code",max_length=255,default=None, blank=True, null=True)   

    description = models.CharField("description",max_length=255,default=None, blank=True, null=True)   

    domain_id = models.ForeignKey("DOMAIN", models.SET_NULL,blank=True,null=True,) 

    maintenance_agency_id = models.ForeignKey("MAINTENANCE_AGENCY", models.SET_NULL,blank=True,null=True,) 

    member_hierarchy_id = models.CharField("member_hierarchy_id",max_length=255,default=None, blank=True, null=True)   

    name = models.CharField("name",max_length=255, primary_key=True)   

	
		
class MEMBER_HIERARCHY_NODE(models.Model):


    comparator = models.CharField("comparator",max_length=255,default=None, blank=True, null=True)   

    level = models.BigIntegerField("level",default=None, blank=True, null=True)   

    member_hierarchy_id = models.ForeignKey("MEMBER_HIERARCHY", models.SET_NULL,blank=True,null=True,) 

    member_id = models.ForeignKey("MEMBER", models.SET_NULL,blank=True,null=True,related_name='member_id_in_hierarchy') 

    operator = models.CharField("operator",max_length=255,default=None, blank=True, null=True)   

    parent_member_id = models.ForeignKey("MEMBER", models.SET_NULL,blank=True,null=True,related_name='parent_member_id') 

    valid_from = models.DateTimeField("valid_from",default=None, blank=True, null=True)   

    valid_to = models.DateTimeField("valid_to",default=None, blank=True, null=True)   

	
		
class SUBDOMAIN(models.Model):


    code = models.CharField("code",max_length=255,default=None, blank=True, null=True)   

    description = models.CharField("description",max_length=255,default=None, blank=True, null=True)   

    domain_id = models.ForeignKey("DOMAIN", models.SET_NULL,blank=True,null=True,) 

    facet_id = models.ForeignKey("FACET_COLLECTION", models.SET_NULL,blank=True,null=True,) 

    is_listed = models.BooleanField("is_listed",default=None, blank=True, null=True)   

    is_natural = models.BooleanField("is_natural",default=None, blank=True, null=True)   

    maintenance_agency_id = models.ForeignKey("MAINTENANCE_AGENCY", models.SET_NULL,blank=True,null=True,) 

    subdomain_id = models.CharField("subdomain_id",max_length=255, primary_key=True)   

    items = models.ForeignKey("SUBDOMAIN_ENUMERATION", models.SET_NULL,blank=True,null=True,) 

	
		
class SUBDOMAIN_ENUMERATION(models.Model):


    member_id = models.ForeignKey("MEMBER", models.SET_NULL,blank=True,null=True,) 

    order = models.BigIntegerField("order",default=None, blank=True, null=True)   

    valid_from = models.DateTimeField("valid_from",default=None, blank=True, null=True)   

    valid_to = models.DateTimeField("valid_to",default=None, blank=True, null=True)   

	
		
class VARIABLE(models.Model):


    code = models.CharField("code",max_length=255,default=None, blank=True, null=True)   

    description = models.CharField("description",max_length=255,default=None, blank=True, null=True)   

    domain_id = models.ForeignKey("DOMAIN", models.SET_NULL,blank=True,null=True,) 

    maintenance_agency_id = models.ForeignKey("MAINTENANCE_AGENCY", models.SET_NULL,blank=True,null=True,) 

    primary_concept = models.CharField("primary_concept",max_length=255,default=None, blank=True, null=True)   

    variable_id = models.CharField("variable_id",max_length=255,default=None, blank=True, null=True)   

    name = models.CharField("name",max_length=255, primary_key=True)   

    displayName = models.CharField("displayName",max_length=255,default=None, blank=True, null=True)   

	
		
class VARIABLE_SET(models.Model):


    code = models.CharField("code",max_length=255,default=None, blank=True, null=True)   

    description = models.CharField("description",max_length=255,default=None, blank=True, null=True)   

    maintenance_agency_id = models.ForeignKey("MAINTENANCE_AGENCY", models.SET_NULL,blank=True,null=True,related_name='variable_set_maintenance_agency_id') 

    name = models.CharField("name",max_length=255,default=None, blank=True, null=True)   

    variable_set_id = models.CharField("variable_set_id",max_length=255, primary_key=True)   

    variable_set_items = models.ForeignKey("VARIABLE_SET_ENUMERATION", models.SET_NULL,blank=True,null=True,) 

	
		
class VARIABLE_SET_ENUMERATION(models.Model):


    is_flow = models.BooleanField("is_flow",default=None, blank=True, null=True)   

    order = models.BigIntegerField("order",default=None, blank=True, null=True)   

    subdomain_id = models.ForeignKey("SUBDOMAIN", models.SET_NULL,blank=True,null=True,) 

    valid_from = models.DateTimeField("valid_from",default=None, blank=True, null=True)   

    valid_to = models.DateTimeField("valid_to",default=None, blank=True, null=True)   

    variable_id = models.ForeignKey("VARIABLE", models.SET_NULL,blank=True,null=True,) 

	
		
class COMBINATION(models.Model):


    code = models.CharField("code",max_length=255,default=None, blank=True, null=True)   

    combination_id = models.CharField("combination_id",max_length=255,default=None, blank=True, null=True)   

    maintenance_agency_id = models.ForeignKey("MAINTENANCE_AGENCY", models.SET_NULL,blank=True,null=True,) 

    name = models.CharField("name",max_length=255, primary_key=True)   

    valid_from = models.DateTimeField("valid_from",default=None, blank=True, null=True)   

    valid_to = models.DateTimeField("valid_to",default=None, blank=True, null=True)   

    version = models.CharField("version",max_length=255,default=None, blank=True, null=True)   

    combination_items = models.ForeignKey("COMBINATION_ITEM", models.SET_NULL,blank=True,null=True,) 

	
		
class COMBINATION_ITEM(models.Model):


    member_id = models.ForeignKey("MEMBER", models.SET_NULL,blank=True,null=True,) 

    subdomain_id = models.ForeignKey("SUBDOMAIN", models.SET_NULL,blank=True,null=True,) 

    variable_id = models.ForeignKey("VARIABLE", models.SET_NULL,blank=True,null=True,) 

    variable_set_id = models.ForeignKey("VARIABLE_SET", models.SET_NULL,blank=True,null=True,) 

	
		
class CUBE(models.Model):


    code = models.CharField("code",max_length=255,default=None, blank=True, null=True)   

    cube_id = models.CharField("cube_id",max_length=255,default=None, blank=True, null=True)   

    cube_structure_id = models.ForeignKey("CUBE_STRUCTURE", models.SET_NULL,blank=True,null=True,) 

    cube_type = models.CharField("cube_type",max_length=255,default=None, blank=True, null=True)   

    description = models.CharField("description",max_length=255,default=None, blank=True, null=True)   

    framework_id = models.ForeignKey("FRAMEWORK", models.SET_NULL,blank=True,null=True,) 

    is_allowed = models.BooleanField("is_allowed",default=None, blank=True, null=True)   

    maintenance_agency_id = models.ForeignKey("MAINTENANCE_AGENCY", models.SET_NULL,blank=True,null=True,) 

    name = models.CharField("name",max_length=255, primary_key=True)   

    published = models.BooleanField("published",default=None, blank=True, null=True)   

    valid_from = models.DateTimeField("valid_from",default=None, blank=True, null=True)   

    valid_to = models.DateTimeField("valid_to",default=None, blank=True, null=True)   

    version = models.CharField("version",max_length=255,default=None, blank=True, null=True)   

    displayName = models.CharField("displayName",max_length=255,default=None, blank=True, null=True)   

	
		
class CUBE_GROUP(models.Model):


    code = models.CharField("code",max_length=255,default=None, blank=True, null=True)   

    cube_group_id = models.CharField("cube_group_id",max_length=255, primary_key=True)   

    description = models.CharField("description",max_length=255,default=None, blank=True, null=True)   

    maintenance_agency_id = models.ForeignKey("MAINTENANCE_AGENCY", models.SET_NULL,blank=True,null=True,) 

    name = models.CharField("name",max_length=255,default=None, blank=True, null=True)   

	
		
class CUBE_GROUP_ENUMERATION(models.Model):


    cube_group_id = models.ForeignKey("CUBE_GROUP", models.SET_NULL,blank=True,null=True,) 

    cube_id = models.ForeignKey("CUBE", models.SET_NULL,blank=True,null=True,) 

    order = models.BigIntegerField("order",default=None, blank=True, null=True)   

    valid_from = models.DateTimeField("valid_from",default=None, blank=True, null=True)   

    valid_to = models.DateTimeField("valid_to",default=None, blank=True, null=True)   

	
		
class CUBE_HIERARCHY(models.Model):


    code = models.CharField("code",max_length=255,default=None, blank=True, null=True)   

    cube_hierarchy_id = models.CharField("cube_hierarchy_id",max_length=255,default=None, blank=True, null=True)   

    cube_hierarchy_type = models.CharField("cube_hierarchy_type",max_length=255,default=None, blank=True, null=True)   

    framework_id = models.ForeignKey("FRAMEWORK", models.SET_NULL,blank=True,null=True,) 

    maintenance_agency_id = models.ForeignKey("MAINTENANCE_AGENCY", models.SET_NULL,blank=True,null=True,) 

    name = models.CharField("name",max_length=255,default=None, blank=True, null=True)   

	
		
class CUBE_HIERARCHY_NODE(models.Model):


    cube_group_id = models.ForeignKey("CUBE_GROUP", models.SET_NULL,blank=True,null=True,) 

    cube_hierarchy_id = models.ForeignKey("CUBE_HIERARCHY", models.SET_NULL,blank=True,null=True,) 

    level = models.BigIntegerField("level",default=None, blank=True, null=True)   

    nODE_CODE = models.CharField("nODE_CODE",max_length=255, primary_key=True)   

    node_name = models.CharField("node_name",max_length=255,default=None, blank=True, null=True)   

    order = models.BigIntegerField("order",default=None, blank=True, null=True)   

    parent_node_code = models.ForeignKey("CUBE_HIERARCHY_NODE", models.SET_NULL,blank=True,null=True,) 

    valid_from = models.DateTimeField("valid_from",default=None, blank=True, null=True)   

    valid_to = models.DateTimeField("valid_to",default=None, blank=True, null=True)   

	
		
class CUBE_RELATIONSHIP(models.Model):


    code = models.CharField("code",max_length=255,default=None, blank=True, null=True)   

    cube_relationship_id = models.CharField("cube_relationship_id",max_length=255,default=None, blank=True, null=True)   

    description = models.CharField("description",max_length=255,default=None, blank=True, null=True)   

    establishes_integrity = models.BooleanField("establishes_integrity",default=None, blank=True, null=True)   

    foreign_cube_id = models.ForeignKey("CUBE", models.SET_NULL,blank=True,null=True,related_name='foreign_cube',) 

    foreign_cube_variable_code = models.ForeignKey("VARIABLE", models.SET_NULL,blank=True,null=True,related_name='foreign_cube_variable_code') 

    maintenance_agency_id = models.ForeignKey("MAINTENANCE_AGENCY", models.SET_NULL,blank=True,null=True,) 

    name = models.CharField("name",max_length=255,default=None, blank=True, null=True)   

    primary_cube_id = models.ForeignKey("CUBE", models.SET_NULL,blank=True,null=True,related_name='primary_cube',) 

    primary_cube_variable_id = models.ForeignKey("VARIABLE", models.SET_NULL,blank=True,null=True,related_name='primary_cube_variable_id') 

    valid_from = models.DateTimeField("valid_from",default=None, blank=True, null=True)   

    valid_to = models.DateTimeField("valid_to",default=None, blank=True, null=True)   

    version = models.CharField("version",max_length=255,default=None, blank=True, null=True)   

	
		
class CUBE_STRUCTURE(models.Model):


    code = models.CharField("code",max_length=255,default=None, blank=True, null=True)   

    cube_structure_id = models.CharField("cube_structure_id",max_length=255,default=None, blank=True, null=True)   

    description = models.CharField("description",max_length=255,default=None, blank=True, null=True)   

    maintenance_agency_id = models.ForeignKey("MAINTENANCE_AGENCY", models.SET_NULL,blank=True,null=True,) 

    name = models.CharField("name",max_length=255, primary_key=True)   

    valid_from = models.DateTimeField("valid_from",default=None, blank=True, null=True)   

    valid_to = models.DateTimeField("valid_to",default=None, blank=True, null=True)   

    version = models.CharField("version",max_length=255,default=None, blank=True, null=True)   

    displayName = models.CharField("displayName",max_length=255,default=None, blank=True, null=True)   

	
		
class CUBE_STRUCTURE_ITEM(models.Model):


    attribute_associated_variable = models.ForeignKey("VARIABLE", models.SET_NULL,blank=True,null=True,related_name='attribute_associated_variable') 

    cube_structure_id = models.ForeignKey("CUBE_STRUCTURE", models.SET_NULL,blank=True,null=True,) 

    description = models.CharField("description",max_length=255,default=None, blank=True, null=True)   

    TYP_DMNSN = {"B":"B",
"M":"M",
"T":"T",
"U":"U",
} 
    dimension_type = models.CharField("dimension_type",max_length=255, choices=TYP_DMNSN,default=None, blank=True, null=True)   

    is_flow = models.BooleanField("is_flow",default=None, blank=True, null=True)   

    is_mandatory = models.BooleanField("is_mandatory",default=None, blank=True, null=True)   

    member_id = models.ForeignKey("MEMBER", models.SET_NULL,blank=True,null=True,) 

    order = models.BigIntegerField("order",default=None, blank=True, null=True)   

    TYP_RL = {"O":"O",
"A":"A",
"D":"D",
} 
    role = models.CharField("role",max_length=255, choices=TYP_RL,default=None, blank=True, null=True)   

    subdomain_id = models.ForeignKey("SUBDOMAIN", models.SET_NULL,blank=True,null=True,) 

    variable_id = models.ForeignKey("VARIABLE", models.SET_NULL,blank=True,null=True,) 

    variable_set_id = models.ForeignKey("VARIABLE_SET", models.SET_NULL,blank=True,null=True,) 

    isIdentifier = models.BooleanField("isIdentifier",default=None, blank=True, null=True)   

    cube_variable_code = models.CharField("cube_variable_code",max_length=255,default=None, blank=True, null=True)   

	
		
class CUBE_TO_COMBINATION(models.Model):


    combination_id = models.ForeignKey("COMBINATION", models.SET_NULL,blank=True,null=True,) 

    cube_id = models.ForeignKey("CUBE", models.SET_NULL,blank=True,null=True,) 

	
		
class FRAMEWORK(models.Model):


    code = models.CharField("code",max_length=255,default=None, blank=True, null=True)   

    description = models.CharField("description",max_length=255,default=None, blank=True, null=True)   

    framework_id = models.CharField("framework_id",max_length=255, primary_key=True)   

    maintenance_agency_id = models.ForeignKey("MAINTENANCE_AGENCY", models.SET_NULL,blank=True,null=True,) 

    name = models.CharField("name",max_length=255,default=None, blank=True, null=True)   

	
		
class FRAMEWORK_SUBDOMAIN(models.Model):


    framework_id = models.ForeignKey("FRAMEWORK", models.SET_NULL,blank=True,null=True,) 

    subdomain_id = models.ForeignKey("SUBDOMAIN", models.SET_NULL,blank=True,null=True,) 

	
		
class FRAMEWORK_VARIABLE_SET(models.Model):


    framework_id = models.ForeignKey("FRAMEWORK", models.SET_NULL,blank=True,null=True,) 

    variable_set_id = models.ForeignKey("VARIABLE_SET", models.SET_NULL,blank=True,null=True,) 

	
		
class CUBE_MAPPING(models.Model):


    cube_mapping_id = models.CharField("cube_mapping_id",max_length=255, primary_key=True)   

    description = models.CharField("description",max_length=255,default=None, blank=True, null=True)   

    source_cube = models.ForeignKey("CUBE", models.SET_NULL,blank=True,null=True,related_name='source_cube') 

    target_cube = models.ForeignKey("CUBE", models.SET_NULL,blank=True,null=True,related_name='target_cube',) 

    name = models.CharField("name",max_length=255,default=None, blank=True, null=True)   

    code = models.CharField("code",max_length=255,default=None, blank=True, null=True)   

    maintenance_agency_id = models.CharField("maintenance_agency_id",max_length=255,default=None, blank=True, null=True)   

    mappingsToCube = models.ForeignKey("MAPPING_TO_CUBE", models.SET_NULL,blank=True,null=True,) 

	
		
class MEMBER_MAPPING(models.Model):


    name = models.CharField("name",max_length=255,default=None, blank=True, null=True)   

    code = models.CharField("code",max_length=255,default=None, blank=True, null=True)   

    maintenance_agency_id = models.CharField("maintenance_agency_id",max_length=255,default=None, blank=True, null=True)   

    member_mapping_id = models.CharField("member_mapping_id",max_length=255, primary_key=True)   

    memberMappingItems = models.ForeignKey("MEMBER_MAPPING_ITEM", models.SET_NULL,blank=True,null=True,) 

	
		
class MEMBER_MAPPING_ITEM(models.Model):


    isSource = models.CharField("isSource",max_length=255,default=None, blank=True, null=True)   

    member = models.ForeignKey("MEMBER", models.SET_NULL,blank=True,null=True,) 

    variable = models.ForeignKey("VARIABLE", models.SET_NULL,blank=True,null=True,) 

    row = models.CharField("row",max_length=255,default=None, blank=True, null=True)   

    member_hierarchy = models.ForeignKey("MEMBER_HIERARCHY", models.SET_NULL,blank=True,null=True,) 

	
		
class VARIABLE_MAPPING_ITEM(models.Model):


    isSource = models.CharField("isSource",max_length=255,default=None, blank=True, null=True)   

    variable = models.ForeignKey("VARIABLE", models.SET_NULL,blank=True,null=True,) 

	
		
class VARIABLE_MAPPING(models.Model):


    varaibleMappingItems = models.ForeignKey("VARIABLE_MAPPING_ITEM", models.SET_NULL,blank=True,null=True,) 

    name = models.CharField("name",max_length=255,default=None, blank=True, null=True)   

    code = models.CharField("code",max_length=255,default=None, blank=True, null=True)   

    maintenance_agency_id = models.CharField("maintenance_agency_id",max_length=255,default=None, blank=True, null=True)   

    variable_mapping_id = models.CharField("variable_mapping_id",max_length=255, primary_key=True)   

	
		
class MAPPING_TO_CUBE(models.Model):


    mapping = models.ForeignKey("MAPPING_DEFINITION", models.SET_NULL,blank=True,null=True,) 

	
		
class VARIABLE_SET_MAPPING(models.Model):


    sourceMapping = models.ForeignKey("MAPPING_DEFINITION", models.SET_NULL,blank=True,null=True,related_name='sourceMapping') 

    targetMapping = models.ForeignKey("MAPPING_DEFINITION", models.SET_NULL,blank=True,null=True,related_name='targetMapping') 

	
		
class MAPPING_DEFINITION(models.Model):


    name = models.CharField("name",max_length=255,default=None, blank=True, null=True)   

    code = models.CharField("code",max_length=255,default=None, blank=True, null=True)   

    maintenance_agency_id = models.CharField("maintenance_agency_id",max_length=255,default=None, blank=True, null=True)   

    mapping_id = models.CharField("mapping_id",max_length=255, primary_key=True)   

    mapping_type = models.CharField("mapping_type",max_length=255,default=None, blank=True, null=True)   

    memberMapping = models.ForeignKey("MEMBER_MAPPING", models.SET_NULL,blank=True,null=True,) 

    variableMapping = models.ForeignKey("VARIABLE_MAPPING", models.SET_NULL,blank=True,null=True,) 

	
		
class AXIS(models.Model):


    axis_id = models.CharField("axis_id",max_length=255, primary_key=True)   

    code = models.CharField("code",max_length=255,default=None, blank=True, null=True)   

    description = models.CharField("description",max_length=255,default=None, blank=True, null=True)   

    is_open_axis = models.BooleanField("is_open_axis",default=None, blank=True, null=True)   

    name = models.CharField("name",max_length=255,default=None, blank=True, null=True)   

    order = models.BigIntegerField("order",default=None, blank=True, null=True)   

    orientation = models.CharField("orientation",max_length=255,default=None, blank=True, null=True)   

    table_id = models.ForeignKey("TABLE", models.SET_NULL,blank=True,null=True,) 

    displayName = models.CharField("displayName",max_length=255,default=None, blank=True, null=True)   

	
		
class AXIS_ORDINATE(models.Model):


    axis_id = models.ForeignKey("AXIS", models.SET_NULL,blank=True,null=True,) 

    axis_ordinate_id = models.CharField("axis_ordinate_id",max_length=255,default=None, blank=True, null=True)   

    code = models.CharField("code",max_length=255,default=None, blank=True, null=True)   

    description = models.CharField("description",max_length=255,default=None, blank=True, null=True)   

    is_abstract_header = models.BooleanField("is_abstract_header",default=None, blank=True, null=True)   

    level = models.BigIntegerField("level",default=None, blank=True, null=True)   

    name = models.CharField("name",max_length=255, primary_key=True)   

    order = models.BigIntegerField("order",default=None, blank=True, null=True)   

    parent_axis_ordinate_id = models.ForeignKey("AXIS_ORDINATE", models.SET_NULL,blank=True,null=True,) 

    path = models.CharField("path",max_length=255,default=None, blank=True, null=True)   

    displayName = models.CharField("displayName",max_length=255,default=None, blank=True, null=True)   

	
		
class CELL_POSITION(models.Model):


    axis_ordinate_id = models.ForeignKey("AXIS_ORDINATE", models.SET_NULL,blank=True,null=True,) 

    cell_id = models.ForeignKey("TABLE_CELL", models.SET_NULL,blank=True,null=True,) 

	
		
class ORDINATE_ITEM(models.Model):


    axis_ordinate_id = models.ForeignKey("AXIS_ORDINATE", models.SET_NULL,blank=True,null=True,) 

    is_starting_member_included = models.BooleanField("is_starting_member_included",default=None, blank=True, null=True)   

    member_hierarchy_id = models.ForeignKey("MEMBER_HIERARCHY", models.SET_NULL,blank=True,null=True,) 

    member_id = models.ForeignKey("MEMBER", models.SET_NULL,blank=True,null=True,) 

    starting_member_id = models.ForeignKey("MEMBER", models.SET_NULL,blank=True,null=True,related_name='starting_member_id') 

    variable_id = models.ForeignKey("VARIABLE", models.SET_NULL,blank=True,null=True,) 

	
		
class TABLE(models.Model):


    code = models.CharField("code",max_length=255,default=None, blank=True, null=True)   

    description = models.CharField("description",max_length=255,default=None, blank=True, null=True)   

    maintenance_agency_id = models.ForeignKey("MAINTENANCE_AGENCY", models.SET_NULL,blank=True,null=True,) 

    name = models.CharField("name",max_length=255, primary_key=True)   

    table_id = models.CharField("table_id",max_length=255,default=None, blank=True, null=True)   

    valid_from = models.DateTimeField("valid_from",default=None, blank=True, null=True)   

    valid_to = models.DateTimeField("valid_to",default=None, blank=True, null=True)   

    version = models.CharField("version",max_length=255,default=None, blank=True, null=True)   

    displayName = models.CharField("displayName",max_length=255,default=None, blank=True, null=True)   

	
		
class TABLE_CELL(models.Model):


    cell_id = models.CharField("cell_id",max_length=255,default=None, blank=True, null=True)   

    combination_id = models.ForeignKey("COMBINATION", models.SET_NULL,blank=True,null=True,) 

    is_shaded = models.BooleanField("is_shaded",default=None, blank=True, null=True)   

    table_id = models.ForeignKey("TABLE", models.SET_NULL,blank=True,null=True,) 

    name = models.CharField("name",max_length=255, primary_key=True)   

	
		
class DomainModule(SDDModule):


    domains = models.ForeignKey("DOMAIN", models.SET_NULL,blank=True,null=True,) 

	
		
class MemberHierarchyModule(SDDModule):


    memberHierarchies = models.ForeignKey("MEMBER_HIERARCHY", models.SET_NULL,blank=True,null=True,) 

    memberHierarchiesNodes = models.ForeignKey("MEMBER_HIERARCHY_NODE", models.SET_NULL,blank=True,null=True,) 

	
		
class MemberModule(SDDModule):


    members = models.ForeignKey("MEMBER", models.SET_NULL,blank=True,null=True,) 

	
		
class VariableModule(SDDModule):


    variables = models.ForeignKey("VARIABLE", models.SET_NULL,blank=True,null=True,) 

	
		
class VariableSetModule(SDDModule):


    variableSets = models.ForeignKey("VARIABLE_SET", models.SET_NULL,blank=True,null=True,) 

	
		
class SubDomainModule(SDDModule):


    subdomains = models.ForeignKey("SUBDOMAIN", models.SET_NULL,blank=True,null=True,) 

	
		
class SMCubesCoreModel(SDDModule):


    variableModules = models.ForeignKey("VariableModule", models.SET_NULL,blank=True,null=True,) 

    variableSetModules = models.ForeignKey("VariableSetModule", models.SET_NULL,blank=True,null=True,) 

    domainModules = models.ForeignKey("DomainModule", models.SET_NULL,blank=True,null=True,) 

    memberModules = models.ForeignKey("MemberModule", models.SET_NULL,blank=True,null=True,) 

    subDomainModules = models.ForeignKey("SubDomainModule", models.SET_NULL,blank=True,null=True,) 

    memberHierarchyModules = models.ForeignKey("MemberHierarchyModule", models.SET_NULL,blank=True,null=True,) 

    maintenanceAgencyModules = models.ForeignKey("MaintenanceAgencyModule", models.SET_NULL,blank=True,null=True,) 

	
		
class SMCubesModel(SDDModule):


    coreModel = models.ForeignKey("SMCubesCoreModel", models.SET_NULL,blank=True,null=True,) 

    extraModel = models.ForeignKey("SMCubesExtraModel", models.SET_NULL,blank=True,null=True,) 

	
		
class SMCubesExtraModel(SDDModule):


    combinationModules = models.ForeignKey("CombinationModule", models.SET_NULL,blank=True,null=True,) 

    cubeModules = models.ForeignKey("CubeModule", models.SET_NULL,blank=True,null=True,) 

    frameworkModules = models.ForeignKey("FrameworkModule", models.SET_NULL,blank=True,null=True,) 

    mappingDefinitionModules = models.ForeignKey("MappingDefinitionModule", models.SET_NULL,blank=True,null=True,) 

    cubeMappingModules = models.ForeignKey("CubeMappingModule", models.SET_NULL,blank=True,null=True,) 

    memberMappingModules = models.ForeignKey("MemberMappingModule", models.SET_NULL,blank=True,null=True,) 

    variableMappingModules = models.ForeignKey("VariableMappingModule", models.SET_NULL,blank=True,null=True,) 

    axisModules = models.ForeignKey("AxisModule", models.SET_NULL,blank=True,null=True,) 

    axisOrdinateModules = models.ForeignKey("AxisOrdinateModule", models.SET_NULL,blank=True,null=True,) 

    cellPositionModules = models.ForeignKey("CellPositionModule", models.SET_NULL,blank=True,null=True,) 

    ordinateItemsModules = models.ForeignKey("OrdinateItemModule", models.SET_NULL,blank=True,null=True,) 

    reportTableModules = models.ForeignKey("ReportTableModule", models.SET_NULL,blank=True,null=True,) 

    tableCellModules = models.ForeignKey("TableCellModule", models.SET_NULL,blank=True,null=True,) 

	
		
class CombinationModule(SDDModule):


    combinations = models.ForeignKey("COMBINATION", models.SET_NULL,blank=True,null=True,) 

    combination_items = models.ForeignKey("COMBINATION_ITEM", models.SET_NULL,blank=True,null=True,) 

    cubeToCombinationLinks = models.ForeignKey("CUBE_TO_COMBINATION", models.SET_NULL,blank=True,null=True,) 

	
		
class CubeModule(SDDModule):


    cubes = models.ForeignKey("CUBE", models.SET_NULL,blank=True,null=True,) 

    cubeStructures = models.ForeignKey("CUBE_STRUCTURE", models.SET_NULL,blank=True,null=True,) 

    cubeStructureItems = models.ForeignKey("CUBE_STRUCTURE_ITEM", models.SET_NULL,blank=True,null=True,) 

	
		
class FrameworkModule(SDDModule):


    frameworks = models.ForeignKey("FRAMEWORK", models.SET_NULL,blank=True,null=True,) 

	
		
class MappingDefinitionModule(SDDModule):


    mappings = models.ForeignKey("MAPPING_DEFINITION", models.SET_NULL,blank=True,null=True,) 

	
		
class CubeMappingModule(SDDModule):


    cubeMappings = models.ForeignKey("CUBE_MAPPING", models.SET_NULL,blank=True,null=True,) 

	
		
class MemberMappingModule(SDDModule):


    memberMappings = models.ForeignKey("MEMBER_MAPPING", models.SET_NULL,blank=True,null=True,) 

	
		
class VariableMappingModule(SDDModule):


    variableMappings = models.ForeignKey("VARIABLE_MAPPING", models.SET_NULL,blank=True,null=True,) 

	
		
class AxisModule(SDDModule):


    axes = models.ForeignKey("AXIS", models.SET_NULL,blank=True,null=True,) 

	
		
class AxisOrdinateModule(SDDModule):


    axis_ordinates = models.ForeignKey("AXIS_ORDINATE", models.SET_NULL,blank=True,null=True,) 

	
		
class CellPositionModule(SDDModule):


    cellPositions = models.ForeignKey("CELL_POSITION", models.SET_NULL,blank=True,null=True,) 

	
		
class OrdinateItemModule(SDDModule):


    ordinateItems = models.ForeignKey("ORDINATE_ITEM", models.SET_NULL,blank=True,null=True,) 

	
		
class ReportTableModule(SDDModule):


    reportTables = models.ForeignKey("TABLE", models.SET_NULL,blank=True,null=True,) 

	
		
class TableCellModule(SDDModule):


    tableCells = models.ForeignKey("TABLE_CELL", models.SET_NULL,blank=True,null=True,) 

	
		
class MaintenanceAgencyModule(SDDModule):


    maintenanceAgencies = models.ForeignKey("MAINTENANCE_AGENCY", models.SET_NULL,blank=True,null=True,) 

	
		
class CUBE_LINK(models.Model):


    maintenance_agency_id = models.ForeignKey("MAINTENANCE_AGENCY", models.SET_NULL,blank=True,null=True,) 

    cube_link_id = models.CharField("cube_link_id",max_length=255, primary_key=True)   

    code = models.CharField("code",max_length=255,default=None, blank=True, null=True)   

    name = models.CharField("name",max_length=255,default=None, blank=True, null=True)   

    description = models.CharField("description",max_length=255,default=None, blank=True, null=True)   

    valid_from = models.DateTimeField("valid_from",default=None, blank=True, null=True)   

    valid_to = models.DateTimeField("valid_to",default=None, blank=True, null=True)   

    version = models.CharField("version",max_length=255,default=None, blank=True, null=True)   

    order_relevance = models.BigIntegerField("order_relevance",default=None, blank=True, null=True)   

    primary_cube_id = models.ForeignKey("CUBE", models.SET_NULL,blank=True,null=True,related_name='primary_cube_in_cube_link') 

    foreign_cube_id = models.ForeignKey("CUBE", models.SET_NULL,blank=True,null=True,related_name='foreign_cube_in_cube_link') 

    cube_link_type = models.CharField("cube_link_type",max_length=255,default=None, blank=True, null=True)   

	
		
class CUBE_STRUCTURE_ITEM_LINK(models.Model):


    cube_structure_item_link_id = models.CharField("cube_structure_item_link_id",max_length=255, primary_key=True)   

    cube_link_id = models.ForeignKey("CUBE_LINK", models.SET_NULL,blank=True,null=True,) 

    foreign_cube_variable_code = models.ForeignKey("CUBE_STRUCTURE_ITEM", models.SET_NULL,blank=True,null=True,related_name='foreign_cube_variable_code') 

    primary_cube_variable_code = models.ForeignKey("CUBE_STRUCTURE_ITEM", models.SET_NULL,blank=True,null=True,related_name='primary_cube_variable_code') 

    product_identifier = models.ForeignKey("MEMBER", models.SET_NULL,blank=True,null=True,) 

	
		
class MEMBER_LINK(models.Model):


    cube_structure_item_link_id = models.ForeignKey("CUBE_STRUCTURE_ITEM_LINK", models.SET_NULL,blank=True,null=True,) 

    foreign_member_id = models.ForeignKey("MEMBER", models.SET_NULL,blank=True,null=True,related_name='foreign_member_id') 

    primary_member_id = models.ForeignKey("MEMBER", models.SET_NULL,blank=True,null=True,related_name='primary_member_id') 

    valid_from = models.DateTimeField("valid_from",default=None, blank=True, null=True)   

    valid_to = models.DateTimeField("valid_to",default=None, blank=True, null=True)   

    is_linked = models.BooleanField("is_linked",default=None, blank=True, null=True)   

	

	


	

		
class Import(models.Model):


    importedNamespace = models.CharField("importedNamespace",max_length=255,default=None, blank=True, null=True)   

	
		
class Module(models.Model):


    dependencies = models.ForeignKey("Module", models.SET_NULL,blank=True,null=True,) 

    theDescription = models.CharField("theDescription",max_length=255,default=None, blank=True, null=True)   

    license = models.CharField("license",max_length=255,default=None, blank=True, null=True)   

    name = models.CharField("name",max_length=255, primary_key=True)   

    version = models.CharField("version",max_length=255,default=None, blank=True, null=True)   

    imports = models.ForeignKey("Import", models.SET_NULL,blank=True,null=True,) 

    annotationDirectives = models.ForeignKey("ELAnnotationDirective", models.SET_NULL,blank=True,null=True,related_name='the_annotation_directives') 

	
		
class ModuleList(models.Model):


    modules = models.ForeignKey("Module", models.SET_NULL,blank=True,null=True,) 

	
		
class RulesForReport(models.Model):


    outputLayerCube = models.ForeignKey("ELClass", models.SET_NULL,blank=True,null=True,) 

    rulesForTable = models.ForeignKey("RulesForILTable", models.SET_NULL,blank=True,null=True,) 

	
		
class RulesForILTable(models.Model):


    rulesForTablePart = models.ForeignKey("RuleForILTablePart", models.SET_NULL,blank=True,null=True,) 

    inputLayerTable = models.ForeignKey("ELClass", models.SET_NULL,blank=True,null=True,) 

	
		
class SelectColumn(models.Model):


    asAttribute = models.ForeignKey("ELOperation", models.SET_NULL,blank=True,null=True,) 

	
		
class SelectColumnMemberAs(SelectColumn):


    memberAsConstant = models.ForeignKey("ELEnumLiteral", models.SET_NULL,blank=True,null=True,) 

	
		
class SelectColumnAttributeAs(SelectColumn):


    attribute = models.ForeignKey("ELAttribute", models.SET_NULL,blank=True,null=True,) 

	
		
class SelectDerivedColumnAs(SelectColumn):


    attribute = models.ForeignKey("ELOperation", models.SET_NULL,blank=True,null=True,) 

	
		
class SelectValueAs(SelectColumn):


    value = models.CharField("value",max_length=255,default=None, blank=True, null=True)   

	
		
class TableFilter(models.Model):


    predicate = models.ForeignKey("Predicate", models.SET_NULL,blank=True,null=True,) 

	
		
class GenerationRulesModule(Module):


    rulesForReport = models.ForeignKey("RulesForReport", models.SET_NULL,blank=True,null=True,) 

	
		
class RuleForILTablePart(models.Model):


    name = models.CharField("name",max_length=255,default=None, blank=True, null=True)   

    columns = models.ForeignKey("SelectColumn", models.SET_NULL,blank=True,null=True,) 

    whereClause = models.ForeignKey("TableFilter", models.SET_NULL,blank=True,null=True,) 

	
		
class Predicate(models.Model):

	pass
	
		
class AndPredicate(Predicate):


    operands = models.ForeignKey("Predicate", models.SET_NULL,blank=True,null=True,related_name='and_predicate') 

	
		
class OrPredicate(Predicate):


    operands = models.ForeignKey("Predicate", models.SET_NULL,blank=True,null=True,related_name='or_predicate') 

	
		
class NotPredicate(Predicate):


    operand = models.ForeignKey("Predicate", models.SET_NULL,blank=True,null=True,related_name='not_predicate') 

	
		
class AttributePredicate(Predicate):


    attribute1 = models.ForeignKey("ELStructuralFeature", models.SET_NULL,blank=True,null=True,) 

    Comparitor = {"less_than":"less_than",
"equals":"equals",
"greater_than":"greater_than",
"not_equals":"not_equals",
} 
    comparitor = models.CharField("comparitor",max_length=255, choices=Comparitor,default=None, blank=True, null=True)   

    member = models.ForeignKey("ELEnumLiteral", models.SET_NULL,blank=True,null=True,) 

    value = models.CharField("value",max_length=255,default=None, blank=True, null=True)   

class ELModelElement(models.Model):


    eAnnotations = models.ForeignKey("ELAnnotation", models.SET_NULL,blank=True,null=True,) 

	

class ELNamedElement(ELModelElement):


    name = models.CharField("name",max_length=255,default=None, blank=True, null=True)   


class ELTypedElement(ELNamedElement):


    eType = models.ForeignKey("ELClassifier", models.SET_NULL,blank=True,null=True,) 

    upperBound = models.BigIntegerField("upperBound",default=None, blank=True, null=True)   

    lowerBound = models.BigIntegerField("lowerBound",default=None, blank=True, null=True)   

		

class ELStructuralFeature(ELTypedElement):


	pass		
		
class ELAttribute(ELStructuralFeature):


    iD = models.BooleanField("iD",default=None, blank=True, null=True)   

    eAttributeType = models.ForeignKey("ELDataType", models.SET_NULL,blank=True,null=True,) 

class ELClassifier(ELNamedElement):


    ePackage = models.ForeignKey("ELPackage", models.SET_NULL,blank=True,null=True,) 

		
		
class ELClass(ELClassifier):


    eAbstract = models.BooleanField("eAbstract",default=None, blank=True, null=True)   

    eSuperTypes = models.ForeignKey("ELClass", models.SET_NULL,blank=True,null=True,) 

    eStructuralFeatures = models.ForeignKey("ELStructuralFeature", models.SET_NULL,blank=True,null=True,) 

    eOperations = models.ForeignKey("ELOperation", models.SET_NULL,blank=True,null=True,) 

	
		

		
class ELDataType(ELClassifier):


    industryName = models.CharField("industryName",max_length=255,default=None, blank=True, null=True)   

	
		
class ELEnum(ELDataType):


    eLiterals = models.ForeignKey("ELEnumLiteral", models.SET_NULL,blank=True,null=True,) 

	
		
class ELEnumLiteral(ELNamedElement):


    value = models.BigIntegerField("value",default=None, blank=True, null=True)   

    literal = models.CharField("literal",max_length=255,default=None, blank=True, null=True)   

	
		

	
		
class ELOperation(ELTypedElement):


    body = models.CharField("body",max_length=255,default=None, blank=True, null=True)   

	
		
class ELParameter(ELTypedElement):


	pass
		
class ELPackage(Module):


    eClassifiers = models.ForeignKey("ELClassifier", models.SET_NULL,blank=True,null=True,) 

    nsURI = models.CharField("nsURI",max_length=255,default=None, blank=True, null=True)   

    nsPrefix = models.CharField("nsPrefix",max_length=255,default=None, blank=True, null=True)   

	
		
class ELReference(ELStructuralFeature):


    containment = models.BooleanField("containment",default=None, blank=True, null=True)   

    eReferenceType = models.ForeignKey("ELClassifier", models.SET_NULL,blank=True,null=True,) 

    eOpposite = models.ForeignKey("ELReference", models.SET_NULL,blank=True,null=True,) 

	
		

		

		
class ELAnnotation(ELModelElement):


    details = models.ForeignKey("ELStringToStringMapEntry", models.SET_NULL,blank=True,null=True,) 

    source = models.ForeignKey("ELAnnotationDirective", models.SET_NULL,blank=True,null=True,) 

	
		
class ELAnnotationDirective(ELNamedElement):


    module = models.ForeignKey("Module", models.SET_NULL,blank=True,null=True,) 

    sourceURI = models.CharField("sourceURI",max_length=255,default=None, blank=True, null=True)   

	
		
class ELStringToStringMapEntry(models.Model):


    key = models.CharField("key",max_length=255,default=None, blank=True, null=True)   

    value = models.CharField("value",max_length=255,default=None, blank=True, null=True)   

	
		
class Report(models.Model):


    outputLayer = models.ForeignKey("ELClass", models.SET_NULL,blank=True,null=True,) 

    rows = models.ForeignKey("ReportRow", models.SET_NULL,blank=True,null=True,) 

    columns = models.ForeignKey("ReportColumn", models.SET_NULL,blank=True,null=True,) 

	
		
class CellBasedReport(Report):


    reportCells = models.ForeignKey("ReportCell", models.SET_NULL,blank=True,null=True,) 

	
		
class ReportRow(models.Model):


    name = models.CharField("name",max_length=255, primary_key=True)   

	
		
class ReportColumn(models.Model):


    name = models.CharField("name",max_length=255, primary_key=True)   

	
		
class ReportCell(models.Model):


    row = models.ForeignKey("ReportRow", models.SET_NULL,blank=True,null=True,) 

    column = models.ForeignKey("ReportColumn", models.SET_NULL,blank=True,null=True,) 

    filters = models.ForeignKey("Filter", models.SET_NULL,blank=True,null=True,) 

    metric = models.ForeignKey("ELOperation", models.SET_NULL,blank=True,null=True,) 

    datapointID = models.CharField("datapointID",max_length=255,default=None, blank=True, null=True)   

	
		
class Filter(models.Model):


    outputLayer = models.ForeignKey("ELClass", models.SET_NULL,blank=True,null=True,) 

    operation = models.ForeignKey("ELOperation", models.SET_NULL,blank=True,null=True,) 

    member = models.ForeignKey("ELEnumLiteral", models.SET_NULL,blank=True,null=True,) 

	
		
class ReportModule(Module):


    reports = models.ForeignKey("Report", models.SET_NULL,blank=True,null=True,) 

	
		
class RowColumnBasedReport(Report):


    columnFilters = models.ForeignKey("ColumnFilters", models.SET_NULL,blank=True,null=True,) 

    rowFilters = models.ForeignKey("RowFilters", models.SET_NULL,blank=True,null=True,) 

    wholeReportFilters = models.ForeignKey("WholeReportFilters", models.SET_NULL,blank=True,null=True,) 

	
		
class RowFilters(models.Model):


    row = models.ForeignKey("ReportRow", models.SET_NULL,blank=True,null=True,) 

    filters = models.ForeignKey("Filter", models.SET_NULL,blank=True,null=True,) 

    metric = models.ForeignKey("ELOperation", models.SET_NULL,blank=True,null=True,) 

	
		
class ColumnFilters(models.Model):


    column = models.ForeignKey("ReportColumn", models.SET_NULL,blank=True,null=True,) 

    filters = models.ForeignKey("Filter", models.SET_NULL,blank=True,null=True,) 

    metric = models.ForeignKey("ELOperation", models.SET_NULL,blank=True,null=True,) 

	
		
class WholeReportFilters(models.Model):


    filters = models.ForeignKey("Filter", models.SET_NULL,blank=True,null=True,) 

	

	

