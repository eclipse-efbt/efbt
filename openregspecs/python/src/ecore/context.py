from pyecore.ecore import *

class Context(object):
    
    xString = EString
    #xString.name = "EString"
    
    xDouble = EDouble
    #xDouble.name = "EDouble"
    
    xInt = EInt
    #xInt.name = "EInt"
    
    xDate = EDate
    #xDate.name = "EDate"
    # This is the root package where we add each type, class and enum
    typesPackage = EPackage(name='types', nsURI='http://www.eclipse.org/bird/types', nsPrefix='types')
    inputLayerEnumsPackage = EPackage(name='input_layer_enums', nsURI='http://www.eclipse.org/bird/input_layer_enums', nsPrefix='input_layer_enums')
    outputLayerEnumsPackage = EPackage(name='output_layer_enums', nsURI='http://www.eclipse.org/bird/output_layer_enums', nsPrefix='output_layer_enums')
    inputLayerEntitiesPackage = EPackage(name='input_layer_entities', nsURI='http://www.eclipse.org/bird/input_layer_entities', nsPrefix='input_layer_entities')
    outputLayerEntitiesPackage = EPackage(name='output_layer_entities', nsURI='http://www.eclipse.org/bird/output_layer_entities', nsPrefix='output_layer_entities')
    
    # classesMap keeps a reference between ldm ID's for classes and
    # the class instance
    classesMap = {}
    # classesMap keeps a reference between ldm ID's for classes and
    # the list of these calsses that we call tables
    tableMap = {}
    # A map between the LDM names for primitive types types, and
    # our standard primitive types such as XString  
    datatypeMap = {}
    # For the reference output layers we record a map between variables 
    # and domains
    variableToDomainMap = {}
    # For the reference output layers we record a map between domains 
    # and domain names
    domainToDomainNameMap = {}
    # For the reference output layers we record a map between members ids  
    # andtheir containing domains
    memberIDToDomainMap = {}
    # For the reference output layers we record a map between members ids  
    # and their names
    memberIDToMemberNameMap = {}
    # For the reference output layers we record a map between members ids  
    # and their codes
    memberIDToMemberCodeMap = {}
    
    FKtoMandatoryMap = {}
    FKToColumnMap = {}
    # we record a list of missing domains
    missingDomains = []
    # enumMap keeps a reference between ldm ID's for domains and
    # the enum instance
    enumMap = {}
    # the directory where we get our input files
    fileDirectory = ""
    # the directory where we save our outputs.
    outputDirectory = ""
    

    useSubDomains = False
    
    subDomainToMemberListMap = {}
    subDomainIDToDomainID = {}
    def __init__(self):

        a=0

        