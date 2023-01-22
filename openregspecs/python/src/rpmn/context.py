from open_reg_specs import *

class Context(object):
    # we create the main 4 'primitive' data types
    xString = XDataType()
    xString.name = "String"
    
    xDouble = XDataType()
    xDouble.name = "Double"
    
    xInt = XDataType()
    xInt.name = "Integer"
    
    xDate = XDataType()
    xDate.name = "Date"

    moduleList = ModuleList()
    
    typesPackage = XPackage(name='types')
    inputLayerEnumsPackage = XPackage(name='input_layer_enums')
    outputLayerEnumsPackage = XPackage(name='output_layer_enums')
    inputLayerEntitiesPackage = XPackage(name='input_layer_entities')
    outputLayerEntitiesPackage = XPackage(name='output_layer_entities')
    
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
    
    viewModule = ViewModule(name='generations')
    
    workflowModule = WorkflowModule(name='finrepWorkflow')
    
    vtlModule = VTLModule(name = "vtl")
    moduleList = ModuleList()

    persistVTLComments = True
    
    useSubDomains = False
    
    subDomainToMemberListMap = {}
    subDomainIDToDomainID = {}
    def __init__(self):
        

        self.typesPackage.classifiers.append(self.xString)
        self.typesPackage.classifiers.append(self.xDouble)
        self.typesPackage.classifiers.append(self.xInt)
        self.moduleList.modules.append(self.typesPackage)
        self.moduleList.modules.append(self.inputLayerEnumsPackage)
        self.moduleList.modules.append(self.outputLayerEnumsPackage)
        self.moduleList.modules.append(self.inputLayerEntitiesPackage)
        self.moduleList.modules.append(self.outputLayerEntitiesPackage)
        self.moduleList.modules.append(self.viewModule)
        self.moduleList.modules.append(self.workflowModule)
        self.moduleList.modules.append(self.vtlModule)
        
        