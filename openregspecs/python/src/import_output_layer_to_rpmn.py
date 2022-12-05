# coding=UTF-8#
# Copyright (c) 2020 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Neil Mackenzie - initial API and implementation
#
from pickle import TRUE
'''
Created on 22 Jan 2022

@author: Neil
'''
from open_reg_specs import *
import csv
from Utils import Utils
from context import Context

class ROLImport(object):
    
    
  
    def doImport(self,context): 
        '''
        import the items from the Output layer csv files
        '''
        ROLImport.addROLClassesToPackage(self,context) 
        ROLImport.createVariableToDomainMap(self,context) 
        ROLImport.createDomainToDomainNameMap(self,context)
        ROLImport.createMemberMaps(self,context)
        ROLImport.addROLEnumsAndLiteralsToPackage(self,context)
        ROLImport.addROLAttributesToClasses(self,context)
        
    
    def addROLClassesToPackage(self,context):        
        fileLocation = context.fileDirectory + "\\cube.csv"
        headerSkipped = False
        # Load all the entities from the csv file, make an XClass per entity,
        # and add the XClass to the package
        with open(fileLocation,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                # skip the first line which is the header.
                if (not headerSkipped):
                    headerSkipped = True
                else:

                    className = row[5]
                    objectID  = className
                    cube_type = row[6]
                    valid_to  = row[9]
                    version   = row[10]
                    
                    if ( (version ==  "FINREP 3.0") and (cube_type == "RC") and (valid_to == "31.12.9999") ) :

                        alteredClassName = Utils.makeValidID(self,className);  
                        
                        xclass = XClass(name=alteredClassName+"_OutputItem")
                        xclassTable = XClass(name=alteredClassName+"_OutputTable")
                        xclassTable.containedEntityType = xclass
                        containmentReference  = XReference()
                        containmentReference.name=xclass.name+"s"
                        containmentReference.type=xclass
                        containmentReference.upperBound = -1
                        containmentReference.lowerBound=0
                        containmentReference.containment= True
                        xclassTable.members.append(containmentReference)
                        xclassTableOperation = XOperation()
                        xclassTableOperation.name=xclass.name+"s"
                        xclassTableOperation.type=xclass
                        xclassTableOperation.upperBound = -1
                        xclassTableOperation.lowerBound=0
                        xclassTable.members.append(xclassTableOperation)
                        context.rpmnPackage.classifiers.extend([xclass])
                        context.rpmnPackage.classifiers.extend([xclassTable])
                        
            
                        # maintain a map a objectIDs to XClasses
                        context.classesMap[objectID]=xclass
                        context.tableMap[xclass]=xclassTable
                        
    def createVariableToDomainMap(self,context):       
        # Make a variable to Domain Map
        fileLocation = context.fileDirectory + "\\variable.csv"
        headerSkipped = False
        # or each attribute add an Xattribute to the correct XClass represtnting the Entity
        # the attribute should have the correct type, which may be a specific
        # enumeration
        
        with open(fileLocation,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if (not headerSkipped):
                        headerSkipped = True
                else:
                    variableName = row[1]
                    #domainName = Utils.makeValidID(self,row[3])
                    domain = row[4]
                    context.variableToDomainMap[variableName] = domain
             
    def createDomainToDomainNameMap(self,context):         
        '''
         Make a domain  to Domain Name Map
        ''' 
        fileLocation = context.fileDirectory + "\\domain.csv"
        headerSkipped = False

        with open(fileLocation,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if (not headerSkipped):
                        headerSkipped = True
                else:
                    domainID = row[1]
                    #domainName = Utils.makeValidID(self,row[3])
                    domainName = row[2]
                    context.domainToDomainNameMap[domainID] = domainName
                   
    def createMemberMaps(self,context):   
         # Make a domain  to Domain Name Map 
        fileLocation = context.fileDirectory + "\\member.csv"
        headerSkipped = False

        
        with open(fileLocation,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if (not headerSkipped):
                        headerSkipped = True
                else:
                    memberID = row[1]
                    #domainName = Utils.makeValidID(self,row[3])
                    memberCode= row[2]
                    memberName = row[3]
                    domainId =  row[4]
                    context.memberIDToDomainMap[memberID] = domainId
                    context.memberIDToMemberNameMap[memberID] = memberName
                    context.memberIDToMemberCodeMap[memberID] = memberCode
                    
                       
                       
    def addROLEnumsAndLiteralsToPackage(self,context):                   
        fileLocation = context.fileDirectory + "\\cube_structure_item.csv"
        headerSkipped = False
        # or each attribute add an Xattribute to the correct XClass represtnting the Entity
        # the attribute should have the correct type, which may be a specific
        # enumeration

        with open(fileLocation,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if (not headerSkipped):
                        headerSkipped = True
                else:
                    variable = row[2]
                    classID = row[0]
                    try: 

                        domainID = context.variableToDomainMap[variable]
                        # domain_ID_Name = context.domainToDomainNameMap[domainID]
                        amendedDomainName = Utils.makeValidID(self,domainID)
                        theEnum =  Utils.findROLEnum(self,amendedDomainName+"_domain",context.enumMap)
                        if  theEnum is not None:                     
                            print( "not missing domainID: " )
                        else:
                            print( "missing domainID: " )
                            print(domainID)
                            print(classID)
                            if not(domainID in context.missingDomains):
                                context.missingDomains.append(domainID)
                    except:
                            print( "missing ROL class2: " )
                            print(classID)    
                            
        for theDomain in context.missingDomains:
            
            #domain_ID_Name = context.domainToDomainNameMap[theDomain]
            amendedDomainName = Utils.makeValidID(self,theDomain)+ "_domain"
            if not( (amendedDomainName == "String") or (amendedDomainName == "Date")  ):
                theEnum = XEnum()
                theEnum.name = amendedDomainName 
                #maintain a map of enum IDS to XEnum objects
                context.enumMap[amendedDomainName] = theEnum
                context.rpmnPackage.classifiers.extend([theEnum])
                theDomainMembers= Utils.getMembersOfTheDomain(self,theDomain, context.memberIDToDomainMap)
                counter1 = 0
                for member in theDomainMembers:
                    enumLiteral = XEnumLiteral()
                    enumUsedName = Utils.makeValidID(self,context.memberIDToMemberCodeMap[member])
                    adaptedValue = Utils.makeValidID(self,context.memberIDToMemberNameMap[member])
                    newAdaptedValue = Utils.uniqueValue(self, theEnum, adaptedValue)
                    newAdaptedName = Utils.uniqueName(self, theEnum, enumUsedName)
                    
                    
                    
                    enumLiteral.name = newAdaptedName
                    enumLiteral.literal = newAdaptedValue
                    counter1 = counter1 + 1
                    enumLiteral.value = counter1
                    theEnum.literals.extend([enumLiteral])
            
    def addROLAttributesToClasses(self,context):
        '''
        For each attribute add an Xattribute to the correct XClass represtnting the Entity
        the attribute should have the correct type, which may be a specific
        enumeration
        '''
        fileLocation = context.fileDirectory + "\\cube_structure_item.csv"
        headerSkipped = False
        

        with open(fileLocation,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if (not headerSkipped):
                        headerSkipped = True
                else:
                    attributeName = row[1]
                    amendedAttributeName = Utils.makeValidID(self,attributeName)
                    variable = row[2]
                   
                    classID = row[0]
                    try: 
                        theClass = context.classesMap[classID]
                        
                        classIsDerived = True
                            
                        theAttributeName =  amendedAttributeName
       
                        domainID = context.variableToDomainMap[variable]
                        #domain_ID_Name = context.domainToDomainNameMap[domainID]
                        amendedDomainName = Utils.makeValidID(self,domainID)
                        theEnum =  Utils.findROLEnum(self,amendedDomainName+"_domain",context.enumMap)
                        if  theEnum is not None:                     
                            attribute = XAttribute()

                            attribute.lowerBound=0
                            attribute.upperBound=1
                            if(theEnum.name == "String"):
                                attribute.name = theAttributeName
                                attribute.type = context.xString
                            elif(theEnum.name.startswith("String_")):
                                attribute.name = theAttributeName
                                attribute.type = context.xString
                            elif(theEnum.name == "Number"):
                                attribute.name = theAttributeName
                                attribute.type = context.xDouble
                            elif(theEnum.name.startswith("Real_")):
                                attribute.name = theAttributeName
                                attribute.type = context.xDouble
                            elif(theEnum.name.startswith("Monetary")):
                                attribute.name = theAttributeName
                                attribute.type = context.xInt
                            elif(theEnum.name.startswith("Non_negative_monetary_amounts_with_2_decimals")): 
                                attribute.name = theAttributeName
                                attribute.type = context.xInt
                            elif(theEnum.name.startswith("Non_negative_integers")): 
                                attribute.name = theAttributeName
                                attribute.type = context.xInt
                            elif(theEnum.name.startswith("All_possible_dates")):
                                attribute.name = theAttributeName
                                attribute.type = context.xDate
                                
                            # This is a common domain used for String identifiers in BIRD in SQLDeveloper
                            
                            else:
                                attribute.name = theAttributeName
                                attribute.type = theEnum  
                            
                            if classIsDerived:
                                operation = XOperation()
                                operation.lowerBound=0
                                operation.upperBound=1
                                if(theEnum.name == "String"):
                                    operation.name = theAttributeName
                                    operation.type = context.xString
                                elif(theEnum.name.startswith("String_")):
                                    operation.name = theAttributeName
                                    operation.type = context.xString
                                elif(theEnum.name == "Number"):
                                    operation.name = theAttributeName
                                    operation.type = context.xDouble
                                
                                elif(theEnum.name.startswith("Real_")):
                                    operation.name = theAttributeName
                                    operation.type = context.xDouble
                                elif(theEnum.name.startswith("Monetary")): 
                                    operation.name = theAttributeName
                                    operation.type = context.xInt
                                elif(theEnum.name.startswith("Non_negative_monetary_amounts_with_2_decimals")): 
                                    operation.name = theAttributeName
                                    operation.type = context.xInt
                                elif(theEnum.name.startswith("Non_negative_integers")): 
                                    operation.name = theAttributeName
                                    operation.type = context.xInt
                                elif(theEnum.name.startswith("All_possible_dates")):   
                                    operation.name = theAttributeName
                                    operation.type = context.xDate  
                                else:
                                    operation.name = theAttributeName
                                    operation.type = theEnum  
        
                            try:
            
                                theClass = context.classesMap[classID]
                                theClass.members.extend([attribute])
                                if classIsDerived:
                                    theClass.members.extend([operation])
            
                            except:
                                print( "missing class2: " )
                        else:
                            print( "XXXXX missing domainID: " )
                            print(domainID)
                            print(classID)
                            if not(domainID in context.missingDomains):
                                context.missingDomains.append(domainID)
                    except:
                            print( "XX missing ROL class1: " )
                            print(classID)                                
                            