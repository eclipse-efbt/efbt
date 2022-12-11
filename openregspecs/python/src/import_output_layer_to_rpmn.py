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

                    className = row[0]
                    objectID  = row[1]
                    cube_type = row[3]
                    valid_to  = row[11]
                    framework   = row[5]
                    print("objectID")
                    print(objectID)
                    
                    if ( (framework ==  "FINREP_REF") and (cube_type == "RC") and (valid_to == "12/31/9999") ) :

                        alteredClassName = Utils.makeValidID(className);  
                        
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
                        print("objectID")
                        print(objectID)
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
                    variableName = row[6]
                    #domainName = Utils.makeValidID(row[3])
                    domain = row[2]
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
                    domainID = row[0]
                    #domainName = Utils.makeValidID(row[3])
                    domainName = row[8]
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
                    memberID = row[4]
                    print("memberid")
                    print(memberID)
                    #domainName = Utils.makeValidID(row[3])
                    memberCode= row[0]
                    memberName = row[5]
                    domainId =  row[2]
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
                   
                    classID = row[1]
    
                    try: 

                        domainID = context.variableToDomainMap[variable]
                        # domain_ID_Name = context.domainToDomainNameMap[domainID]
                        amendedDomainName = Utils.makeValidID(domainID)
                        theEnum =  Utils.findROLEnum(amendedDomainName+"_domain",context.enumMap)
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
            amendedDomainName = Utils.makeValidID(theDomain)+ "_domain"
            if not( (amendedDomainName == "String") or (amendedDomainName == "Date")  ):
                theEnum = XEnum()
                theEnum.name = amendedDomainName 
                #maintain a map of enum IDS to XEnum objects
                context.enumMap[amendedDomainName] = theEnum
                context.rpmnPackage.classifiers.extend([theEnum])
                theDomainMembers= Utils.getMembersOfTheDomain(theDomain, context.memberIDToDomainMap)
                counter1 = 0
                for member in theDomainMembers:
                    enumLiteral = XEnumLiteral()
                    enumUsedName = Utils.makeValidID(context.memberIDToMemberCodeMap[member])
                    adaptedValue = Utils.makeValidID(context.memberIDToMemberNameMap[member])
                    newAdaptedValue = Utils.uniqueValue( theEnum, adaptedValue)
                    newAdaptedName = Utils.uniqueName( theEnum, enumUsedName)

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
                    attributeName = row[11]
                    amendedAttributeName = Utils.makeValidID(attributeName)
                    variable = row[2]
                   
                    classID = row[1]
                    try: 
                        theClass = context.classesMap[classID]
                        
                        classIsDerived = True
                            
                        theAttributeName =  amendedAttributeName
       
                        domainID = context.variableToDomainMap[variable]
                        #domain_ID_Name = context.domainToDomainNameMap[domainID]
                        amendedDomainName = Utils.makeValidID(domainID)
                        theEnum =  Utils.findROLEnum(amendedDomainName+"_domain",context.enumMap)
                        if  theEnum is not None:                     
                            
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
                            