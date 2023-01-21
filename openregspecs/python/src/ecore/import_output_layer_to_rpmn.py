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
from _ast import Try
'''
Created on 22 Jan 2022

@author: Neil
'''
from pyecore.ecore import *
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
        if context.useSubDomains:
            ROLImport.createSubDomainToDomainMap(self,context)
            ROLImport.createSubDomainToMemberMaps(self,context)
            ROLImport.addROLEnumsAndLiteralsToPackageUsingSubDomains(self,context)
        else:
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
                        
                        xclass = EClass(name=alteredClassName+"_OutputItem")
                        xclassTable = EClass(name=alteredClassName+"_OutputTable")
                        xclassTable.containedEntityType = xclass
                        containmentReference  = EReference()
                        containmentReference.name=xclass.name+"s"
                        containmentReference.eType=xclass
                        containmentReference.upperBound = -1
                        containmentReference.lowerBound=0
                        containmentReference.containment= True
                        xclassTable.eStructuralFeatures.append(containmentReference)
                        xclassTableOperation = EOperation()
                        xclassTableOperation.name=xclass.name+"s"
                        xclassTableOperation.eType=xclass
                        xclassTableOperation.upperBound = -1
                        xclassTableOperation.lowerBound=0
                        xclassTable.eOperations.append(xclassTableOperation)
                        context.rpmnPackage.eClassifiers.extend([xclass])
                        context.rpmnPackage.eClassifiers.extend([xclassTable])
                        
            
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
                    if (memberName is None) or (memberName == ""):
                        memberName = memberID
                    domainId =  row[2]
                    
                    #if there is no domain ID this suggests a falty row in the csv due to return statements in fields
                    if not(domainId is None) and not(domainId == ""):
                        context.memberIDToDomainMap[memberID] = domainId
                        context.memberIDToMemberNameMap[memberID] = memberName
                        context.memberIDToMemberCodeMap[memberID] = memberCode
                    
    def createSubDomainToDomainMap(self,context):
        fileLocation = context.fileDirectory + "\\subdomain.csv"
        headerSkipped = False
        #for each subdomain createw a lsit

        with open(fileLocation,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if (not headerSkipped):
                        headerSkipped = True
                else:                   
                    domain_id = row[2]
                    subdomain_id = row[8]
                    context.subDomainIDToDomainID[subdomain_id]=domain_id
                    
    def createSubDomainToMemberMaps(self,context):
        fileLocation = context.fileDirectory + "\\subdomain_enumeration.csv"
        headerSkipped = False
        #for each subdomain createw a lsit

        with open(fileLocation,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if (not headerSkipped):
                        headerSkipped = True
                else:                   
                    member_id = row[0]
                    subdomain_id = row[2]
                    valid_to=row[4]
                    if (valid_to == "12/31/9999"):
                        memberList = None
                        try: 
                            memberList =context.subDomainToMemberListMap[subdomain_id]
                        except:
                            memberList =[]
                            context.subDomainToMemberListMap[subdomain_id] = memberList
                        
                        if not(member_id in memberList):
                            memberList.append(member_id)
                        
                            
                    
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
                theEnum = EEnum()
                theEnum.name = amendedDomainName 
                #maintain a map of enum IDS to XEnum objects
                context.enumMap[amendedDomainName] = theEnum
                context.rpmnPackage.eClassifiers.extend([theEnum])
                theDomainMembers= Utils.getMembersOfTheDomain(theDomain, context.memberIDToDomainMap)
                counter1 = 0
                for member in theDomainMembers:
                    enumLiteral = EEnumLiteral()
                    enumUsedName = Utils.makeValidID(context.memberIDToMemberCodeMap[member])
                    adaptedValue = Utils.makeValidID(context.memberIDToMemberNameMap[member])
                    newAdaptedValue = Utils.uniqueValue( theEnum, adaptedValue)
                    newAdaptedName = Utils.uniqueName( theEnum, enumUsedName)

                    enumLiteral.name = newAdaptedName
                    enumLiteral.literal = newAdaptedValue
                    counter1 = counter1 + 1
                    enumLiteral.value = counter1
                    theEnum.eLiterals.extend([enumLiteral])
        
    def addROLEnumsAndLiteralsToPackageUsingSubDomains(self,context):
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
                    subDomainID = row[10]
                    classID = row[1]
    
                    try: 

                        domainID = context.subDomainIDToDomainID[subDomainID]
                        domain_ID_Name = context.domainToDomainNameMap[domainID]
                        amendedDomainName = Utils.makeValidID(subDomainID + "_ISSUBDOMAINOF_" + domain_ID_Name)
                        theEnum =  Utils.findROLEnum(amendedDomainName,context.enumMap)
                        if theEnum is None:
                            if not( (amendedDomainName == "String") or (amendedDomainName == "Date")  ):
                                theEnum = EEnum()
                                theEnum.name = amendedDomainName 
                                #maintain a map of enum IDS to XEnum objects
                                context.enumMap[amendedDomainName] = theEnum
                                context.rpmnPackage.eClassifiers.extend([theEnum])
                                theDomainMembers= context.subDomainToMemberListMap[subDomainID]
                                counter1 = 0
                                for member in theDomainMembers:
                                    enumLiteral = EEnumLiteral()
                                    enumUsedName = Utils.makeValidID(context.memberIDToMemberCodeMap[member])
                                    adaptedValue = Utils.makeValidID(context.memberIDToMemberNameMap[member])
                                    newAdaptedValue = Utils.uniqueValue( theEnum, adaptedValue)
                                    newAdaptedName = Utils.uniqueName( theEnum, enumUsedName)
                
                                    enumLiteral.name = newAdaptedName
                                    enumLiteral.literal = newAdaptedValue
                                    counter1 = counter1 + 1
                                    enumLiteral.value = counter1
                                    theEnum.eLiterals.extend([enumLiteral])    
                            
                    except:
                            print( "missing ROL class2: " )
                            print(classID)    
                            
      
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
                    subDomainID = row[10]
                    classID = row[1]
                    try: 
                        theClass = context.classesMap[classID]
                        
                        classIsDerived = True
                            
                        theAttributeName =  amendedAttributeName
                        amendedDomainName = None
                        if context.useSubDomains:
                            domainID = context.subDomainIDToDomainID[subDomainID]
                            domain_ID_Name = context.domainToDomainNameMap[domainID]
                            amendedDomainName = Utils.makeValidID(subDomainID + "_ISSUBDOMAINOF_" + domain_ID_Name)
                        else:
                            domainID = context.variableToDomainMap[variable]
                            amendedDomainName = Utils.makeValidID(domainID+"_domain")
                        #domain_ID_Name = context.domainToDomainNameMap[domainID]
                      
                        theEnum =  Utils.findROLEnum(amendedDomainName,context.enumMap)
                        if  theEnum is not None:                     
                            
                            if classIsDerived:
                                operation = EOperation()
                                operation.lowerBound=0
                                operation.upperBound=1
                                if(theEnum.name == "String"):
                                    operation.name = theAttributeName
                                    operation.eType = context.xString
                                elif(theEnum.name.startswith("String_")):
                                    operation.name = theAttributeName
                                    operation.eType = context.xString
                                elif(theEnum.name == "Number"):
                                    operation.name = theAttributeName
                                    operation.eType = context.xDouble
                                
                                elif(theEnum.name.startswith("Real_")):
                                    operation.name = theAttributeName
                                    operation.eType = context.xDouble
                                elif(theEnum.name.startswith("Monetary")): 
                                    operation.name = theAttributeName
                                    operation.eType = context.xInt
                                elif(theEnum.name.startswith("Non_negative_monetary_amounts_with_2_decimals")): 
                                    operation.name = theAttributeName
                                    operation.eType = context.xInt
                                elif(theEnum.name.startswith("Non_negative_integers")): 
                                    operation.name = theAttributeName
                                    operation.eType = context.xInt
                                elif(theEnum.name.startswith("All_possible_dates")):   
                                    operation.name = theAttributeName
                                    operation.eType = context.xDate  
                                else:
                                    operation.name = theAttributeName
                                    operation.eType = theEnum  
        
                            try:
            
                                theClass = context.classesMap[classID]
                                
                                if classIsDerived:
                                    theClass.eOperations.extend([operation])
            
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
                            