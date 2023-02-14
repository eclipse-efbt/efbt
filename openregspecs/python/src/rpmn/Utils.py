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
from pyecore.resources import ResourceSet, URI
from pyecore.ecore import *
from pyecore.resources.xmi import XMIResource
from pyecore.resources.xmi import XMIOptions

import csv
import unidecode 


class Utils(object):
    
    @classmethod 
    def uniqueValue (cls, theEnum, adaptedValue):
        '''
            if the adapted value already exists in the enum then append it with _x2
            if the that string appended with _x2 already exists, then append with_x3 instead
            if that exists then _x4 etc.
        '''
        newAdaptedValue = adaptedValue
        if Utils.containsLiteral(theEnum.literals, adaptedValue ):
            newAdaptedValue = adaptedValue +"_x2"
        counter = 1
        finished = False
        # within the bird data model there is re-use of the same id or name
        # for multiple members, which is not ideal. For a very small number 
        # of domains this is in the hundreds or over one thousand,
        # which is why we need a high limit here.
        # it would be better if BIRD addressed this repetition.
        # it is particularly noticeable in NUTS and NACE codes.
        # this high limit increases the processing time from under 1 minute
        # to a few minutes for the full BIRD data model.
        limit = 32
        while ((counter < limit) and not(finished)):
            counter = counter + 1
            if Utils.containsLiteral(theEnum.literals, adaptedValue +"_x" + str(counter)):
                newAdaptedValue = adaptedValue +"_x"+ str(counter+1)
            else:
                finished = True
        
        return newAdaptedValue
    
    @classmethod
    def uniqueName (cls, theEnum, enumUsedName):
        '''
        if the adapted name already exists in the enum then append it with _x2
        if the that string appended with _x2 already exists, then append with_x3 instead
        if that exists then _x4 etc.
        ''' 
        newAdaptedName = enumUsedName
        counter = 1
        finished = False
        limit = 32
        if Utils.containsName(theEnum.literals, enumUsedName ):
                newAdaptedName = enumUsedName +"_x2"
                
        while ((counter < limit) and not(finished)):
            counter = counter + 1
            if Utils.containsName(theEnum.literals, enumUsedName +"_x" + str(counter)):
                newAdaptedName = enumUsedName +"_x"+ str(counter+1)
            else:
                finished = True 
  
        return newAdaptedName
    
    @classmethod
    def  getMembersOfTheDomain  (cls,  theDomain, memberIDToDomainMap):
        returnList = []
        for key, value in memberIDToDomainMap.items():
            if value == theDomain:
                returnList.append(key)
        return returnList
    
   
    @classmethod    
    def saveModelAsXMIFile(cls,context):
        '''
         save model as an xmi file representing an object tree.
        '''
        rset = ResourceSet()

        resource = rset.create_resource(URI(context.outputDirectory + 'IL.rpmn'))  # This will create an XMI resource
        resource.append(context.rpmnPackage)
        resource.save()
    
   
    @classmethod    
    def superclassContainsFeature(cls,theSuperClass, attribute) :
        '''
        Checks if a superclass contains the attribute
        '''
        attributes = theSuperClass.members
        contains = False;
        for attribute2 in attributes:
            if (attribute2.name == attribute.name):
                contains = True;

        return contains
    
    @classmethod
    def hasMemberCalled(cls,theClass,memberName) :
        '''
        Checks if the class has a member with the name memberName
        '''
        
        members = theClass.members
        contains = False;
        for member in members:
            if (member.name == memberName):
                contains = True;

        return contains
    
    @classmethod
    def numberofRelationShipsToThisClass(cls,sourceClass, targetClass):
        '''
        Checks how many relationships there are between 2 classes
        It is possible that one class might have 2 different relationships 
        to the same class.
        '''
        features = sourceClass.members
        counter = 0;
        # do this for relationship attributes only.
        for feature in features:
            if ( isinstance(feature,XReference)):
                featureType = feature.type            
                if (featureType == targetClass):
                    counter = counter+1
            
        return counter
    
    
    @classmethod
    def makeValidID(cls,inputString):
        ''' 
        Tranlate text to be a valid id, without special characters, and following
        the rules for valid id's in RPMN
        '''
       
        # we do not allow id's to start with  number, if it does then we prepend with an underscore
        if(len(inputString) > 0):
            if((inputString[0] >= '0') and (inputString[0] <= '9')) :
                inputString = "_" + inputString
        # we replace special characters not allowed in id's with an underscore
        amendedInputString  =  inputString.replace('  ', ' ').replace(' ', '_').replace(')', '_').replace('(', '_') \
                .replace(',', '_').replace('\'', '_').replace('\n', '_').replace('\r', '_').replace('\'t', '_').replace('new', 'New') \
                .replace('\\', '_').replace('/', '_').replace('-', '_').replace(':', '_') \
                .replace('+', '_').replace('.', '_').replace('?', '_').replace('\'', '_').replace('>', '_gt') \
                .replace('<', '_lt').replace('\"', '_').replace(';', '_').replace('$', '_').replace('=', '_eq').replace('#', '_') \
                .replace('&', '_').replace('%', '_').replace('[', '_').replace(']', '_').replace('?', '_').replace('–', '_').replace('__', '_').replace('__', '_') \
                .replace( chr(0x2019), '_').replace( chr(65533), '_') \
                .replace(chr(0x2018), '_').replace(chr(0x0060), '_').replace(chr(0x00B4), '_').replace(chr(0x2026), '_') \
                .replace(chr(0x200B), '_').replace(chr(0x202F), '_').replace(chr(0x205F), '_').replace(chr(0x3000), '_') \
                .replace(chr(0x2000), '_').replace(chr(0x2001), '_').replace(chr(0x2002), '_').replace(chr(0x2003), '_') \
                .replace(chr(0x2004), '_').replace(chr(0x2005), '_').replace(chr(0x2006), '_').replace(chr(0x2007), '_') \
                .replace(chr(0x2008), '_').replace(chr(0x2009), '_').replace(chr(0x200A), '_').replace(chr(0x00A0), '_') \
                .replace(chr(0x0027), '_').replace(chr(0x2019), '_').replace(chr(0x2018), '_')
          
        returnString = Utils.replaceAcutesGravesAndCircumflexes(amendedInputString).replace('\'', '_')      

        if (returnString == "op"):
            returnString = "_op"
        return returnString;
    
    @classmethod
    def replaceAcutesGravesAndCircumflexes(cls, theInputString):
        '''
        We replace letters with acutes , graves, and circumflexes, with the basic letter.
        So for example "a acute" is replaced with "a"
        
        returnString = theInputString.replace(chr(0x00E9), 'e').replace(chr(0x00C9), 'E').replace(chr(0x00E8), 'e')  \
                .replace(chr(0x00EB), 'e').replace(chr(0x00CB), 'E').replace(chr(0x0116), 'E').replace(chr(0x0117), 'e').replace(chr(0x0118), 'E').replace(chr(0x0119), 'e') \
                .replace(chr(0x00CA), 'E').replace(chr(0x00EA), 'e').replace(chr(0x00E7), 'c').replace(chr(0x00C7), 'C').replace(chr(0x010C), 'C').replace(chr(0x010D), 'c').replace(chr(0x00FC), 'u') \
                .replace(chr(0x00DA), 'U').replace(chr(0x00FA), 'u').replace(chr(0x00DC), 'U').replace(chr(0x00FC), 'u').replace(chr(0x00F6), 'o') \
                .replace(chr(0x00ED), 'i').replace(chr(0x00CC), 'I').replace(chr(0x00EC), 'i').replace(chr(0x00CE), 'I') \
                .replace(chr(0x00EE), 'i').replace(chr(0x00E4), 'a').replace(chr(0x00E1), 'a').replace(chr(0x00C1), 'A').replace(chr(0x00D6), 'O') \
                .replace(chr(0x00D3), 'O').replace(chr(0x00D4), 'O').replace(chr(0x00F3), 'o').replace(chr(0x00D1), 'N').replace(chr(0x00F1), 'n') \
                .replace(chr(0x00DF), 'ss').replace(chr(0x00E0), 'a').replace(chr(0x00C0), 'A').replace(chr(0x00C2), 'A').replace(chr(0x00E2), 'a') \
                .replace(chr(0x00C3), 'A').replace(chr(0x00E3), 'a') \
                .replace(chr(0x00FD), 'y').replace(chr(0x017D), 'Z').replace(chr(0x017E), 'z').replace(chr(0x0179), 'Z').replace(chr(0x017A), 'z').replace(chr(0x017B), 'Z').replace(chr(0x017C), 'z') \
                .replace(chr(0x0160), 'S').replace(chr(0x0161), 's').replace(chr(0x015A), 'S').replace(chr(0x015B), 's').replace(chr(0x00D8), 'O').replace(chr(0x00F8), 'o').replace(chr(0x00F4), 'o').replace(chr(0x00D5), 'O').replace(chr(0x00F5), 'o')  \
                .replace(chr(0x00C6), 'AE').replace(chr(0x01E2), 'AE').replace(chr(0x01FC), 'AE')   \
                .replace(chr(0x00E6), 'ae').replace(chr(0x01E3), 'ae').replace(chr(0x01FD), 'ae')    \
                .replace(chr(0x00C5), 'A').replace(chr(0x00E5), 'a').replace(chr(0x0143), 'N').replace(chr(0x0144), 'n').replace(chr(0x0141), 'L').replace(chr(0x0142), 'l').replace(chr(0x0173), 'u')
        '''
        return unidecode.unidecode(theInputString)     
       
    @classmethod
    def inEnumBlackList(cls,adaptedEnumName):
        # TODO not sure if we still need this, it was introduces to deal with 
        # problematic domains in the past.
        if( (adaptedEnumName == "All_last_days_of_months___YYYY_MM") or
            (adaptedEnumName =="All_last_days_of_quarters___YYYY_MM") or
            (adaptedEnumName == "All_possible_dates_YYYY_MM_DD")  ):

            print(" field in blacklist: " + adaptedEnumName)
            return True
        else:
            return False 
        
    @classmethod    
    def containsLiteral(cls,members,adaptedValue):
        '''
        checks if an enum contains a particular literal
        '''
        contains = False;
        for eEnumLiteral in members:
            if (eEnumLiteral.name.lower() == adaptedValue.lower()):
                contains = True

        return contains
    
    @classmethod
    def containsName(cls,members,adaptedName):
        '''
        checks if an enum contains a particular name
        '''
        contains = False;
        for eEnumLiteral in members:
            if (eEnumLiteral.literal.lower() == adaptedName.lower()):
                contains = True

        return contains
    
    @classmethod
    def getLiteralsForEnumeration(cls, domain, membersModule):
        '''
        returns the list of literals for an enumerations
        ''' 
        returnMembersList = []   
        for member in membersModule.members:
            if (member.domain_id == domain):
                returnMembersList.append(member) 
        return returnMembersList
    
    @classmethod
    def getEcoreDataTypeForDataType(cls,context):

        return context.xString
    
    @classmethod
    def findROLEnum(cls,enumName, enumMap):
        returnVal= None
        for key,value in enumMap.items():
            if value.name.lower() == enumName.lower():
               returnVal =  value
        
        return returnVal
    
    @classmethod
    def specialCases(cls,theEnum,newAdaptedValue):
        returnVal = newAdaptedValue
        if (newAdaptedValue == "A_S"):
           returnVal = "A_S_dup"
        if (newAdaptedValue == "s_p_"):
           returnVal = "s_p_dup"
        return returnVal
  
