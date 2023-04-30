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
from ecore4reg import *

class ImportSDD(object):
    '''
    Class responsible for the import of  SDD csv files 
    into an instance of the analaysis model
    '''
    
    def import_sdd(self,sdd_context):
        '''
        Import SDD csv files into an instance of the analaysis model
        '''
        ImportSDD.createAllDomains(self, context) 
        ImportSDD.createAllMembers(self, context)
        ImportSDD.createAllVariables(self, context)
        ImportSDD.createAllSubdomains(self, context)
        ImportSDD.createAllSubdomainEnumerarions(self, context)
        #ImportSDD.createVariableSetToVariableMap(self, context)
        #ImportSDD.createVariableToDomainMap(self, context)
        #ImportSDD.createDomainToDomainNameMap(self, context)
        #ImportSDD.createMemberMaps(self, context)
        
    def createAllDomains(self, context):
        '''
        imprt all the domains
        '''
        fileLocation = context.fileDirectory + os.sep + "domain.csv"
        headerSkipped = False

        with open(fileLocation,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not headerSkipped:
                    headerSkipped = True
                else:
                    domainID = row[0]
                    # domainName = Utils.makeValidID(row[3])
                    domainName = row[8]
                    context.domainToDomainNameMap[domainID] = domainName
        
        
        
        