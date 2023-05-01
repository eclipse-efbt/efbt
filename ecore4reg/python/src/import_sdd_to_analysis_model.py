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
import os
import csv
from sdd_model import *

class ImportSDD(object):
    '''
    Class responsible for the import of  SDD csv files
    into an instance of the analaysis model
    '''

    def import_sdd(self, sdd_context):
        '''
        Import SDD csv files into an instance of the analaysis model
        '''
        ImportSDD.create_all_domains(self, sdd_context)
        ImportSDD.create_all_members(self, sdd_context)
        ImportSDD.create_all_variables(self, sdd_context)
        ImportSDD.create_all_subdomains(self, sdd_context)
        ImportSDD.create_all_subdomainEnumerations(self, sdd_context)
        # ImportSDD.createVariableSetToVariableMap(self, context)
        # ImportSDD.createVariableToDomainMap(self, context)
        # ImportSDD.createDomainToDomainNameMap(self, context)
        # ImportSDD.createMemberMaps(self, context)

    def create_all_domains(self, context):
        '''
        import all the domains
        '''
        fileLocation = context.file_directory + os.sep + "domain.csv"
        header_skipped = False

        with open(fileLocation,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    code = row[0]
                    data_type = row[1]
                    description = row[2]
                    domain_id = row[3]
                    is_enumerated = row[5]
                    is_reference = row[6]
                    # domainName = Utils.makeValidID(row[3])
                    domain_name = row[8]
                    context.domainToDomainNameMap[domain_id] = domain_name

                    # creat the Domain abject and set its fields
                    domain = DOMAIN(
                        name=ImportSDD.replaceDots(self, domain_id))
                    domain.code = code

                    # if (dataTypeString != null) {
					# if (dataTypeString.contains("tring")) {
					# valueType = FACET_VALUE_TYPE.STRING;
					# domain.setData_type(valueType);
					# }
					# f (dataTypeString.contains("nteger")) {
					# valueType = FACET_VALUE_TYPE.BIG_INTEGER;
					# domain.setData_type(valueType);
					# }
					# if (dataTypeString.contains("ate")) {
					# valueType = FACET_VALUE_TYPE.DATE_TIME;
					# domain.setData_type(valueType);
					# }
					# if (dataTypeString.contains("umber") || dataTypeString.contains("onetary")) {
					# valueType = FACET_VALUE_TYPE.DECIMAL;
					# domain.setData_type(valueType);
					# }
					# if (dataTypeString.contains("oolean")) {
					# valueType = FACET_VALUE_TYPE.BOOLEAN;
					# domain.setData_type(valueType);
					# }

                    domain.description = description
                    domain.domain_id = ImportSDD.replaceDots(self, domain_id)
                    domain.displayName = domain_name
                    if is_enumerated:
                        domain.is_enumerated = True
                    else:
                        domain.is_enumerated = False

                    if is_enumerated:
                        domain.is_reference = True
                    else:
                        domain.is_reference = False

                    context.domains.domains.append(domain)

    def create_all_members(self, context):
        '''
        import all the members
        '''
        fileLocation = context.file_directory + os.sep + "member.csv"
        header_skipped = False

        with open(fileLocation,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    code = row[0]
                    description = row[1]
                    domain_id = row[2]
                    member_id = row[4]
                    member_name = row[5]
                    member = MEMBER(
                        name=ImportSDD.replaceDots(self, member_id))
                    member.member_id = ImportSDD.replaceDots(self, member_id)
                    member.code = code
                    member.description = description
                    member.displayName = member_name
                    domain = ImportSDD.get_domain_with_id(
                        self, context, domain_id)
                    member.domain_id = domain
                    context.members.members.append(member)

    def create_all_variables(self, context):

        fileLocation = context.file_directory + os.sep + "variable.csv"
        header_skipped = False

        with open(fileLocation,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    code = row[0]
                    description = row[1]
                    domain_id = row[2]
                    name = row[4]
                    variable_id = row[6]
                    variable = VARIABLE(
                        name=ImportSDD.replaceDots(self, variable_id))
                    variable.code = code
                    variable.variable_id = ImportSDD.replaceDots(
                        self, variable_id)
                    variable.displayName = name
                    domain = ImportSDD.get_domain_with_id(self, context, domain_id)
                    variable.domain_id =domain
                    variable.description = description

                    context.variables.variables.append(variable)

    def create_all_subdomains(self, context):

        fileLocation = context.file_directory + os.sep + "subdomain.csv"
        header_skipped = False

        with open(fileLocation,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    code = row[0]
                    description = row[1]
                    domain_id = row[2]
                    name = row[7]
                    subdomain_id = row[8]

                    subDomain = SUBDOMAIN()
                    subDomain.code = code
                    subDomain.subdomain_id = ImportSDD.replaceDots(
                        self, subdomain_id)
                    subDomain.displayName = name
                    subDomain.description = description

                    domain = ImportSDD.get_domain_with_id(self, context, domain_id)
                    subDomain.domain_id = domain
                    context.subdomains.subdomains.append(subDomain);

    def create_all_subdomainEnumerations(self, context):

        fileLocation = context.file_directory + os.sep + "subdomain_enumeration.csv"
        header_skipped = False

        with open(fileLocation,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    member_id = row[0]
                    subdomain_id = row[2]

        subdomain = ImportSDD.get_subdomain_with_id(self, context, subdomain_id)
        domain = subdomain.domain_id
        member = ImportSDD.get_member_with_id_and_domain(
            self, context, member_id, domain)
        subDomainEnum = SUBDOMAIN_ENUMERATION()
        subDomainEnum.member_id = member
        subdomain.items.append(subDomainEnum)

    def get_domain_with_id(self, context, domain_id_string):
        for domain in context.domains.domains:
            if domain.domain_id == domain_id_string:
                return domain

    def get_subdomain_with_id(self, context, subdomain_id_string):

        for subdomain in context.subdomains.subdomains:
            if subdomain.subdomain_id == subdomain_id_string:
                return subdomain

    def get_member_with_id_and_domain(self, context, member_id, domain):
        for member in context.members.members:
            if (member_id == member.name) and (member.domain_id == domain):
                return member

    def replaceDots(self, text):
        return text.replace('.', '_')
