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
from pyecore.resources import ResourceSet, URI
from utils.utils import Utils


class StandardMatchingQueries(object):
    '''
    Documentation for StandardMatchingQueries
    '''
    il_domains_model = None
    input_tables_model = None
    sdd_domains_model = None
    output_tables_model = None
    types_model = None
    variable_name_to_code_map = {}
    unique_tuple_list = []
    derivableList = []

    def query1(self,context):
        '''
        Documentation for query1
        '''

        rset = ResourceSet()
        extension = 'ecore'
        il_domains_resource = rset.get_resource(
            URI(context.output_directory + os.sep + extension + 
            os.sep + "il_domains.ecore"))
        il_domains_root = il_domains_resource.contents[0]
        rset.metamodel_registry[il_domains_root.nsURI] = il_domains_root
        self.il_domains_model = il_domains_root

        sdd_domains_resource = rset.get_resource(
            URI(context.output_directory + os.sep + extension +
            os.sep + "sdd_domains.ecore"))
        sdd_domains_root = sdd_domains_resource.contents[0]
        rset.metamodel_registry[sdd_domains_root.nsURI] = sdd_domains_root
        self.sdd_domains_model = sdd_domains_root

        input_tables_resource = rset.get_resource(
            URI(context.output_directory + os.sep + extension +
            os.sep+ "input_tables.ecore"))
        input_tables_root = input_tables_resource.contents[0]
        rset.metamodel_registry[input_tables_root.nsURI] = input_tables_root
        self.input_tables_model = input_tables_root

        output_tables_resource = rset.get_resource(
            URI(context.output_directory + os.sep + extension +
            os.sep + "output_tables.ecore"))
        output_tables_root = output_tables_resource.contents[0]
        rset.metamodel_registry[output_tables_root.nsURI] = output_tables_root
        self.output_tables_model = output_tables_root

        StandardMatchingQueries.create_input_layer_to_output_layer_matches(
            self, context)

    def create_input_layer_to_output_layer_matches(self, context):
        '''
        Documentation for createInputLayerToOutputLayerMatches
        '''
        output_layers = StandardMatchingQueries.get_output_layers(self)
        csv_strings = []
        headerString = "outputLayer,Variable name in outputLayer,Variable code in outputLayer,SubDomain code in outputLayer,Domain name in outputLayer ,MatchedAttribute in inputLayer,MatchedAttributeILDomain in inputLayer ,MatchedAttributeEntity in inputLayer,AttributeMatchType,SubDomainMember name in outputLayer ,MatchedDomainMember in inputLayer ,MemberMatchType"
        csv_strings.append(headerString)
        for output_layer in output_layers:
            output_layer_name = output_layer.name
            if StandardMatchingQueries.is_report_in_scope(self, output_layer_name, context):
                for attribute in output_layer.eOperations:
                    attribute_name = attribute.name
                    attribute_type = attribute.eType
                    atribute_type_name = attribute_type.name

                    if not (StandardMatchingQueries.in_excluded_list(self, attribute_name)):
                        domain_name = None
                        try:
                            index_of_issubdomainof = atribute_type_name.index(
                                '_ISSUBDOMAINOF_')
                            length = len(atribute_type_name)
                            domain_name = atribute_type_name[index_of_issubdomainof + 15:length]
                        except:
                            domain_name = atribute_type_name

                        if (attribute_name in self.derivableList):
                            csv_text_string = output_layer_name + "," + attribute_name + "," + \
                                self.variable_name_to_code_map[attribute_name] + "," + \
                                atribute_type_name + "," + domain_name + ",,DERIVABLE,,,,,"
                            csv_strings.append(csv_text_string)
                            tuple1 = (attribute_name,
                                     self.variable_name_to_code_map[attribute_name],
                                     "NONE",
                                     "NONE",
                                     "DERIVABLE")
                            if not (tuple1 in self.unique_tuple_list):
                                self.unique_tuple_list.append(tuple1)
                        else:
                            related_input_layer_attributes = StandardMatchingQueries.get_related_input_layer_attributes(
                                self, attribute_name,attribute_type)

                            if len(related_input_layer_attributes) > 0:
                                for related_input_layer_attribute in related_input_layer_attributes:
                                    related_input_layer_domain = related_input_layer_attribute.eType
                                    attribute_match_type = related_input_layer_attribute.matchType
                                    if (hasattr(related_input_layer_attribute, "subStringGuess")):
                                        subStringGuess = "SUBSTRING"

                                    csv_text_string = None
                                    if hasattr(attribute_type, "eLiterals"):
                                        for literal in attribute_type.eLiterals:
                                            literal_name = literal.name
                                            literal_match_type = "NONE"

                                            related_literal = StandardMatchingQueries.getRelatedInputLayerLiteral(
                                                self, literal, related_input_layer_domain)
                                            related_literal_name = "None"
                                            if not (related_literal is None):
                                                related_literal_name = related_literal.name
                                                literal_match_type = related_literal.matchType
                                            csv_text_string = output_layer_name + "," + attribute_name + "," + self.variable_name_to_code_map[attribute_name] + "," + atribute_type_name + "," + domain_name + "," + related_input_layer_attribute.name + \
                                                "," + related_input_layer_attribute.eType.name + "," + related_input_layer_attribute.eContainer().name + \
                                                "," + attribute_match_type + "," + literal_name + \
                                                "," + related_literal_name + "," + literal_match_type
                                            csv_strings.append(csv_text_string)
                                            tuple1 = (attribute_name,
                                                     self.variable_name_to_code_map[attribute_name],
                                                     related_input_layer_attribute.name,
                                                     related_input_layer_attribute.eContainer().name,
                                                     attribute_match_type)
                                            if not (tuple1 in self.unique_tuple_list):
                                                self.unique_tuple_list.append(
                                                    tuple1)

                                    else:
                                        csv_text_string = output_layer_name + "," + attribute_name + "," + self.variable_name_to_code_map[attribute_name] + "," + atribute_type_name + "," + domain_name + "," + \
                                            related_input_layer_attribute.name + "," + related_input_layer_attribute.eType.name + \
                                            "," + related_input_layer_attribute.eContainer().name + "," + \
                                            attribute_match_type + ",,,"
                                        csv_strings.append(csv_text_string)
                                        tuple1 = (attribute_name,
                                                 self.variable_name_to_code_map[attribute_name],
                                                 related_input_layer_attribute.name,
                                                 related_input_layer_attribute.eContainer().name,
                                                 attribute_match_type)
                                        if not (tuple1 in self.unique_tuple_list):
                                            self.unique_tuple_list.append(tuple1)
                            else:
                                csv_text_string = output_layer_name + "," + attribute_name + "," + \
                                    self.variable_name_to_code_map[attribute_name] + "," + \
                                    atribute_type_name + "," + domain_name + ",,NONE,,,,,"
                                csv_strings.append(csv_text_string)
                                tuple1 = (attribute_name,
                                         self.variable_name_to_code_map[attribute_name],
                                         "NONE",
                                         "NONE",
                                         "NONE")
                                if not (tuple1 in self.unique_tuple_list):
                                    self.unique_tuple_list.append(tuple1)
        extension = 'matches'
        f = open(context.output_directory + os.sep + extension + 
            os.sep + 'matches.csv', "a",  encoding='utf-8')
        for the_string in csv_strings:
            f.write(the_string)
            f.write('\n')
        f.close()

        f2 = open(context.output_directory + os.sep + extension +
            os.sep + 'uniqueMatches.csv', "a",  encoding='utf-8')

        f2.write("Variable name in outputLayer,Variable code in outputLayer,MatchedAttribute in inputLayer,MatchedAttributeEntity in inputLayer,AttributeMatchType")
        f2.write('\n')
        for t in self.unique_tuple_list:
            f2.write(t[0])
            f2.write(',')
            f2.write(t[1])
            f2.write(',')
            f2.write(t[2])
            f2.write(',')
            f2.write(t[3])
            f2.write(',')
            f2.write(t[4])
            f2.write('\n')
        f2.close()

    def in_excluded_list(self, the_string):
        '''
        This method is used to exclude certain attributes from the matches.csv file
        '''
        excluded_list = False
        if the_string.strip() == 'Held_for_trading_indicator':
            excluded_list = True
        if the_string.strip() == 'Held_to_maturity_indicator':
            excluded_list = True
        if the_string.strip() == 'Accounting_portfolio_valuation_rules':
            excluded_list = True
        if the_string.strip() == 'Accounting_framework':
            excluded_list = True
        if the_string.strip() == 'Subject_to_impairment_indicator':
            excluded_list = True
        if the_string.strip() == 'Reference_date':
            excluded_list = True
        if the_string.strip() == 'Value_Decimal':
            excluded_list = True
        if the_string.strip() == 'The_RIAD_code_of_the_reporting_agent':
            excluded_list = True
        if the_string.strip() == 'Observation_value':
            excluded_list = True
        if the_string.strip() == 'SUBA_Reporting_Identifier':
            excluded_list = True

        return excluded_list

    def getRelatedInputLayerLiteral(self, literal, relatedInputLayerDomain):
        '''
        This method is used to find the related literal in the input layer
        '''
        return_literal = None
        if (hasattr(relatedInputLayerDomain, "eLiterals")):
            for the_literal in relatedInputLayerDomain.eLiterals:
                if the_literal.name.lower() == literal.name.lower():
                    return_literal = the_literal
                    the_literal.matchType = "EXACT"

            if return_literal is None:
                for the_literal in relatedInputLayerDomain.eLiterals:
                    if literal.name.lower() == the_literal.name.lower()[0:len(literal.name)]:
                        return_literal = the_literal
                        return_literal.matchType = "STARTSWITH"

            if return_literal is None:
                for the_literal in relatedInputLayerDomain.eLiterals:
                    if literal.name.lower() in the_literal.name.lower():
                        return_literal = the_literal
                        return_literal.matchType = "SUBSTRING"

            if return_literal is None:
                for the_literal in relatedInputLayerDomain.eLiterals:
                    if the_literal.name.lower() in literal.name.lower():
                        return_literal = the_literal
                        return_literal.matchType = "REVERSE_SUBSTRING"

        return return_literal

    def get_related_input_layer_attributes(self, the_attribute_name,attribute_type):
        '''
        This method is used to find the related attribute in the input layer
        '''
        classifiers = self.input_tables_model.eClassifiers
        return_attributes = []
        

        for classifier in classifiers:
            attributes = classifier.eAttributes
            for attribute in attributes:
                # we do not match for id fields that  we created to reflect 
                # a composite key
                if not (attribute.iD and attribute.name.endswith('_PK')):
                    attribute_name = attribute.name.lower()

                    if the_attribute_name.lower() == attribute_name:
                        attribute.matchType = "EXACT"
                        return_attributes.append(attribute)
                    elif the_attribute_name.lower() in attribute_name:
                        attribute.matchType = "SUBSTRING"
                        return_attributes.append(attribute)
                    elif attribute_name in the_attribute_name.lower():
                        attribute.matchType = "REVERSE_SUBSTRING"
                        return_attributes.append(attribute)

        if len(return_attributes) == 0:
            last_resort_matching_attribute =  StandardMatchingQueries.lastResortMatch(self,attribute_type)
            if not last_resort_matching_attribute is None:
                last_resort_matching_attribute.matchType = "LAST_RESORT"
                return_attributes.append(last_resort_matching_attribute)

        return return_attributes


    def get_related_input_layer_literal2(self,literal,relatedInputLayerDomain):
        '''
        This method is used to find the related attribute in the input layer
        '''
        return_literal = None
        if(hasattr(relatedInputLayerDomain, "eLiterals")):
            for the_literal in relatedInputLayerDomain.eLiterals:
                if the_literal.name.lower() == literal.name.lower():
                    return_literal = the_literal
                    the_literal.matchType2 ="EXACT"

            if return_literal is None:
                for the_literal in relatedInputLayerDomain.eLiterals:
                    if literal.name.lower() == the_literal.name.lower()[0:len(literal.name)]:
                        return_literal = the_literal
                        return_literal.matchType2 ="STARTSWITH"

            if return_literal is None:
                for the_literal in relatedInputLayerDomain.eLiterals:
                    if the_literal.name.lower() in  literal.name.lower():
                        return_literal = the_literal
                        return_literal.matchType2 ="SUPERSTRING"



        return return_literal



    def lastResortMatch(self,inputLayerAttributeType):

        classifiers = self.input_tables_model.eClassifiers
        best_attribute = None
        best_attributes_member_matching_count = 1;
        for classifier in classifiers:
            attributes = classifier.eAttributes
            for attribute in attributes:
                attribute_type = attribute.eType 
                member_match_count = 0

                if hasattr(attribute_type, "eLiterals"):
                    for literal in attribute_type.eLiterals:

                        related_literal = StandardMatchingQueries.get_related_input_layer_literal2(self,literal,inputLayerAttributeType)

                        if not (related_literal is None):
                            member_match_count = member_match_count +1

                    if member_match_count > best_attributes_member_matching_count:
                        best_attribute = attribute
                        best_attributes_member_matching_count = member_match_count

        return best_attribute

    def get_output_layers(self):
        '''
        This method is used to find the output layers
        '''
        classifiers = self.output_tables_model.eClassifiers
        output_layers = []
        for classifier in classifiers:
            if classifier.name.endswith("_OutputItem"):
                output_layers.append(classifier)

        return output_layers

    def is_report_in_scope(self, classname, context):
        '''
        This method is used to find if the report is in scope
        '''
        file_location = context.in_scope_file_directory + os.sep + "in_scope_reports.csv"
        cut_class_name = classname

        try:

            theindex = classname.index('_REF_OutputItem')
            cut_class_name = classname[0:theindex]
        except:
            print("hit exception")
            cut_class_name = classname

        header_skipped = False
        # Load all the entities from the csv file, make an XClass per entity,
        # and add the XClass to the package
        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                # skip the first line which is the header.
                if (not header_skipped):
                    header_skipped = True
                else:
                    reportTemplate = row[0]
                    if reportTemplate == cut_class_name:
                        return True

        return False

    def create_variable_name_to_code_map(self,context):
        '''
        This method is used to create a map of variable names to codes
        '''
        # Make a variable to Domain Map
        file_location = context.in_scope_file_directory + os.sep + "variable.csv"
        header_skipped = False
        # or each attribute add an Xattribute to the correct XClass represtnting the Entity
        # the attribute should have the correct type, which may be a specific
        # enumeration

        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if (not header_skipped):
                    header_skipped = True
                else:
                    long_name = row[context.variable_long_name_index]
                    variable_code = row[context.variable_code_index]
                    self.variable_name_to_code_map[Utils.make_valid_id(
                        long_name)] = variable_code

    def create_derivable_list(self,context):
        '''
        This method is used to create a list of derivable variables
        '''
        # Make a variable to Domain Map
        file_location = context.in_scope_file_directory + os.sep + "derivable.csv"
        header_skipped = False

        with open(file_location,  encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    long_name = row[1]
                    self.derivableList.append(long_name)
