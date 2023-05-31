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
from pyecore.resources import ResourceSet, URI
from pyecore.ecore import EEnum



class StandardBIRDQueries(object):
    '''
    Documentation for StandardBIRDQueries
    '''

    il_domains_model = None
    input_tables_model = None
    sdd_domains_model = None
    output_tables_model = None
    typesModel = None

    def query1(self, file_directory):
        '''
        Documentation for query1
        '''
        rset = ResourceSet()

        il_domains_resource = rset.get_resource(
            URI(file_directory + os.sep + "il_domains.ecore"))
        il_domains_root = il_domains_resource.contents[0]
        rset.metamodel_registry[il_domains_root.nsURI] = il_domains_root
        self.il_domains_model = il_domains_root

        sdd_domains_resource = rset.get_resource(
            URI(file_directory + os.sep + "sdd_domains.ecore"))
        sdd_domains_root = sdd_domains_resource.contents[0]
        rset.metamodel_registry[sdd_domains_root.nsURI] = sdd_domains_root
        self.sdd_domains_model = sdd_domains_root

        input_tables_resource = rset.get_resource(
            URI(file_directory + os.sep + "input_tables.ecore"))
        input_tables_root = input_tables_resource.contents[0]
        rset.metamodel_registry[input_tables_root.nsURI] = input_tables_root
        self.input_tables_model = input_tables_root

        output_tables_resource = rset.get_resource(
            URI(file_directory + os.sep + "output_tables.ecore"))
        output_tables_root = output_tables_resource.contents[0]
        rset.metamodel_registry[output_tables_root.nsURI] = output_tables_root
        self.output_tables_model = output_tables_root

        print("\n\n package Name")
        print(self.input_tables_model.name)

        print("\n\n All Classifiers - entities and domains")
        entities_and_domains = self.input_tables_model.eClassifiers
        print(entities_and_domains)

        print("\n\n First Classifiers - An Entity")
        an_entity = entities_and_domains[0]
        print(an_entity.name)

        print("\n\n An Entities attributes")
        the_attributes = an_entity.eAttributes
        print(the_attributes)

        print("\n\n  An Entities first attribute ")
        first_attribute = the_attributes[0]
        print(first_attribute.name)

        print("\n\n  the  attribute type ")
        attribute_type = first_attribute.eType
        print(attribute_type.name)

        print("\n\n  the  attribute allowed Values ")
        if isinstance(attribute_type, EEnum):
            attribute_allowed_values = first_attribute.eType.eLiterals
            print(attribute_allowed_values)

        # StandardBIRDQueries.createInputLayerToOutputLayerMatches(self,fileDirectory)
        rset2 = ResourceSet()
        il_domains_resource2 = rset2.create_resource(URI(
            file_directory + os.sep + "inputLayerEnums2.ecore"))  
        # This will create an XMI resource
        # we add the EPackage instance in the resource
        il_domains_resource2.append(self.il_domains_model)
        il_domains_resource2.save()
        sdd_domains_resource2 = rset2.create_resource(URI(
            file_directory + os.sep + "outputLayerEnums2.ecore"))  
        # This will create an XMI resource
        # we add the EPackage instance in the resource
        sdd_domains_resource2.append(self.sdd_domains_model)
        sdd_domains_resource2.save()
        input_tables_resource2 = rset2.create_resource(URI(
            file_directory + os.sep + "inputLayerEntities2.ecore"))
        # This will create an XMI resource
        # we add the EPackage instance in the resource
        input_tables_resource2.append(self.input_tables_model)
        input_tables_resource2.save()
        output_tables_resource2 = rset2.create_resource(URI(
            file_directory + os.sep + "outputLayerEntities2.ecore"))
        # This will create an XMI resource
        # we add the EPackage instance in the resource
        output_tables_resource2.append(self.output_tables_model)
        output_tables_resource2.save()


if __name__ == '__main__':
    standardBIRDQueries = StandardBIRDQueries()
    standardBIRDQueries.query1('/workspaces/efbt/ecore4reg/python/results')
