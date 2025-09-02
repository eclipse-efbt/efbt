# coding=UTF-8
# Copyright (c) 2024 Bird Software Solutions Ltd
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

'''
Created on 22 Jan 2022

@author: Neil
'''

import unidecode

from pybirdai.regdna import ELReference


class Utils(object):
    '''
    Documentation for Utils
    '''

    @classmethod
    def unique_value(cls, the_enum, adapted_value):
        '''
            if the adapted value already exists in the enum then
            append it with _x2
            if the that string appended with _x2 already exists,
            then append with_x3 instead
            if that exists then _x4 etc.
        '''
        new_adapted_value = adapted_value
        if Utils.contains_literal(the_enum.eLiterals, adapted_value):
            new_adapted_value = adapted_value + "_x2"
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
        while ((counter < limit) and not (finished)):
            counter = counter + 1
            if Utils.contains_literal(the_enum.eLiterals, adapted_value + "_x" + str(counter)):
                new_adapted_value = adapted_value + "_x" + str(counter+1)
            else:
                finished = True

        return new_adapted_value

    @classmethod
    def unique_name(cls, the_enum, enum_used_name):
        '''
        if the adapted name already exists in the enum then append it with _x2
        if the that string appended with _x2 already exists, then append with_x3 instead
        if that exists then _x4 etc.
        '''
        new_adapted_name = enum_used_name
        counter = 1
        finished = False
        limit = 32
        if Utils.contains_name(the_enum.eLiterals, enum_used_name):
            new_adapted_name = enum_used_name + "_x2"

        while (counter < limit) and not finished:
            counter = counter + 1
            if Utils.contains_name(the_enum.eLiterals, enum_used_name + "_x" + str(counter)):
                new_adapted_name = enum_used_name + "_x" + str(counter+1)
            else:
                finished = True

        return new_adapted_name

    @classmethod
    def make_valid_id_for_literal(cls, input_string):
        '''
        Tranlate text to be a valid id, without special characters, and following
        the rules for valid id's in regdna
        '''
        amended_input_string = input_string.replace('  ', ' ').replace(' ', '_').replace(')', '_').replace('(', '_') \
            .replace(',', '_').replace('\'', '_').replace('\n', '_').replace('\r', '_').replace('\'t', '_').replace('new', 'New') \
            .replace('\\', '_').replace('/', '_').replace(':', '_') \
            .replace('+', '_').replace('.', '_').replace('?', '_').replace('\'', '_').replace('>', '_gt') \
            .replace('<', '_lt').replace('\"', '_').replace(';', '_').replace('$', '_').replace('=', '_eq').replace('#', '_') \
            .replace('&', '_').replace('%', '_').replace('[', '_').replace(']', '_').replace('?', '_').replace('–', '_').replace('__', '_').replace('__', '_') \
            .replace(chr(0x2019), '_').replace(chr(65533), '_').replace(chr(0x00A8), '_').replace(chr(0x00A9), '_')  \
            .replace(chr(0x00A4), '_').replace(chr(0x00B6), '_').replace(chr(0x00D0), '_').replace(chr(0x00BA), '_') \
            .replace(chr(0x2020), '_').replace(chr(0x00B5), '_').replace(chr(0x20AC), '_').replace(chr(0x00B4), '_') \
            .replace(chr(0x0192), '_').replace(chr(0x00B2), '_').replace(chr(0x00BF), '_').replace(chr(0x00B0), '_') \
            .replace(chr(0x00A6), '_').replace(chr(0x203A), '_').replace(chr(0x00A2), '_').replace(chr(0x2122), '_') \
            .replace(chr(0x00B1), '_').replace(chr(0x00B9), '_').replace(chr(0x00AE), '_').replace(chr(0x2014), '_') \
            .replace(chr(0x02DC), '_').replace(chr(0x201E), '_').replace(chr(0x2026), '_').replace(chr(0x00BF), '_') \
            .replace(chr(0x00BB), '_').replace(chr(0x00AB), '_').replace(chr(0x2022), '_').replace(chr(0x00AC), '_') \
            .replace(chr(0x2021), '_').replace(chr(0x00A5), '_').replace(chr(0x201E), '_').replace(chr(0x201C), '_') \
            .replace(chr(0x00AF), '_').replace(chr(0x201D), '_').replace(chr(0x00A3), '_').replace(chr(0x2030), '_') \
            .replace(chr(0x00BD), '_').replace(chr(0x00BC), '_').replace(chr(0x00BE), '_').replace(chr(0x00A1), '_') \
            .replace(chr(0x2018), '_').replace(chr(0x0060), '_').replace(chr(0x00B4), '_').replace(chr(0x2026), '_') \
            .replace(chr(0x200B), '_').replace(chr(0x202F), '_').replace(chr(0x205F), '_').replace(chr(0x3000), '_') \
            .replace(chr(0x2000), '_').replace(chr(0x2001), '_').replace(chr(0x2002), '_').replace(chr(0x2003), '_') \
            .replace(chr(0x2004), '_').replace(chr(0x2005), '_').replace(chr(0x2006), '_').replace(chr(0x2007), '_') \
            .replace(chr(0x2008), '_').replace(chr(0x2009), '_').replace(chr(0x200A), '_').replace(chr(0x00A0), '_') \
            .replace(chr(0x0027), '_').replace(chr(0x2019), '_').replace(chr(0x2018), '_').replace(chr(0x201A), '_').replace(chr(0x00B7), '_')


        return_string = Utils.replace_acutes_graves_and_circumflexes(
            amended_input_string).replace('\'', '_')

        if return_string == "op":
            return_string = "_op"
        return return_string

    @classmethod
    def make_valid_id(cls, input_string):
        '''
        Tranlate text to be a valid id, without special characters, and following
        the rules for valid id's in regdna
        '''

        # we do not allow id's to start with  number, if it does then we prepend with an underscore
        if len(input_string) > 0:
            if ((input_string[0] >= '0') and (input_string[0] <= '9')):
                input_string = "_" + input_string
        # we replace special characters not allowed in id's with an underscore
        amended_input_string = input_string.replace('  ', ' ').replace(' ', '_').replace(')', '_').replace('(', '_') \
            .replace(',', '_').replace('\'', '_').replace('\n', '_').replace('\r', '_').replace('\'t', '_').replace('new', 'New') \
            .replace('\\', '_').replace('/', '_').replace('-', '_').replace(':', '_') \
            .replace('+', '_').replace('.', '_').replace('?', '_').replace('\'', '_').replace('>', '_gt') \
            .replace('<', '_lt').replace('\"', '_').replace(';', '_').replace('$', '_').replace('=', '_eq').replace('#', '_') \
            .replace('&', '_').replace('%', '_').replace('[', '_').replace(']', '_').replace('?', '_').replace('–', '_').replace('__', '_').replace('__', '_') \
            .replace(chr(0x2019), '_').replace(chr(65533), '_').replace(chr(0x00A8), '_').replace(chr(0x00A9), '_')  \
            .replace(chr(0x00A4), '_').replace(chr(0x00B6), '_').replace(chr(0x00D0), '_').replace(chr(0x00BA), '_') \
            .replace(chr(0x2020), '_').replace(chr(0x00B5), '_').replace(chr(0x20AC), '_').replace(chr(0x00B4), '_') \
            .replace(chr(0x0192), '_').replace(chr(0x00B2), '_').replace(chr(0x00BF), '_').replace(chr(0x00B0), '_') \
            .replace(chr(0x00A6), '_').replace(chr(0x203A), '_').replace(chr(0x00A2), '_').replace(chr(0x2122), '_') \
            .replace(chr(0x00B1), '_').replace(chr(0x00B9), '_').replace(chr(0x00AE), '_').replace(chr(0x2014), '_') \
            .replace(chr(0x02DC), '_').replace(chr(0x201E), '_').replace(chr(0x2026), '_').replace(chr(0x00BF), '_') \
            .replace(chr(0x00BB), '_').replace(chr(0x00AB), '_').replace(chr(0x2022), '_').replace(chr(0x00AC), '_') \
            .replace(chr(0x2021), '_').replace(chr(0x00A5), '_').replace(chr(0x201E), '_').replace(chr(0x201C), '_') \
            .replace(chr(0x00AF), '_').replace(chr(0x201D), '_').replace(chr(0x00A3), '_').replace(chr(0x2030), '_') \
            .replace(chr(0x00BD), '_').replace(chr(0x00BC), '_').replace(chr(0x00BE), '_').replace(chr(0x00A1), '_') \
            .replace(chr(0x2018), '_').replace(chr(0x0060), '_').replace(chr(0x00B4), '_').replace(chr(0x2026), '_') \
            .replace(chr(0x200B), '_').replace(chr(0x202F), '_').replace(chr(0x205F), '_').replace(chr(0x3000), '_') \
            .replace(chr(0x2000), '_').replace(chr(0x2001), '_').replace(chr(0x2002), '_').replace(chr(0x2003), '_') \
            .replace(chr(0x2004), '_').replace(chr(0x2005), '_').replace(chr(0x2006), '_').replace(chr(0x2007), '_') \
            .replace(chr(0x2008), '_').replace(chr(0x2009), '_').replace(chr(0x200A), '_').replace(chr(0x00A0), '_') \
            .replace(chr(0x0027), '_').replace(chr(0x2019), '_').replace(chr(0x2018), '_').replace(chr(0x201A), '_').replace(chr(0x00B7), '_')


        return_string = Utils.replace_acutes_graves_and_circumflexes(
            amended_input_string).replace('\'', '_')

        if return_string == "op":
            return_string = "_op"

        return_string = return_string.replace('__', '_').replace('__', '_').replace('__', '_').replace('__', '_').replace('__', '_').replace('__', '_')
        if return_string.endswith('_'):
            return_string = return_string[0:len(return_string)-1]
        return return_string

    @classmethod
    def replace_acutes_graves_and_circumflexes(cls, the_input_string):
        '''
        We replace letters with acutes , graves, and circumflexes, with the basic letter.
        So for example "a acute" is replaced with "a"
        '''
        return unidecode.unidecode(the_input_string)


    @classmethod
    def contains_literal(cls, members, adapted_value):
        '''
        checks if an enum contains a particular literal
        '''
        contains = False
        for e_enum_literal in members:
            if e_enum_literal.name.lower() == adapted_value.lower():
                contains = True

        return contains

    @classmethod
    def contains_name(cls, members, adapted_name):
        '''
        checks if an enum contains a particular name
        '''
        contains = False
        for e_enum_literal in members:
            if e_enum_literal.literal.lower() == adapted_name.lower():
                contains = True

        return contains

    @classmethod
    def get_literals_for_enumeration(cls, domain, members_module):
        '''
        returns the list of literals for an enumerations
        '''
        return_members_list = []
        for member in members_module.members:
            if member.domain_id == domain:
                return_members_list.append(member)
        return return_members_list

    @classmethod
    def get_ecore_datatype_for_datatype(cls, context):
        '''
        returns the ecore data type for a data type
        '''
        return context.e_string


    @classmethod
    def get_annotation_with_source(cls,element, source):
        '''
        returns the annotation with the source
        '''
        return_annotation = None
        for annotation in element.eAnnotations:
            if annotation.source is not None:
                if annotation.source.name == source:
                    return_annotation = annotation
            else:
                print("no source for annotation2" + element.name)

        return return_annotation

    @classmethod
    def get_annotation_directive(cls,package, name):
        '''
        returns the annotation directive with the name
        '''
        return_annotation_directive = None
        for annotation_directive in package.annotationDirectives:
            if annotation_directive.name == name:
                return_annotation_directive = annotation_directive
        return return_annotation_directive

    @classmethod
    def number_of_relationships_to_this_class(cls, source_class, target_class):
        '''
        Checks how many relationships there are between 2 classes
        It is possible that one class might have 2 different relationships
        to the same class.
        '''
        features = source_class.eStructuralFeatures
        counter = 0
        # do this for relationship attributes only.
        for feature in features:
            if isinstance(feature, ELReference):
                feature_type = feature.eType
                if feature_type == target_class:
                    counter = counter+1

        return counter
