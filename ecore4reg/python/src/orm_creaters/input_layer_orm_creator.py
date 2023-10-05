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

'''

@author: Neil
'''

class CreateORMForIL(object):
    '''
    Documentation for CreateORMForIL
    '''
    def create_orm_for_il(self, context):
        '''
        import the items from the Output layer csv files
        '''
        fullTableName = None
        if cube_type == "EIL":
            fullTableName=altered_class_name+"_Table"
        else:
            fullTableName=altered_class_name+"_Table"
            
                                    eclass_table = ELClass(
                            name=fullTableName)
                        eclass_table.containedEntityType = eclass
                        containment_reference = ELReference()
                        containment_reference.name = eclass.name+"s"
                        containment_reference.eType = eclass
                        containment_reference.upperBound = -1
                        containment_reference.lowerBound = 0
                        containment_reference.containment = True
                        eclass_table.eStructuralFeatures.append(
                            containment_reference)
                        
        if not (cube_type == "EIL"):
                            xclass_table_peration = ELPublicOperation()
                            xclass_table_peration.name = eclass.name+"s"
                            xclass_table_peration.eType = eclass
                            xclass_table_peration.upperBound = -1
                            xclass_table_peration.lowerBound = 0
                            eclass_table.eOperations.append(xclass_table_peration)
                            context.output_tables_package.eClassifiers.extend([
                                                                               eclass])
                            context.output_tables_package.eClassifiers.extend([
                                                                               eclass_table])
                        else:
                            
                context.input_tables_package.eClassifiers.extend([
                                                                               eclass_table])
                
            context.table_map[eclass] = eclass_table