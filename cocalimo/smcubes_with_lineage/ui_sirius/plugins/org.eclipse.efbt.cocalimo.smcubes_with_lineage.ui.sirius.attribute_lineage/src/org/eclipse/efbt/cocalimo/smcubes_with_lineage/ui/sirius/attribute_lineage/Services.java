/*******************************************************************************
 * Copyright (c) 2020 Bird Software Solutions Ltd
 * This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License 2.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 *
 * Contributors:
 *    Neil Mackenzie - initial API and implementation
 *******************************************************************************/
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.ui.sirius.attribute_lineage;



import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cube_transformation_logic.CubeTransformationLogic;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.AggregateFunction;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.BasicFunction;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.Function;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.row_transformation_logic.BaseRowStructure;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.query.attribute_lineage.AttributeLineageModelQuery;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.query.lineagefunctions.LineageFunctionsModelQuery;
import org.eclipse.emf.common.util.EList;




/**
 * The services class used by VSM.
 */
public class Services {
    

    public String replaceUnderscoreWithSpace(Function self) {

    	return (self.getName()).replace('_', ' ');
     }
    
      
   
    
    public  EList<CubeTransformationLogic> getTheDependantFunctionalRowLogics(CubeTransformationLogic self)
    {

    	return AttributeLineageModelQuery.getTheDependantFunctionalRowLogics(self);
    	
    }
    
    public  EList<BaseRowStructure> getTheDependantEvaluatedTableSchemas(CubeTransformationLogic self)
    {

    	return AttributeLineageModelQuery.getTheDependantBaseRowStructures(self);
    	
    }
    
   public String getStringForFunction(Function self) {
       
    	
    	return LineageFunctionsModelQuery.getStringForFunction( (BasicFunction) self); 
     }
    
public String getStringForAggregateFunction(AggregateFunction self) {
       
    	
    	return LineageFunctionsModelQuery.getStringForAggregateFunction(self); 
     }

}
