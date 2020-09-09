package org.eclipse.efbt.cocamo.lineage.ui.sirius.attributelineage;


import org.eclipse.efbt.cocamo.core.model.functions.AggregateFunction;
import org.eclipse.efbt.cocamo.core.model.functions.BasicFunction;
import org.eclipse.efbt.cocamo.core.model.functions.Function;
import org.eclipse.efbt.cocamo.lineage.model.cube_transformation_logic.CubeTransformationLogic;
import org.eclipse.efbt.cocamo.lineage.model.row_transformation_logic.BaseRowStructure;
import org.eclipse.efbt.cocamo.lineage.query.attributelineage.AttributeLineageModelQuery;
import org.eclipse.efbt.cocamo.lineage.query.lineagefunctions.LineageFunctionsModelQuery;

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
