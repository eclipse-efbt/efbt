package org.eclipse.efbt.cocalimo.computation.ui.sirius.attribute_lineage;

import org.eclipse.efbt.cocalimo.computation.query.attribute_lineage.AttributeLineageModelQuery;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.query.lineagefunctions.LineageFunctionsModelQuery;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.ecore.EClass;

import functions.AggregateFunction;
import functions.BasicFunction;
import functions.Function;
import cube_transformation_logic.CubeTransformationLogic;


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
    
    public  EList<EClass> getTheDependantEvaluatedTableSchemas(CubeTransformationLogic self)
    {

    	return AttributeLineageModelQuery.getTheDependantEClasss(self);
    	
    }
    
   public String getStringForFunction(Function self) {
       
    	
    	return LineageFunctionsModelQuery.getStringForFunction( (BasicFunction) self); 
     }
    
public String getStringForAggregateFunction(AggregateFunction self) {
       
    	
    	return LineageFunctionsModelQuery.getStringForAggregateFunction(self); 
     }

}
