package org.eclipse.efbt.cocalimo.computation.ui.sirius.attribute_lineage;

import org.eclipse.emf.ecore.EObject;

/**
 * The services class used by VSM.
 */
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
