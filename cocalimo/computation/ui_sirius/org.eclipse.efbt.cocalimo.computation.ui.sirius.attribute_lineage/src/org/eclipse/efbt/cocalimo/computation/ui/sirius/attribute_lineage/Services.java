package org.eclipse.efbt.cocalimo.computation.ui.sirius.computation_lineage;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntity;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Function;
import org.eclipse.efbt.cocalimo.computation.query.computation_lineage.AttributeLineageModelQuery;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.query.lineagefunctions.LineageFunctionsModelQuery;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.ecore.EClass;




/**
 * The services class used by VSM.
 */
public class Services {
    

    public String replaceUnderscoreWithSpace(Function self) {

    	return (self.getName()).replace('_', ' ');
     }
    
      
   
    
    public  EList<DerivedEntity> getTheDependantFunctionalRowLogics(DerivedEntity self)
    {

    	return AttributeLineageModelQuery.getTheDependantFunctionalRowLogics(self);
    	
    }
    
    public  EList<EClass> getTheDependantEvaluatedTableSchemas(DerivedEntity self)
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
