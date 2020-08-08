package org.eclipse.efbt.almengine.ui.sirius;



import org.eclipse.efbt.almengine.component.modelquery.ALMModelQuery;
import org.eclipse.efbt.almengine.model.attribute_lineage.cube_transformation_logic.CubeTransformationLogic;
import org.eclipse.efbt.almengine.model.attribute_lineage.row_transformation_logic.BaseRowStructure;
import org.eclipse.efbt.aorta.aortafunctions.AggregateFunction;
import org.eclipse.efbt.aorta.aortafunctions.BasicFunction;
import org.eclipse.efbt.aorta.aortafunctions.Function;
import org.eclipse.efbt.aorta.base_column_structured_data.BaseRowData;
import org.eclipse.efbt.trl.component.translator.AttributeLineageUtil;
import org.eclipse.efbt.trl.component.modelquery.FunctionTextUtil;
import org.eclipse.emf.common.util.EList;


/**
 * The services class used by VSM.
 */
public class Services {
    
	 
   
    public String replaceUnderscoreWithSpace(Function self) {

    	return (self.getName()).replace('_', ' ');
     }
    
      
    public String getBaseRowID(BaseRowData self) {

       	
    	return ALMModelQuery.getBaseRowID( self); 
   }
    
    public  EList<CubeTransformationLogic> getTheDependantFunctionalRowLogics(CubeTransformationLogic self)
    {

    	return AttributeLineageUtil.getTheDependantFunctionalRowLogics(self);
    	
    }
    
    public  EList<BaseRowStructure> getTheDependantEvaluatedTableSchemas(CubeTransformationLogic self)
    {

    	return AttributeLineageUtil.getTheDependantBaseRowStructures(self);
    	
    }
    
    public String getStringForFunction(Function self) {
       
    	
    	return FunctionTextUtil.getStringForFunction( (BasicFunction) self); 
     }
    
public String getStringForAggregateFunction(AggregateFunction self) {
       
    	
    	return FunctionTextUtil.getStringForAggregateFunction(self); 
     }


/**public EList<CUBE_STRUCTURE_ITEM> getCubeStructureItemsFromDefaultBIRDModel(CubeSchema self) {
       
    	
    	return TRLUtil.getCubeStructureItemsFromDefaultBIRDModel(  self); 
     }*/
    

    
}
