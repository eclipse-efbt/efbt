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
 *  Neil Mackenzie - initial API and implementation
 *******************************************************************************/

package org.eclipse.efb.trl.ui.sirius;



import  org.eclipse.efbt.trl.component.modelquery.CubesAndViewsUtil;
import org.eclipse.efbt.trl.component.modelquery.FunctionTextUtil;
import org.eclipse.efbt.trl.component.modelquery.TRLModelQueryUtil;
import org.eclipse.efbt.almengine.model.cubes.cube_schema.CubeSchema;
import org.eclipse.efbt.almengine.model.cubes.cubes.FreeBirdToolsCube;
import org.eclipse.efbt.aorta.aortafunctions.AggregateFunction;
import org.eclipse.efbt.aorta.aortafunctions.BasicFunction;
import org.eclipse.efbt.aorta.aortafunctions.Function;
import org.eclipse.efbt.trl.model.trl.transformation.VersionedComponentsSet;
import org.eclipse.efbt.trl.model.trl.transformation.VersionedCubeSchemaModule;
import org.eclipse.efbt.trl.model.trl.trl_sql_views.EnrichmentView;
import org.eclipse.efbt.trl.model.trl.trl_sql_views.JoinView;
import org.eclipse.efbt.trl.model.trl.trl_sql_views.SQLView;
import org.eclipse.efbt.trl.model.trl.trl_sql_views.VersionedSQLViewsModule;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.ecore.EObject;













/**
 * The services class used by VSM.
 */
public class Services {
    
   
    public EObject myService(EObject self, String arg) {
      
      return self;
    }
    
    public String replaceUnderscoreWithSpace(JoinView self) {

    	return (self.getName()).replace('_', ' ');
     }
    
    public String replaceUnderscoreWithSpace(EnrichmentView self) {

    	return (self.getName()).replace('_', ' ');
     }
    
    public String replaceUnderscoreWithSpace(Function self) {

    	return (self.getName()).replace('_', ' ');
     }
    


    
   /** public EList<CodeCondition> getCodeConditionsFromDefualtNavigationContext(Scenario scenario)
    {
    	return FunctionalModuleUtil.getCodeConditionsFromDefualtNavigationContext( scenario);
    	
    }*/
   
    
    public EList<FreeBirdToolsCube> getTables(VersionedComponentsSet self) {

    	return CubesAndViewsUtil.getCubes( self);
     }
    
       
    public EList<FreeBirdToolsCube> getBaseTables(VersionedComponentsSet self) {

    	return CubesAndViewsUtil.getBaseCubes( self);
     }
    
    public EList<FreeBirdToolsCube> getBaseTablesButNotDeltaTables(VersionedComponentsSet self) {

    	return CubesAndViewsUtil.getBaseTablesButNotDeltaTables( self);
     }
    
    public EList<FreeBirdToolsCube> getBaseDeltaCubes(VersionedComponentsSet self) {

    	return CubesAndViewsUtil.getBaseDeltaCubes( self);
     }
    
    public EList<FreeBirdToolsCube> getBaseViewCubes(VersionedComponentsSet self) {

    	return CubesAndViewsUtil.getBaseViewCubes( self);
     }


    
    
    public EList<FreeBirdToolsCube> getTables(VersionedSQLViewsModule self) {

    	return CubesAndViewsUtil.getCubes( self);
     }
    
   
 
 
  
    public EList<FreeBirdToolsCube> getTargetTables(VersionedComponentsSet self) {

    	return CubesAndViewsUtil.getTargetTables( self); 
     }
    
    public EList<FreeBirdToolsCube> getDerivedTablesButNotTargetTables(VersionedComponentsSet self) {

    	return CubesAndViewsUtil.getDerivedTablesButNotTargetOrBaseViewTables( self);  
     }
        
    
    public EList<FreeBirdToolsCube> getTables(VersionedCubeSchemaModule self) {

    	return CubesAndViewsUtil.getCubes( self);
     }
    
    public EList<SQLView> getViews(VersionedComponentsSet self) {

    	return CubesAndViewsUtil.getViews( self);
     }
    
 
   
    public String getSchemaDisplayName(CubeSchema schema)
    {
    	return CubesAndViewsUtil.getSchemaDisplayName(schema);
    }
    
    public String getTableDisplayName(FreeBirdToolsCube table)
    {
    	return CubesAndViewsUtil.getCubeDisplayName(table);
    }
    
    public String getViewDisplayName(SQLView view)
    {
    	return CubesAndViewsUtil.getSQLViewDisplayName(view);
    }
    
  
    
    
  
   
    
    public String getStringForFunction(Function self) {
       
    	
    	return FunctionTextUtil.getStringForFunction( (BasicFunction) self); 
     }
    
public String getStringForAggregateFunction(AggregateFunction self) {
       
    	
    	return FunctionTextUtil.getStringForAggregateFunction(self); 
     }




    
/**    public VersionedSQLViewsModule getDefaultSQLViewsModuleForTransformation(LeafFunctionalModule self) {
       
    	
    	return TRLModelQueryUtil.getDefaultSQLViewsModuleForTransformation(  self); 
     }
    
    public VersionedCubeSchemaModule getDefaultCubeSchemaModuleForFunctionalModule(LeafFunctionalModule self) {
       
    	
    	return TRLUtil.getDefaultCubeSchemaModuleForFunctionalModule(  self); 
     }
  */
  /**
public EList<CUBE_STRUCTURE_ITEM> getCubeStructureItemsFromDefaultBIRDModel(CubeSchema self) {
       
    	
    	return TRLUtil.getCubeStructureItemsFromDefaultBIRDModel(  self); 
     }
    

public EList<BooleanFunction> getCodeConditionFilterFunctions(Scenario self) {
    
	
	return FunctionalModuleUtil.getCodeConditionFilterFunctions(  self); 
 }

public EList<DataSetFilterCondition> getDataSetFilterCodeConditionsFromDefualtNavigationContext(Scenario self) {
    
	
	return FunctionalModuleUtil.getDataSetFilterCodeConditionsFromDefualtNavigationContext(  self); 
 }

public EList<ColumnSettingCondition> getColumnSettingCodeConditionsFromDefualtNavigationContext(Scenario self) {
    
	
	return FunctionalModuleUtil.getColumnSettingCodeConditionsFromDefualtNavigationContext(  self); 
 }
*/
    
}
