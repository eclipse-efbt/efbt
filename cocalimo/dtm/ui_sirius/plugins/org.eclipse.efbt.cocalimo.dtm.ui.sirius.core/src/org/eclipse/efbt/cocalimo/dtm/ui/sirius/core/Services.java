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

package org.eclipse.efbt.cocalimo.dtm.ui.sirius.core;



import org.eclipse.efbt.language.dtm.model.transformation.VersionedComponentsSet;
import org.eclipse.efbt.language.dtm.model.transformation.VersionedCubeSchemaModule;
import org.eclipse.efbt.language.dtm.model.dtm_views.EnrichmentView;
import org.eclipse.efbt.language.dtm.model.dtm_views.JoinView;
import org.eclipse.efbt.language.dtm.model.dtm_views.DTMView;
import org.eclipse.efbt.language.dtm.model.dtm_views.VersionedDTMViewsModule;
import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cube_schema.CubeSchema;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBirdToolsCube;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.AggregateFunction;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.BasicFunction;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.Function;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.query.lineagefunctions.LineageFunctionsModelQuery;
import org.eclipse.efbt.cocalimo.dtm.query.core.CubesAndViewsUtil;
import org.eclipse.efbt.cocalimo.dtm.query.core.DTMModelQueryUtil;

import org.eclipse.emf.common.util.EList;


/**
 * The services class used by VSM.
 */
public class Services {
    
   
   
    
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
    


    
    
    public EList<FreeBirdToolsCube> getTables(VersionedDTMViewsModule self) {

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
    
    public EList<DTMView> getViews(VersionedComponentsSet self) {

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
    
    public String getViewDisplayName(DTMView view)
    {
    	return CubesAndViewsUtil.getDTMViewDisplayName(view);
    }
    

    
    public String getStringForFunction(Function self) {
       
    	
    	return LineageFunctionsModelQuery.getStringForFunction( (BasicFunction) self); 
     }
    
public String getStringForAggregateFunction(AggregateFunction self) {
       
    	
    	return LineageFunctionsModelQuery.getStringForAggregateFunction(self); 
     }




    
/**    public VersionedDTMViewsModule getDefaultDTMViewsModuleForTransformation(LeafFunctionalModule self) {
       
    	
    	return dtmModelQueryUtil.getDefaultDTMViewsModuleForTransformation(  self); 
     }
    
    public VersionedCubeSchemaModule getDefaultCubeSchemaModuleForFunctionalModule(LeafFunctionalModule self) {
       
    	
    	return dtmUtil.getDefaultCubeSchemaModuleForFunctionalModule(  self); 
     }
  */
  
public EList<CUBE_STRUCTURE_ITEM> getCubeStructureItemsFromDefaultBIRDModel(CubeSchema self) {
       
    	
    	return DTMModelQueryUtil.getCubeStructureItemsFromDefaultBIRDModel(  self); 
     }
    
/**
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
