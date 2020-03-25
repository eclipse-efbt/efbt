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
package org.eclipse.efbt.model.design; 

import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.ecore.EObject;

import core.DOMAIN;
import core.MEMBER;

import org.eclipse.efbt.model.util.FunctionTextUtil;
import org.eclipse.efbt.model.util.FunctionalModuleUtil;
import org.eclipse.efbt.model.util.TRLUtil;
import org.eclipse.efbt.model.util.TagUtil;


import org.eclipse.efbt.model.util.AttributeLineageUtil;
import org.eclipse.efbt.model.util.CubesAndViewsUtil;
import org.eclipse.efbt.model.platformcalls.PlatformCalls;

import cube_transformation_logic.CubeTransformationLogic;
import row_transformation_logic.BaseRowStructure;
import scenarios.Scenario;
import tags.ScenarioTag;
import tags.FunctionalModuleTag;
import transformation.VersionedComponentsSet;
import functional_module.LeafFunctionalModule;
import functional_module.FunctionalModule;


import platform_call.PlatformCall;
import requirements_text.TitledRequirementsSection;
import functions.AggregateFunction;
import functions.BasicFunction;
import functions.BooleanFunction;
import functions.Function;
import cubes.FreeBirdToolsCube;
import data_definition.CUBE_STRUCTURE_ITEM;
import domain_model_mapping.CodeCondition;
import domain_model_mapping.ColumnSettingCondition;
import domain_model_mapping.DataSetFilterCondition;
//import functions.LinkedColumnParameter;
import cube_schema.CubeSchema;
import transformation.VersionedCubeSchemaModule;
import trl_sql_views.EnrichmentView;
import trl_sql_views.JoinView;
import trl_sql_views.SQLView;
import trl_sql_views.VersionedSQLViewsModule;






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
    
    public EList<FunctionalModule> getPreviousFunctionalModule(FunctionalModule self) {

    	return FunctionalModuleUtil.getPreviousFunctionalModules( self);
     }

    
    public EList<CodeCondition> getCodeConditionsFromDefualtNavigationContext(Scenario scenario)
    {
    	return FunctionalModuleUtil.getCodeConditionsFromDefualtNavigationContext( scenario);
    	
    }
   
    
    public EList<FreeBirdToolsCube> getTables(VersionedComponentsSet self) {

    	return CubesAndViewsUtil.getCubes( self);
     }
    
       
    public EList<FreeBirdToolsCube> getBaseTables(VersionedComponentsSet self) {

    	return CubesAndViewsUtil.getBaseCubes( self);
     }
    
    
    public EList<FreeBirdToolsCube> getTables(VersionedSQLViewsModule self) {

    	return CubesAndViewsUtil.getCubes( self);
     }
    
    public EList<MEMBER> getMembers(DOMAIN self) {

    	return TRLUtil.getMembers( self);
     }
 
 
  
    public EList<FreeBirdToolsCube> getTargetTables(VersionedComponentsSet self) {

    	return CubesAndViewsUtil.getTargetTables( self); 
     }
    
    public EList<FreeBirdToolsCube> getDerivedTablesButNotTargetTables(VersionedComponentsSet self) {

    	return CubesAndViewsUtil.getDerivedCubesButNotTargetCubes( self); 
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
    
  
    
    public void executePlatformCall(PlatformCall self) {

    	
    	PlatformCalls.executeCall( self);
    }
    
    public void executePlatformCallWithPaths(PlatformCall self,  String str1, String str2) {

   	
   	PlatformCalls.executeCall( self);
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


public EList<FunctionalModuleTag>  getTransformationTagsFromDefualtTagGroup(FunctionalModule self) {
    
	
	return TagUtil.getFunctionalModuleTagsFromDefualtTagGroup(  self); 
 }

public EList<TitledRequirementsSection> getTransformationRequirementsFromDefualtTagGroup(LeafFunctionalModule self) {
    
	
	return TagUtil.getFunctionalModuleRequirementsFromDefualtTagGroup(  self); 
 }

public EList<ScenarioTag>  getScenarioTagsFromDefualtTagGroup(Scenario self) {
    
	
	return TagUtil.getScenarioTagsFromDefualtTagGroup(  self); 
 }

public EList<TitledRequirementsSection> getScenarioRequirementsFromDefualtTagGroup(Scenario self) {
    
	
	return TagUtil.getScenarioRequirementsFromDefualtTagGroup(  self);   
 }


    
    public VersionedSQLViewsModule getDefaultSQLViewsModuleForTransformation(LeafFunctionalModule self) {
       
    	
    	return TRLUtil.getDefaultSQLViewsModuleForTransformation(  self); 
     }
    
    public VersionedCubeSchemaModule getDefaultCubeSchemaModuleForFunctionalModule(LeafFunctionalModule self) {
       
    	
    	return TRLUtil.getDefaultCubeSchemaModuleForFunctionalModule(  self); 
     }
  
  
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

    
}
