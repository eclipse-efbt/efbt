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
import org.eclipse.efbt.model.util.TRLUtil;
import org.eclipse.efbt.model.util.NavigationContextUtil;

import org.eclipse.efbt.model.util.AttributeLineageUtil;
import org.eclipse.efbt.model.util.CubesAndViewsUtil;
import org.eclipse.efbt.model.util.Util;
import org.eclipse.efbt.model.platformcalls.PlatformCalls;

import cube_transformation_logic.CubeTransformationLogic;
import row_transformation_logic.BaseRowStructure;

import transformation.VersionedComponentsSet;
import transformation.TransformationScheme;


import platform_call.PlatformCall;
import functions.AggregateFunction;
import functions.BasicFunction;
import functions.Function;
import cubes.FreeBirdToolsCube;
import data_definition.CUBE_STRUCTURE_ITEM;
//import functions.LinkedColumnParameter;
import cube_schema.CubeSchema;
import transformation.VersionedCubeSchemaModule;
import trl_sql_views.EnrichmentView;
import trl_sql_views.JoinView;
import trl_sql_views.SQLView;
import trl_sql_views.VersionedSQLViewsModule;


import transformation.DataSetTransformation;





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


    
    
    public VersionedSQLViewsModule getDefaultSQLViewsModuleForTransformation(TransformationScheme self) {
       
    	
    	return TRLUtil.getDefaultSQLViewsModuleForTransformation(  self); 
     }
    
    public VersionedCubeSchemaModule getDefaultCubeSchemaModuleForTransformationScheme(TransformationScheme self) {
       
    	
    	return TRLUtil.getDefaultCubeSchemaModuleForTransformationScheme(  self); 
     }
  
  
public EList<CUBE_STRUCTURE_ITEM> getCubeStructureItemsFromDefaultBIRDModel(CubeSchema self) {
       
    	
    	return TRLUtil.getCubeStructureItemsFromDefaultBIRDModel(  self); 
     }
    
    
}
