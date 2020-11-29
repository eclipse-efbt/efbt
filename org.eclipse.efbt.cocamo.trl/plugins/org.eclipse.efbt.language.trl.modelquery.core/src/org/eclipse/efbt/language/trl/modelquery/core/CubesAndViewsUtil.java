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
package org.eclipse.efbt.language.trl.modelquery.core;



import java.util.Iterator;

import org.eclipse.efbt.language.trl.model.transformation.VersionedComponentsSet;
import org.eclipse.efbt.language.trl.model.transformation.VersionedCubeSchemaModule;
import org.eclipse.efbt.language.trl.model.transformation.VersionedFunctionalModuleLogic;
import org.eclipse.efbt.language.trl.model.trl_report_cell_views.ReportCellViewModule;
import org.eclipse.efbt.language.trl.model.trl_sql_views.SQLView;
import org.eclipse.efbt.language.trl.model.trl_sql_views.VersionedSQLViewsModule;
import org.eclipse.efbt.lineage.common.model.lineagecubes.cube_schema.CubeSchema;
import org.eclipse.efbt.lineage.common.model.lineagecubes.cubes.BaseCube;
import org.eclipse.efbt.lineage.common.model.lineagecubes.incremental_cubes.BaseDeltaCube;
import org.eclipse.efbt.lineage.common.model.lineagecubes.incremental_cubes.BaseViewCube;
import org.eclipse.efbt.lineage.common.model.lineagecubes.cubes.DerivedCube;
import org.eclipse.efbt.lineage.common.model.lineagecubes.cubes.FreeBirdToolsCube;
import org.eclipse.efbt.lineage.common.model.lineagecubes.cubes.TargetCube;
import org.eclipse.emf.common.util.BasicEList;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.TreeIterator;
import org.eclipse.emf.ecore.EObject;





/**
 * @author Neil Mackenzie
 *
 */
public class CubesAndViewsUtil {

	/**
	 * Provides a UI display name for a cube
	 * @param cube
	 * @return
	 */
	public static String getCubeDisplayName(FreeBirdToolsCube cube)
	{
		if  (cube instanceof BaseCube)
			return "INPUT LAYER CUBE: " + cube.getName().replace('_', ' ');
		else if  (cube instanceof TargetCube)
			return "REPORT CELL: " + cube.getName().replace('_', ' ');
		else if(cube instanceof BaseDeltaCube)
			return "BASE DELTA CUBE: " + cube.getName().replace('_', ' ');
		else if  (cube instanceof BaseViewCube)
			return "BASE VIEW CUBE: " + cube.getName().replace('_', ' ');
		else 
			return "TRANSFORMATION: " + cube.getName().replace('_', ' ');
	}

	

	/**
	 * Privides a UI display name for a CubeSchema.
	 * 
	 * @param schema
	 * @return
	 */
	public static String getSchemaDisplayName(CubeSchema schema)
	{
		return "SCHEMA: " + schema.getBaseCube().getName().replace('_', ' ');
	}
	
	/**
	 * Provides a UI display name for an SQLView.
	 * 
	 * @param view
	 * @return
	 */
	public static String getSQLViewDisplayName(SQLView view)
	{
		String cn = view.getClass().getSimpleName();
		String vn = "";
		if (cn.equals("JoinViewImpl"))
			vn = "JOIN TRANFORMATION";
		if (cn.equals("EnrichmentViewImpl"))
			vn = "ENRICHMENT TRANFORMATION";
		if (cn.equals("FilterByConditionViewImpl"))
			vn = "FILTER BY CONDITION VIEW";
		if (cn.equals("AggregateEnrichmentViewImpl"))
			vn = "GROUP_BY TRANFORMATION";
		if (cn.equals("CopyViewImpl"))
			vn = "COPY TRANFORMATION";
		if (cn.equals("UnionViewImpl"))
			vn = "UNION TRANFORMATION";
		if (cn.equals("GenericViewImpl"))
			vn = "GENERIC TRANFORMATION";
		if (cn.equals("BaseViewIncorporatingDeltasImpl"))
			vn = "BASE VIEW";
		if (cn.equals("DeltaAccumulationImpl"))
			vn = "DELTA ACCUMULATION";
		
		
		
		
		
		return "LOGIC: " + vn; 
	}


	

	
	
	/**
	 * Returns all the BaseCubes referenced in a VersionedComponentsSet.
	 * 
	 * @param versionedComponentsSet
	 * @return
	 */
	public static EList<FreeBirdToolsCube> getBaseCubes ( VersionedComponentsSet versionedComponentsSet)
	{
		
		EList<FreeBirdToolsCube> cubeList = getCubes(versionedComponentsSet);
		EList<FreeBirdToolsCube> returnCubeList = new BasicEList<FreeBirdToolsCube>();
		Iterator<FreeBirdToolsCube> cubeListIter = cubeList.iterator();
		while (cubeListIter.hasNext())
		{
			FreeBirdToolsCube t = cubeListIter.next();
			if( (t instanceof BaseCube) )
				returnCubeList.add(t);
		}
		return returnCubeList;
	}
	
	public static EList<FreeBirdToolsCube> getBaseTablesButNotDeltaTables ( VersionedComponentsSet context)
	{
		
		EList<FreeBirdToolsCube> tableList = getCubes(context);
		EList<FreeBirdToolsCube> returnvalue = new BasicEList<FreeBirdToolsCube>();
		Iterator<FreeBirdToolsCube> tableListIter = tableList.iterator();
		while (tableListIter.hasNext())
		{
			FreeBirdToolsCube t = tableListIter.next();
			if( (t instanceof BaseCube) && !(t instanceof BaseDeltaCube) && !(t instanceof BaseViewCube))
				returnvalue.add(t);
		}
		return returnvalue;
	}
	
	public static EList<FreeBirdToolsCube> getBaseViewCubes ( VersionedComponentsSet context)
	{
		
		
			EList<FreeBirdToolsCube> tableList = getCubes(context);
			EList<FreeBirdToolsCube> returnvalue = new BasicEList<FreeBirdToolsCube>();
			Iterator<FreeBirdToolsCube> tableListIter = tableList.iterator();
			while (tableListIter.hasNext())
			{
				FreeBirdToolsCube t = tableListIter.next();
				if( t instanceof BaseViewCube)
					returnvalue.add(t);
			}
			return returnvalue;
		
	}
	
	public static EList<FreeBirdToolsCube> getBaseDeltaCubes ( VersionedComponentsSet context)
	{
		
		EList<FreeBirdToolsCube> tableList = getCubes(context);
		EList<FreeBirdToolsCube> returnvalue = new BasicEList<FreeBirdToolsCube>();
		Iterator<FreeBirdToolsCube> tableListIter = tableList.iterator();
		while (tableListIter.hasNext())
		{
			FreeBirdToolsCube t = tableListIter.next();
			if( t instanceof BaseDeltaCube)
				returnvalue.add(t);
		}
		return returnvalue;
	}
	

	
	
	
	/**
	 * Returns all the BaseCubes referenced in a VersionedComponentsSet.
	 * 
	 * @param versionedComponentsSet
	 * @return
	 */
	public static EList<FreeBirdToolsCube> getCubes ( VersionedComponentsSet versionedComponentsSet)
	{
	
		EList<FreeBirdToolsCube> returnCubelist = new BasicEList<FreeBirdToolsCube>();
		 EList<VersionedFunctionalModuleLogic> functionalModuleLogic = versionedComponentsSet.getDatasetTransformationModules();
		 EList<ReportCellViewModule> reportcellmodules = versionedComponentsSet.getReportCellViewModules();
		EList<VersionedCubeSchemaModule> cubeSchemaModules = versionedComponentsSet.getCubeSchemaModules();
		Iterator<VersionedFunctionalModuleLogic> functionalModuleLogicIter = functionalModuleLogic.iterator();
		Iterator<VersionedCubeSchemaModule> cubeSchemaModulesIter = cubeSchemaModules.iterator();
		Iterator<ReportCellViewModule> reportcellmodulesIter = reportcellmodules.iterator();
		while (functionalModuleLogicIter.hasNext())
		{
			VersionedFunctionalModuleLogic module = functionalModuleLogicIter.next();
			
			TreeIterator<EObject> moduleContents = module.eAllContents();
			while(moduleContents.hasNext())
			{
				EObject object = moduleContents.next();
				if(object instanceof SQLView)
					returnCubelist.add(((SQLView) object).getCube());
				
			}
		}
		
		while (reportcellmodulesIter.hasNext())
		{
			ReportCellViewModule module = reportcellmodulesIter.next();
			
			TreeIterator<EObject> moduleContents = module.eAllContents();
			while(moduleContents.hasNext())
			{
				EObject object = moduleContents.next();
				if(object instanceof SQLView)
					returnCubelist.add(((SQLView) object).getCube());
				
			}
		}
		
		while (cubeSchemaModulesIter.hasNext())
		{
			VersionedCubeSchemaModule module = cubeSchemaModulesIter.next();
			
			TreeIterator<EObject> moduleContents = module.eAllContents();
			while(moduleContents.hasNext())
			{
				EObject object = moduleContents.next();
				if(object instanceof CubeSchema)
					returnCubelist.add(((CubeSchema) object).getBaseCube());
				
			}
		}
		
		return returnCubelist;
		
	}
	
	/**
	 * Returns all the Derived Cubes but not TargetCubes referenced in a VersionedComponentsSet.
	 * @param versionedComponentsSet
	 * @return
	 */
	public static EList<FreeBirdToolsCube> getDerivedCubesButNotTargetCubes ( VersionedComponentsSet versionedComponentsSet)
	{
		EList<FreeBirdToolsCube> cubeList = getCubes(versionedComponentsSet);
		EList<FreeBirdToolsCube> returnvalue = new BasicEList<FreeBirdToolsCube>();
		Iterator<FreeBirdToolsCube> cubeListIter = cubeList.iterator();
		while (cubeListIter.hasNext())
		{
			FreeBirdToolsCube t = cubeListIter.next();
			if( (t instanceof DerivedCube) && !(t instanceof TargetCube) )
				returnvalue.add(t);
		}
		return returnvalue;
	}
	
	public static EList<FreeBirdToolsCube> getDerivedTablesButNotTargetOrBaseViewTables ( VersionedComponentsSet context)
	{
		EList<FreeBirdToolsCube> tableList = getCubes(context);
		EList<FreeBirdToolsCube> returnvalue = new BasicEList<FreeBirdToolsCube>();
		Iterator<FreeBirdToolsCube> tableListIter = tableList.iterator();
		while (tableListIter.hasNext())
		{
			FreeBirdToolsCube t = tableListIter.next();
			if( (t instanceof DerivedCube) && !(t instanceof TargetCube) && !(t instanceof BaseViewCube))
				returnvalue.add(t);
		}
		return returnvalue;
	}
	
	
	
	/**
	 * Returns all the TargetCubes referenced in a VersionedComponentsSet.
	 *  
	 * @param versionedComponentsSet
	 * @return
	 */
	public static EList<FreeBirdToolsCube> getTargetTables ( VersionedComponentsSet versionedComponentsSet)
	{
		EList<FreeBirdToolsCube> cubeList = getCubes(versionedComponentsSet);
		EList<FreeBirdToolsCube> returnvalue = new BasicEList<FreeBirdToolsCube>();
		Iterator<FreeBirdToolsCube> cubeListIter = cubeList.iterator();
		while (cubeListIter.hasNext())
		{
			FreeBirdToolsCube t = cubeListIter.next();
			if( t instanceof TargetCube)
				returnvalue.add(t);
		}
		return returnvalue;
	}
	
	
	

	
	                                                                            
	/**
	 * Returns all the SQLViews  referenced in a VersionedComponentsSet.
	 * 
	 * @param versionedComponentsSet
	 * @return
	 */
	public static EList<SQLView> getViews( VersionedComponentsSet versionedComponentsSet)
	{
	
		
		EList<SQLView> returnvalue = new BasicEList<SQLView>();
		 EList<VersionedFunctionalModuleLogic> functionalModuleLogics = versionedComponentsSet.getDatasetTransformationModules();
		Iterator<VersionedFunctionalModuleLogic> functionalModuleLogicsIter = functionalModuleLogics.iterator();
		while (functionalModuleLogicsIter.hasNext())
		{
			VersionedFunctionalModuleLogic module = functionalModuleLogicsIter.next();
			
			TreeIterator<EObject> moduleContents = module.eAllContents();
			while(moduleContents.hasNext())
			{
				EObject object = moduleContents.next();
				if(object instanceof SQLView)
					returnvalue.add((SQLView) object);
				
			}
		}
		return returnvalue;
		
	}
	
	/**
	 * Returns all the BaseCubes from a  VersionedCubeSchemaModule
	 * @param module
	 * @return
	 */
	public static EList<FreeBirdToolsCube> getCubes ( VersionedCubeSchemaModule module)
	{
	
		
		EList<FreeBirdToolsCube> returnvalue = new BasicEList<FreeBirdToolsCube>();
		Iterator<CubeSchema> schemaIter = module.getSchemas().iterator();
		while (schemaIter.hasNext())
		{
			CubeSchema std = schemaIter.next();
			returnvalue.add(std.getBaseCube());
		}
		return returnvalue;
		
	}
	
	/**
	 * Gets the list of DerivedCubes referenced in a VersionedSQLViewsModule.
	 * 
	 * @param module
	 * @return
	 */
	public static EList<FreeBirdToolsCube> getCubes ( VersionedSQLViewsModule module)
	{		
		EList<FreeBirdToolsCube> returnvalue = new BasicEList<FreeBirdToolsCube>();
		Iterator<SQLView> viewIter = module.getSqlViews().iterator();
		while (viewIter.hasNext())
		{
			SQLView view = viewIter.next();
			returnvalue.add(view.getCube());
		}
		return returnvalue;
		
	}
	
	
}

