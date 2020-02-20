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
package org.eclipse.efbt.model.util;



import java.util.Iterator;

import org.eclipse.emf.common.util.BasicEList;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.TreeIterator;
import org.eclipse.emf.ecore.EObject;


import cube_schema.CubeSchema;
import transformation.VersionedCubeSchemaModule;
import cubes.DerivedCube;
import cubes.FreeBirdToolsCube;
import cubes.BaseCube;

import cubes.TargetCube;
import transformation.VersionedComponentsSet;
import transformation.TransformationSchemeModule;

import trl_report_cell_views.ReportCellViewModule;
import trl_sql_views.SQLView;
import trl_sql_views.VersionedSQLViewsModule;


import transformation.VersionedTransformationSchemeLogic;
import transformation.TransformationScheme;

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
			return "INPUT LAYER CUBE: " + cube.getCube_name().replace('_', ' ');
		else if  (cube instanceof TargetCube)
			return "REPORT CELL: " + cube.getCube_name().replace('_', ' ');
		else 
			return "TRANSFORATION: " + cube.getCube_name().replace('_', ' ');
	}
	
	/**
	 * Privides a UI display name for a CubeSchema.
	 * 
	 * @param schema
	 * @return
	 */
	public static String getSchemaDisplayName(CubeSchema schema)
	{
		return "SCHEMA: " + schema.getBaseCube().getCube_name().replace('_', ' ');
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
		
		
		
		
		return "LOGIC: " + vn; 
	}

	/**
	 * Gets the top level container for a TransformationScheme.
	 * @param ts
	 * @return
	 */
	private static TransformationSchemeModule getRootContainer(TransformationScheme ts) {
	
		EObject container = ts.eContainer();
		if(container.eContainer() != null)
		{
			container = container.eContainer();
			if(container.eContainer() != null)
			{
				container = container.eContainer();
				if(container.eContainer() != null)
				{
					container = container.eContainer();
					if(container.eContainer() != null)
					{
						container = container.eContainer();
						if(container.eContainer() != null)
						{
							container = container.eContainer();
							if(container.eContainer() != null)
							{
								container = container.eContainer();
							}
						}
					}
				}
			}
		}
		
		return (TransformationSchemeModule)container;
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
	
	
	
	/**
	 * Returns all the BaseCubes referenced in a VersionedComponentsSet.
	 * 
	 * @param versionedComponentsSet
	 * @return
	 */
	public static EList<FreeBirdToolsCube> getCubes ( VersionedComponentsSet versionedComponentsSet)
	{
	
		EList<FreeBirdToolsCube> returnCubelist = new BasicEList<FreeBirdToolsCube>();
		 EList<VersionedTransformationSchemeLogic> transformationSchemeLogic = versionedComponentsSet.getDatasetTransformationModules();
		 EList<ReportCellViewModule> reportcellmodules = versionedComponentsSet.getReportCellViewModules();
		EList<VersionedCubeSchemaModule> cubeSchemaModules = versionedComponentsSet.getCubeSchemaModules();
		Iterator<VersionedTransformationSchemeLogic> transformationSchemeLogicIter = transformationSchemeLogic.iterator();
		Iterator<VersionedCubeSchemaModule> cubeSchemaModulesIter = cubeSchemaModules.iterator();
		Iterator<ReportCellViewModule> reportcellmodulesIter = reportcellmodules.iterator();
		while (transformationSchemeLogicIter.hasNext())
		{
			VersionedTransformationSchemeLogic module = transformationSchemeLogicIter.next();
			
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
		 EList<VersionedTransformationSchemeLogic> transformationSchemeLogics = versionedComponentsSet.getDatasetTransformationModules();
		Iterator<VersionedTransformationSchemeLogic> transformationSchemeLogicsIter = transformationSchemeLogics.iterator();
		while (transformationSchemeLogicsIter.hasNext())
		{
			VersionedTransformationSchemeLogic module = transformationSchemeLogicsIter.next();
			
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

