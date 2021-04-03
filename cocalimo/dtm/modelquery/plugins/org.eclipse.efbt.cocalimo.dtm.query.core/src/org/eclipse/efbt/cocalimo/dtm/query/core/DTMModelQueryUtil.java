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
package org.eclipse.efbt.cocalimo.dtm.query.core;

import java.util.Iterator;

import org.eclipse.efbt.cocalimo.core.model.module_management.Module;
import org.eclipse.efbt.cocalimo.core.model.module_management.ModuleDependency;
import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE;
import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE;
import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE_STRUCTURE_ITEM;
import org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.CubeModule;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cube_schema.CubeSchema;
import org.eclipse.emf.common.util.BasicEList;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.ecore.EObject;

public class dtmModelQueryUtil {
	
	public static EList<CUBE_STRUCTURE_ITEM> getCubeStructureItemsFromDefaultBIRDModel(CubeSchema schema)
	  {
		  CUBE cube = schema.getCube();
		 
		    
		    CUBE_STRUCTURE cubeStructure = cube.getCube_structure_id();
		    EList<ModuleDependency> dependantModules = getDependantModules(cubeStructure);
		     
		    EList<CUBE_STRUCTURE_ITEM> returnlist = new BasicEList<CUBE_STRUCTURE_ITEM>();
		    for (ModuleDependency moduleDependency : dependantModules) {
		    	Module module = moduleDependency.getTheModule();
		    	if (module instanceof CubeModule)
		    	{
		    		CubeModule cubeModule = (CubeModule) module;
					EList<CUBE_STRUCTURE_ITEM> items = cubeModule.getCubeStructureItems();
					for (Iterator iterator2 = items.iterator(); iterator2.hasNext();) {
						CUBE_STRUCTURE_ITEM cube_STRUCTURE_ITEM = (CUBE_STRUCTURE_ITEM) iterator2.next();
						if(cube_STRUCTURE_ITEM.getCube_structure_id().equals(cubeStructure))
						{
							returnlist.add(cube_STRUCTURE_ITEM);
						}
						
					}
		    	}
		    }
		    
		    return returnlist;
	  }

	public static EList<ModuleDependency> getDependantModules( EObject o) {
	    return  ((Module) o.eContainer()).getDependencies().getTheModules();

	  }
	 /**
		 * For a FunctionalityModule, get the SQLViewsModule associated according to
		 * the DefaultNavigationContext.
		 * 
		 * @param functionalModule
		 * @return
		 */
/*		public static VersionedSQLViewsModule getDefaultSQLViewsModuleForTransformation( 
				LeafFunctionalityModule functionalModule) {

			VersionedSQLViewsModule returnModule = null;
			EList<FunctionalityModuleTag> returnlist = new BasicEList<FunctionalityModuleTag>();
			NavigationContext nc = dtmUtil.getDefaultNavigationContext(functionalModule);
			/**
			 * VersionedComponentsSet context = nc.getTransformationContext();
			 * 
			 * EList<VersionedCubeSchemaModule> sm = context.getCubeSchemaModules();
			 * EList<ReportCellViewModule> rm = context.getReportCellViewModules();
			 * EList<VersionedFunctionalModuleLogic> tm =
			 * context.getDatasetTransformationModules();
			 * 
			 * for (Iterator iterator = tm.iterator(); iterator.hasNext();) {
			 * VersionedFunctionalModuleLogic dataSetTransformationModule =
			 * (VersionedFunctionalModuleLogic) iterator .next(); if
			 * (dataSetTransformationModule.getFunctionalityModule().equals(functionalModule))
			 * { returnModule = (VersionedSQLViewsModule) dataSetTransformationModule; }
			 * 
			 * }
			 **/
	/*			DomainMappingModule domainMapping = nc.getDomainModelMapping();
			EList<LeafModuleToVersionedSQLViewsModule> csm = domainMapping.getLeafModuleToVersionedSQLViewsModules();

			for (LeafModuleToVersionedSQLViewsModule leafModuleToVersionedSQLViewsModule : csm) {
				if (leafModuleToVersionedSQLViewsModule.getScheme().equals(functionalModule)) {
					returnModule = leafModuleToVersionedSQLViewsModule.getModule();
				}
			}

			return returnModule;
		}*/
		
}
