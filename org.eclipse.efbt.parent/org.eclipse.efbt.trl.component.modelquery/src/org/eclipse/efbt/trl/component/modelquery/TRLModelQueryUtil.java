package org.eclipse.efbt.trl.component.modelquery;

import org.eclipse.efbt.aorta.functionality_module.FunctionalityModuleTag;
import org.eclipse.efbt.aorta.functionality_module.LeafFunctionalityModule;
import org.eclipse.efbt.navigation_context.model.navigation_context.NavigationContext;
import org.eclipse.efbt.trl.component.translator.TRLUtil;
import org.eclipse.efbt.trl.model.trl.domain_model_mapping.DomainMappingModule;
import org.eclipse.efbt.trl.model.trl.domain_model_mapping.LeafModuleToVersionedSQLViewsModule;
import org.eclipse.efbt.trl.model.trl.trl_sql_views.VersionedSQLViewsModule;
import org.eclipse.emf.common.util.BasicEList;
import org.eclipse.emf.common.util.EList;

public class TRLModelQueryUtil {

	 /**
		 * For a FunctionalityModule, get the SQLViewsModule associated according to
		 * the DefaultNavigationContext.
		 * 
		 * @param functionalModule
		 * @return
		 */
		public static VersionedSQLViewsModule getDefaultSQLViewsModuleForTransformation( 
				LeafFunctionalityModule functionalModule) {

			VersionedSQLViewsModule returnModule = null;
			EList<FunctionalityModuleTag> returnlist = new BasicEList<FunctionalityModuleTag>();
			NavigationContext nc = TRLUtil.getDefaultNavigationContext(functionalModule);
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
			DomainMappingModule domainMapping = nc.getDomainModelMapping();
			EList<LeafModuleToVersionedSQLViewsModule> csm = domainMapping.getLeafModuleToVersionedSQLViewsModules();

			for (LeafModuleToVersionedSQLViewsModule leafModuleToVersionedSQLViewsModule : csm) {
				if (leafModuleToVersionedSQLViewsModule.getScheme().equals(functionalModule)) {
					returnModule = leafModuleToVersionedSQLViewsModule.getModule();
				}
			}

			return returnModule;
		}
		
}
