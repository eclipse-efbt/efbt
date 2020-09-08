package org.eclipse.efbt.cocamo.trl.query.core;



public class TRLModelQueryUtil {

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
