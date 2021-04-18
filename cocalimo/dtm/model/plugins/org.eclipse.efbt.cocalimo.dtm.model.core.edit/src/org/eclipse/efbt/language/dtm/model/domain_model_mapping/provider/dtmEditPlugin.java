/**
 */
package org.eclipse.efbt.language.dtm.model.domain_model_mapping.provider;

import org.eclipse.efbt.cocalimo.core.model.functionality_module.provider.Functionality_moduleEditPlugin;

import org.eclipse.efbt.cocalimo.core.model.module_management.provider.Module_managementEditPlugin;

import org.eclipse.efbt.cocalimo.core.model.requirements_text.provider.Requirements_textEditPlugin;

import org.eclipse.efbt.cocalimo.core.model.test_definition.provider.TestingEditPlugin;

import org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.provider.Smcubes_input_dataEditPlugin;

import org.eclipse.efbt.cocalimo.smcubes.model.cocamo.provider.CocamoEditPlugin;

import org.eclipse.efbt.cocalimo.smcubes.model.core.provider.SmcubesEditPlugin;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.advanced_variable_lineagefunctions.provider.LineagefunctionsEditPlugin;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cube_schema.provider.LineagecubesEditPlugin;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.provider.FunctionsEditPlugin;

import org.eclipse.emf.common.EMFPlugin;

import org.eclipse.emf.common.util.ResourceLocator;

/**
 * This is the central singleton for the dtm edit plugin.
 * <!-- begin-user-doc -->
 * <!-- end-user-doc -->
 * @generated
 */
public final class dtmEditPlugin extends EMFPlugin {
	/**
	 * Keep track of the singleton.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static final dtmEditPlugin INSTANCE = new dtmEditPlugin();

	/**
	 * Keep track of the singleton.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private static Implementation plugin;

	/**
	 * Create the instance.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public dtmEditPlugin() {
		super
		  (new ResourceLocator [] {
		     CocamoEditPlugin.INSTANCE,
		     Functionality_moduleEditPlugin.INSTANCE,
		     FunctionsEditPlugin.INSTANCE,
		     LineagecubesEditPlugin.INSTANCE,
		     LineagefunctionsEditPlugin.INSTANCE,
		     Module_managementEditPlugin.INSTANCE,
		     Requirements_textEditPlugin.INSTANCE,
		     Smcubes_input_dataEditPlugin.INSTANCE,
		     SmcubesEditPlugin.INSTANCE,
		     TestingEditPlugin.INSTANCE,
		   });
	}

	/**
	 * Returns the singleton instance of the Eclipse plugin.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the singleton instance.
	 * @generated
	 */
	@Override
	public ResourceLocator getPluginResourceLocator() {
		return plugin;
	}

	/**
	 * Returns the singleton instance of the Eclipse plugin.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the singleton instance.
	 * @generated
	 */
	public static Implementation getPlugin() {
		return plugin;
	}

	/**
	 * The actual implementation of the Eclipse <b>Plugin</b>.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static class Implementation extends EclipsePlugin {
		/**
		 * Creates an instance.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		public Implementation() {
			super();

			// Remember the static instance.
			//
			plugin = this;
		}
	}

}
