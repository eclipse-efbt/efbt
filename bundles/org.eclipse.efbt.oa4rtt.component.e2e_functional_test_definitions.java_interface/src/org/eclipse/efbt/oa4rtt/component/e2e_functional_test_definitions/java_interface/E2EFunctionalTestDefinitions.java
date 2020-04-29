package org.eclipse.efbt.oa4rtt.component.e2e_functional_test_definitions.java_interface;

import java.util.List;

import functionality_module.FunctionalityModuleModule;
import requirements_based_test_definition.E2ERequirementsBasedTestDefinitionModule;


public interface E2EFunctionalTestDefinitions {

	
	public List<E2ERequirementsBasedTestDefinitionModule>  getModules();
	public E2ERequirementsBasedTestDefinitionModule  getModuleForVerion(String version);
	public void createTrialVersion(E2ERequirementsBasedTestDefinitionModule oldVersion, FunctionalityModuleModule dependantVersion, String version );
	public void setTrialModuleAsFinal(String version);
	public void removeInvalidItemsFromTrialModule(String version);
}
