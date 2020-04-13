package org.eclipse.efbt.oa4rr.component.test_definitions.java_interface;

import test_definition.TestDefinitionModule;

import java.util.List;

import requirements_environments.TestRequirementsEnvironmentModule;

public interface TestDefinitions
		{
	
	public List<TestDefinitionModule>  getModules();
	public TestDefinitionModule  getModuleForVerion(String version);
	public void createTrialVersion(TestDefinitionModule oldVersion, TestRequirementsEnvironmentModule dependantVersion, String version );
	public void setTrialModuleAsFinal(String version);
	public void removeInvalidItemsFromTrialModule(String version);

}
