package org.eclipse.efbt.oa4rtt.component.e2e_technical_tests.java_interface;

import java.util.List;

import requirements_based_test_definition.E2ERequirementsBasedTestDefinitionModule;
import test.TestModule;

public interface E2ETechnicalTests {

	public List<TestModule>  getModules();
	public TestModule  getModuleForVerion(String version);
	public void createTrialVersion(TestModule oldVersion, E2ERequirementsBasedTestDefinitionModule dependantVersion, String version );
	public void setTrialModuleAsFinal(String version);
	public void removeInvalidItemsFromTrialModule(String version);
	
}
