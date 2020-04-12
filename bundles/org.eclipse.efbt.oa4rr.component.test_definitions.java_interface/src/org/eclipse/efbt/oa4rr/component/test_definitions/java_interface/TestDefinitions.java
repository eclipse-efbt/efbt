package org.eclipse.efbt.oa4rr.component.test_definitions.java_interface;


import test_definition.TestDefinitionModule;
import requirements_environments.TestRequirementsEnvironment;

public interface TestDefinitions {
		

	public TestDefinitionModule createTrialVersion(TestDefinitionModule oldVersion,
			TestRequirementsEnvironment requirementsEnvironment, String version);

	public void setTestDefinitionModuleAsFinal(String version);

	public TestDefinitionModule getTestDefinitionModule(String version);

}
