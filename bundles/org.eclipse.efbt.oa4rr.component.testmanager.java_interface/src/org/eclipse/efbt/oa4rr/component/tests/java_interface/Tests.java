package org.eclipse.efbt.oa4rr.component.tests.java_interface;


import test.TestModule;
import test_definition.TestDefinitionModule;
import execution_environments.TestExecutionEnvironment;

public interface Tests {
	
	public TestModule getTestModule(String version);

	public TestModule createTrialVersion(TestModule oldVersion,
			TestDefinitionModule testDefModule,
			TestExecutionEnvironment executionEnvironment,
			String version);

	public void setTestModuleAsFinal(String version);

}
