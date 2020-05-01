package org.eclipse.efbt.oa4rtt.component.executed_tests.java_interface;

import java.util.List;



import execution_environments.TestExecutionEnvironmentModule;
import test.ExecutedTestModule;
import test.TestModule;


public interface ExecutedTests {
	
	public List<ExecutedTestModule>  getModules();
	public ExecutedTestModule  getModuleForTestModuleAndExecutionEnvironment(TestExecutionEnvironmentModule testEnvironment,
			TestModule testModule);
	public void createNewExecutedTestsModule(TestExecutionEnvironmentModule testEnvironment,
			TestModule testModule, String version);

}
