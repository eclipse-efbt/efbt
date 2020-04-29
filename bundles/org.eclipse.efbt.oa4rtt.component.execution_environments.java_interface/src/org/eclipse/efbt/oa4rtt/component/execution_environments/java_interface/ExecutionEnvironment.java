package org.eclipse.efbt.oa4rtt.component.execution_environments.java_interface;

import java.util.List;




import execution_environments.TestExecutionEnvironmentModule;


public interface ExecutionEnvironment {
	
	/**public void createNewExecutionEnvironmentModule(InputDatastructureModule idsModule,
			OutputDataStructureModule odsModule,
			ExecutableLogicModule elModule,
			String version);*/
	
	
	public TestExecutionEnvironmentModule getTestExecutionEnvironment(String version);
	
	public List<TestExecutionEnvironmentModule> getTestExecutionEnvironmentModules();
}
