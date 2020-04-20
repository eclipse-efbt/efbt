package org.eclipse.efbt.oa4rtt.component.execution_environments.java_interface;

import java.util.List;

import column_structures.ColumnStructureModule;
import report_structures.RegReportModule;
import executable_logic.ExecutableLogicModule;
import execution_environments.TestExecutionEnvironment;

public interface ExecutionEnvironment {
	
	public void createNewExecutionEnvironment(ColumnStructureModule csModule,
			RegReportModule rrModule,
			ExecutableLogicModule elModule,
			String version);
	
	public TestExecutionEnvironment getTestExecutionEnvironment(String version);
	
	public List<TestExecutionEnvironment> getTestExecutionEnvironments();
}
