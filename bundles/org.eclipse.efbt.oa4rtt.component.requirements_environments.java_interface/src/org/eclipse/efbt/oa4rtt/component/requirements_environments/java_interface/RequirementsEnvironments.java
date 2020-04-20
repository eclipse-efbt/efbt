package org.eclipse.efbt.oa4rtt.component.requirements_environments.java_interface;

import java.util.List;

import functional_module.FunctionalModule;

import requirements_environments.TestRequirementsEnvironmentModule;
import report_structures.RegReportModule;

public interface RequirementsEnvironments {
	
	public void createNewRequirementsEnvironmentModule(FunctionalModule functionalModule,
			RegReportModule rrModule,
			String version);
	
	public TestRequirementsEnvironmentModule getTestRequirementsEnvironment(String version);
	public List<TestRequirementsEnvironmentModule> getTestRequirementsEnvironmentModules();

}
