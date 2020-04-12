package org.eclipse.efbt.oa4rr.component.requirements_environments.java_interface;

import functional_module.FunctionalModule;
import requirements_environments.TestRequirementsEnvironment;
import report_structures.RegReportModule;

public interface RequirementsEnvironments {
	
	public void createNewRequirementsEnvironment(FunctionalModule functionalModule,
			RegReportModule rrModule,
			String version);
	
	public TestRequirementsEnvironment getTestRequirementsEnvironment(String version);

}
