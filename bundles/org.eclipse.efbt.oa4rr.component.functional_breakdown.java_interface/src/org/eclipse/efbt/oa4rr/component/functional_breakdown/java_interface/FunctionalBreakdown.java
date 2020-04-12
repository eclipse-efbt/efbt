package org.eclipse.efbt.oa4rr.component.functional_breakdown.java_interface;

import functional_module.FunctionalModule;
import requirements_text.RequirementsModule;

public interface FunctionalBreakdown {
	
	public FunctionalModule getFunctionalModule(String version);
	
	public FunctionalModule createTrialVersion(FunctionalModule oldVersion,
												RequirementsModule requirementsModule,
												String version);
	
	public void setFunctionalModuleAsFinal(String version);

}
