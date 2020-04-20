package org.eclipse.efbt.oa4rtt.component.functional_breakdown.java_interface;

import java.util.List;


import functional_module.FunctionalModuleModule;
import requirements_text.RequirementsModule;

public interface FunctionalBreakdown  {
	
	public List<FunctionalModuleModule>  getModules();
	public FunctionalModuleModule  getModuleForVerion(String version);
	public void createTrialVersion(FunctionalModuleModule oldVersion, RequirementsModule dependantVersion, String version );
	public void setTrialModuleAsFinal(String version);
	public void removeInvalidItemsFromTrialModule(String version);
	
	

}
