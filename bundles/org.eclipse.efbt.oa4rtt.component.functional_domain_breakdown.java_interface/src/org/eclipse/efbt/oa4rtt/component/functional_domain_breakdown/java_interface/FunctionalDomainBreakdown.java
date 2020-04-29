package org.eclipse.efbt.oa4rtt.component.functional_domain_breakdown.java_interface;

import java.util.List;

import functionality_module.FunctionalityModuleModule;
import output_data_structures.OutputDataStructureModule;
import requirements_text.RequirementsModule;

public interface FunctionalDomainBreakdown {

	public List<FunctionalityModuleModule>  getModules();
	public FunctionalityModuleModule  getModuleForVerion(String version);
	public void createTrialVersion(FunctionalityModuleModule oldVersion,
			RequirementsModule dependantRequirementsVersion,
			OutputDataStructureModule dependantOutputStructuresVersion,
			String version );
	public void setTrialModuleAsFinal(String version);
	public void removeInvalidItemsFromTrialModule(String version);
	
}
 