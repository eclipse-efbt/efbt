package org.eclipse.efbt.oa4rtt.component.technical_specification_breakdown.java_interface;

import java.util.List;

import functionality_module.FunctionalityModuleModule;
import output_data_structures.OutputDataStructureModule;
import requirements_text.RequirementsModule;

public interface TechnicalSpecificationBreakdown {

	public List<FunctionalityModuleModule>  getModules();
	public FunctionalityModuleModule  getModuleForVerion(String version);
	public void createTrialVersion(FunctionalityModuleModule oldVersion,
			RequirementsModule dependantRequirementsVersion,
			OutputDataStructureModule dependantOutputStructuresVersion,
			String version );
	public void setTrialModuleAsFinal(String version);
	public void removeInvalidItemsFromTrialModule(String version);
}
