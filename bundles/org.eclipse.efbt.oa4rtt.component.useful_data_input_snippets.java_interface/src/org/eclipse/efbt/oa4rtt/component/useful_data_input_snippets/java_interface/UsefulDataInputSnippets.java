package org.eclipse.efbt.oa4rtt.component.useful_data_input_snippets.java_interface;

import test_input_data.UsefulInputDataSnippetsModule;

import java.util.List;


import column_structures.ColumnStructureModule;

public interface UsefulDataInputSnippets {
	
	public List<UsefulInputDataSnippetsModule>  getModules();
	public UsefulInputDataSnippetsModule  getModuleForVerion(String version);
	public void createTrialVersion(UsefulInputDataSnippetsModule oldVersion, ColumnStructureModule dependantVersion, String version );
	public void setTrialModuleAsFinal(String version);
	public void removeInvalidItemsFromTrialModule(String version);

}
