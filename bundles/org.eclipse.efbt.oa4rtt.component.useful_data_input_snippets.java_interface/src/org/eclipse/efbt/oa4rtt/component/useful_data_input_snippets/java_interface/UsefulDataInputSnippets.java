package org.eclipse.efbt.oa4rtt.component.useful_data_input_snippets.java_interface;

import test_input_data.UsefulInputDataSnippetsModule;

import java.util.List;


import input_data_structures.InputDatastructureModule;

public interface UsefulDataInputSnippets {
	
	public List<UsefulInputDataSnippetsModule>  getModules();
	public UsefulInputDataSnippetsModule  getModuleForVerion(String version);
	public void createTrialVersion(UsefulInputDataSnippetsModule oldVersion, InputDatastructureModule dependantVersion, String version );
	public void setTrialModuleAsFinal(String version);
	public void removeInvalidItemsFromTrialModule(String version);

}
