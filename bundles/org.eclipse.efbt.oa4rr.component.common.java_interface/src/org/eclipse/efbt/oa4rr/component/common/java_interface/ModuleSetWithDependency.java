package org.eclipse.efbt.oa4rr.component.common.java_interface;

import java.util.List;

import module_management.Module;

public interface ModuleSetWithDependency<module extends Module, dependentModule extends Module> {
	
	public List<module>  getModules();
	public module  getModuleForVerion(String version);
	public void createTrialVersion(module oldVersion, dependentModule dependantVersion, String version );
	public void setTrialModuleAsFinal(String version);
	public void removeInvalidItemsFromTrialModule(String version);
	

}
