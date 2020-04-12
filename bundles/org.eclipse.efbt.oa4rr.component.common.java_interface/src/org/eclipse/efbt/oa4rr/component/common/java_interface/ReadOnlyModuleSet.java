package org.eclipse.efbt.oa4rr.component.common.java_interface;

import java.util.List;

import module_management.Module;

public interface ReadOnlyModuleSet<module extends Module> {
	
	public List<module>  getModules();
	public module  getModuleForVerion(String version);

}
