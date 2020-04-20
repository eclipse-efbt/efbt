package org.eclipse.efbt.oa4rtt.component.regulatory_text.java_interface;

import java.util.List;


import requirements_text.RequirementsModule;

public interface RegulatoryText {
	
	public List<RequirementsModule>  getModules();
	public RequirementsModule  getModuleForVerion(String version);

}
