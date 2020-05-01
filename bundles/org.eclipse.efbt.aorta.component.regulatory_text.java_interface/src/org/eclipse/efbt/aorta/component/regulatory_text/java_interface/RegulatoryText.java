package org.eclipse.efbt.aorta.component.regulatory_text.java_interface;

import java.util.List;


import requirements_text.RequirementsModule;

public interface RegulatoryText {
	
	public List<RequirementsModule>  getModules();
	public RequirementsModule  getModuleForVerion(String version);

}
