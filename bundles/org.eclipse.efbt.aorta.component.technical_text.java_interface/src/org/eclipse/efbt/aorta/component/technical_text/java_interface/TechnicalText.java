package org.eclipse.efbt.aorta.component.technical_text.java_interface;

import java.util.List;

import requirements_text.RequirementsModule;

public interface TechnicalText {
	
	public List<RequirementsModule>  getModules();
	public RequirementsModule  getModuleForVerion(String version);

}
