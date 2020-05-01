package org.eclipse.efbt.aorta.component.deep_structures.java_interface;

import java.util.List;

import deep_structures.DeepStructureDefinitionProxyModule;



public interface DeepStructures {
	
	public List<DeepStructureDefinitionProxyModule>  getModules();
	public DeepStructureDefinitionProxyModule  getModuleForVerion(String version);

}
