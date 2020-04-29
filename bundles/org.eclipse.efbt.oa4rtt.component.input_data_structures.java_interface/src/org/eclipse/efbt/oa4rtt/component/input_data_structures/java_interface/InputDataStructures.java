package org.eclipse.efbt.oa4rtt.component.input_data_structures.java_interface;

import java.util.List;


import input_data_structures.InputDatastructureModule;


public interface InputDataStructures  {
	
	public List<InputDatastructureModule>  getModules();
	public InputDatastructureModule  getModuleForVerion(String version);


}
