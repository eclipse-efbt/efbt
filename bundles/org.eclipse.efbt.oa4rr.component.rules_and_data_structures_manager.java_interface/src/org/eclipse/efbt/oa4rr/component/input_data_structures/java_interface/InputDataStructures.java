package org.eclipse.efbt.oa4rr.component.input_data_structures.java_interface;

import java.util.List;


import column_structures.ColumnStructureModule;


public interface InputDataStructures  {
	
	public List<ColumnStructureModule>  getModules();
	public ColumnStructureModule  getModuleForVerion(String version);


}
