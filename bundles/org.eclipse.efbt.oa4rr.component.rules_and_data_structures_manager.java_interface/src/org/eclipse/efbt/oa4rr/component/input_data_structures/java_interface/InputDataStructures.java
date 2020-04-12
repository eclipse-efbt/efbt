package org.eclipse.efbt.oa4rr.component.input_data_structures.java_interface;

import org.eclipse.efbt.oa4rr.component.common.java_interface.ReadOnlyModuleSet;

import column_structures.ColumnStructureModule;


public interface InputDataStructures extends ReadOnlyModuleSet<ColumnStructureModule> {
	
	public ColumnStructureModule getColumnStructureModule(String version);

}
