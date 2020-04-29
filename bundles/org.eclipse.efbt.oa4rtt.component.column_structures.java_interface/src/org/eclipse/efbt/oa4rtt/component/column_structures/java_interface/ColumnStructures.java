package org.eclipse.efbt.oa4rtt.component.column_structures.java_interface;

import java.util.List;

import column_structures.ColumnStructureModule;

public interface ColumnStructures {

	public List<ColumnStructureModule>  getModules();
	public ColumnStructureModule  getModuleForVerion(String version);
}
