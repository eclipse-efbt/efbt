package org.eclipse.efbt.aorta.component.output_data_structures.java_interface;

import java.util.List;
import output_data_structures.OutputDataStructureModule;


public interface OutputDataStructures {
	
	public List<OutputDataStructureModule>  getModules();
	public OutputDataStructureModule  getModuleForVerion(String version);

}
