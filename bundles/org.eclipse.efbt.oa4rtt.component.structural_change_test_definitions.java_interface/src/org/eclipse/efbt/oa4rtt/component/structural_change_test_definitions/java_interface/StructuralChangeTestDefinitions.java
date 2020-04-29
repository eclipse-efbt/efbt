package org.eclipse.efbt.oa4rtt.component.structural_change_test_definitions.java_interface;

import java.util.List;


import input_data_structures.InputDatastructureModule;
import output_data_structures.OutputDataStructureModule;
import structural_change_test_definitions.StructuralChangeTestDefinitionModule;

public interface StructuralChangeTestDefinitions {

	public List<StructuralChangeTestDefinitionModule>  getModules();
	public StructuralChangeTestDefinitionModule  getModuleForVerion(String version);
	public void createTrialVersion(StructuralChangeTestDefinitionModule oldVersion,
			InputDatastructureModule dependantInputStructuresVersion,
			OutputDataStructureModule dependantOutputStructuresVersion,
			String version );
	public void setTrialModuleAsFinal(String version);
	public void removeInvalidItemsFromTrialModule(String version);
}
