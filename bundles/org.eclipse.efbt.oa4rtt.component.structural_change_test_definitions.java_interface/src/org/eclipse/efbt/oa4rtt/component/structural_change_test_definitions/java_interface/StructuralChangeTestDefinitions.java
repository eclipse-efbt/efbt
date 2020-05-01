package org.eclipse.efbt.oa4rtt.component.structural_change_test_definitions.java_interface;

import java.util.List;



import structural_change_test_definitions.StructuralChangeMapping;
import structural_change_test_definitions.StructuralChangeTestDefinitionModule;

public interface StructuralChangeTestDefinitions {

	public List<StructuralChangeTestDefinitionModule>  getModules();
	public StructuralChangeTestDefinitionModule  getModuleForVerion(String version);
	public void createTrialVersion(StructuralChangeTestDefinitionModule oldVersion,
			StructuralChangeMapping dependantchangemapping,			
			String version );
	public void setTrialModuleAsFinal(String version);
	public void removeInvalidItemsFromTrialModule(String version);
}
