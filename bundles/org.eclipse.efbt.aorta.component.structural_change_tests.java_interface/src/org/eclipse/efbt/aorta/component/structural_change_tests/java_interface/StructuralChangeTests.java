package org.eclipse.efbt.aorta.component.structural_change_tests.java_interface;

import java.util.List;

import structural_change_test_definitions.StructuralChangeTestDefinitionModule;
import test.TestModule;

public interface StructuralChangeTests {
	public List<TestModule>  getModules();
	public TestModule  getModuleForVerion(String version);
	public void createTrialVersion(TestModule oldVersion, StructuralChangeTestDefinitionModule dependantVersion, String version );
	public void setTrialModuleAsFinal(String version);
	public void removeInvalidItemsFromTrialModule(String version);

}
