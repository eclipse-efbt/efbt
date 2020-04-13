package org.eclipse.efbt.oa4rr.component.tests.java_interface;

import test.TestModule;
import test_definition.TestDefinitionModule;

import java.util.List;


public interface Tests   {
	
	public List<TestModule>  getModules();
	public TestModule  getModuleForVerion(String version);
	public void createTrialVersion(TestModule oldVersion, TestDefinitionModule dependantVersion, String version );
	public void setTrialModuleAsFinal(String version);
	public void removeInvalidItemsFromTrialModule(String version);

}
