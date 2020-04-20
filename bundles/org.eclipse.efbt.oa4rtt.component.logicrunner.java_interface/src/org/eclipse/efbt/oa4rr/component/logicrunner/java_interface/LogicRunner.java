package org.eclipse.efbt.oa4rtt.component.logicrunner.java_interface;

import java.util.List;


import executable_logic.ExecutableLogicModule;
import executable_logic.ExecutableLogicProxy;
import test.TestModule;

public interface LogicRunner {
	
	public void runTests(TestModule testModule, ExecutableLogicProxy executableLogic);
	public List<ExecutableLogicModule>  getModules();
	public ExecutableLogicModule  getModuleForVerion(String version);

}
