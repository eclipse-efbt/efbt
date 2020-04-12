package org.eclipse.efbt.oa4rr.component.logicrunner.java_interface;

import org.eclipse.efbt.oa4rr.component.common.java_interface.ReadOnlyModuleSet;
import executable_logic.ExecutableLogicModule;
import executable_logic.ExecutableLogicProxy;
import test.TestModule;

public interface LogicRunner extends ReadOnlyModuleSet<ExecutableLogicModule>{
	
	public void runTests(TestModule testModule, ExecutableLogicProxy executableLogic);

}
