/*******************************************************************************
 * Copyright (c) 2020 Bird Software Solutions Ltd
 * This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License 2.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 *
 * Contributors:
 *    Neil Mackenzie - initial API and implementation
 *******************************************************************************/
package org.eclipse.efbt.openregspecs.testing.ui.sirius.core;

import org.eclipse.efbt.openregspecs.model.open_reg_specs.Activity;
import org.eclipse.efbt.openregspecs.model.open_reg_specs.ScriptTask;
import org.eclipse.efbt.openregspecs.model.open_reg_specs.ServiceTask;
import org.eclipse.efbt.openregspecs.model.open_reg_specs.SubProcess;
import org.eclipse.efbt.openregspecs.model.open_reg_specs.Task;
import org.eclipse.efbt.openregspecs.model.open_reg_specs.UserTask;
import org.eclipse.efbt.openregspecs.model.open_reg_specs.ActivityTag;
import org.eclipse.efbt.openregspecs.model.open_reg_specs.E2ETestScope;
import org.eclipse.efbt.openregspecs.model.open_reg_specs.LogicalTransformationModule;
import org.eclipse.efbt.openregspecs.model.open_reg_specs.Scenario;
import org.eclipse.efbt.openregspecs.model.open_reg_specs.ScenarioTag;
import org.eclipse.efbt.openregspecs.model.open_reg_specs.Test;
import org.eclipse.efbt.openregspecs.model.open_reg_specs.TestModule;
import org.eclipse.efbt.openregspecs.model.open_reg_specs.TestScope;
import org.eclipse.efbt.openregspecs.model.open_reg_specs.UnitTestScope;
import org.eclipse.efbt.openregspecs.model.open_reg_specs.Attribute;
import org.eclipse.efbt.openregspecs.model.open_reg_specs.BasicEntity;
import org.eclipse.emf.common.util.BasicEList;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.TreeIterator;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.util.EcoreUtil;

/**
 * Test and conversion services to operate on Task objects.
 * 
 * @author Neil Mackenzie
 *
 */
public class TaskService {

	public String getLabel(Task task, boolean borderedNodes) {
		String result = " \n \n" + task.getName();
		return result;
	}

	public String trimLabel(EObject eo, String newLabel) {
		if (eo instanceof Task) {
			return newLabel.trim();
		}
		return newLabel;
	}
	
	public boolean isLikeScriptTask(final EObject eObject) {
		boolean isLikeScriptTask = eObject instanceof ScriptTask;
		return isLikeScriptTask;
	}
	
	public boolean isLikeServiceTask(final EObject eObject) {
		return eObject instanceof ServiceTask;
	}
	
	public boolean isLikeUserTask(final EObject eObject) {
		return eObject instanceof UserTask;
	}
	
	public ScenarioTag getScenarioTag(Scenario self) {

		
		EObject root = EcoreUtil.getRootContainer(self);
		TreeIterator<EObject> contents = root.eAllContents();
		while (contents.hasNext() )
		{
			EObject o = contents.next();
			if (o instanceof ScenarioTag)
			{
				if (((ScenarioTag) o).getScenario().equals(self))
					return (ScenarioTag) o;
			}
		}
		return null;
		
	}
	
	public ActivityTag getActivityTag(Activity self) {

		
		EObject root = EcoreUtil.getRootContainer(self);
		TreeIterator<EObject> contents = root.eAllContents();
		while (contents.hasNext() )
		{
			EObject o = contents.next();
			if (o instanceof ActivityTag)
			{
				if (((ActivityTag) o).getActivity().equals(self))
					return (ActivityTag) o;
			}
		}
		return null;
		
	}
	public EList<Task> getDependantTasks(Task self) 
	{
		EList<Task> returnTasks = new BasicEList<Task>();
		
		if (self instanceof ServiceTask)
		{
			ServiceTask serviceTask = (ServiceTask) self;
			Attribute attr = serviceTask.getEnrichedAttribute();
			EList<Scenario> scenarios = serviceTask.getScenarios();
			for (Scenario scenario : scenarios) {
				scenario.getRequiredAttributes();
				//get the other service tasks and user tasks that contain theis atribute, then add them to the list of they dont exist.
			}
		}
		return returnTasks;
	}
	
	public EList<Attribute> getILAttributes(SubProcess self)
	{
		EList<Attribute> returnAttributes = new BasicEList<Attribute>();
		TreeIterator<EObject> contents = self.eAllContents();
		while(contents.hasNext())
		{
			EObject item = contents.next();
			if(item instanceof UserTask )
			{
				UserTask userTask= (UserTask) item;
				BasicEntity eclass = userTask.getEntity();
				EList<Attribute> attributes = eclass.getAttributes();
				returnAttributes.addAll(attributes);
			}
		}
		
		return returnAttributes;
		
	}
	
	public EList<Attribute> getEILAttributes(SubProcess self)
	{
		EList<Attribute> returnAttributes = new BasicEList<Attribute>();
		TreeIterator<EObject> contents = self.eAllContents();
		while(contents.hasNext())
		{
			EObject item = contents.next();
			if(item instanceof ServiceTask )
			{
				ServiceTask serviceTask= (ServiceTask) item;
				
				Attribute attribute = serviceTask.getEnrichedAttribute();
				returnAttributes.add(attribute);
			}
		}
		
		return returnAttributes;
		
	}
	
	
		
	public EList<Test> getTests(Scenario self) {

		EList<Test> tests = new BasicEList<Test>();
		EObject root = EcoreUtil.getRootContainer(self);
		LogicalTransformationModule theRoot = (LogicalTransformationModule) root;
		EList<TestModule> testModules = theRoot.getTestModules();
		for (TestModule testModule : testModules)
		{
			
			EList<Test> theTests = testModule.getTests();
			for (Test test : theTests) {
				TestScope testScope = test.getScope();
		
				if(testScope instanceof E2ETestScope)
				{
					EList<Scenario> testScenarios = ((E2ETestScope) testScope).getScenarios();
				
					if (testScenarios.contains(self))
					{
						tests.add(test);
					}
				}
				if(testScope instanceof UnitTestScope)
				{
					Scenario testScenario = ((UnitTestScope) testScope).getScenarios();
				
					if (testScenario.equals(self))
					{
						tests.add(test);
					}
				}
				
			}
		
		
		}
	
		return tests;
		
	}


}
