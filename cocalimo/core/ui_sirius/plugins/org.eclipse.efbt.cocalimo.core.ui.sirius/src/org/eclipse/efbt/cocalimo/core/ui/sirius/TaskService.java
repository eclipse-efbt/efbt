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
package org.eclipse.efbt.cocalimo.core.ui.sirius;

import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.Activity;
import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.ScriptTask;
import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.ServiceTask;
import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.Task;
import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.UserTask;
import org.eclipse.efbt.cocalimo.core.model.logical_transformations.LogicalTransformationModule;
import org.eclipse.efbt.cocalimo.core.model.logical_transformations.Scenario;
import org.eclipse.efbt.cocalimo.core.model.logical_transformations.ScenarioTag;
import org.eclipse.efbt.cocalimo.core.model.logical_transformations.ActivityTag;
import org.eclipse.efbt.cocalimo.core.model.logical_transformations.Test;
import org.eclipse.efbt.cocalimo.core.model.logical_transformations.TestModule;
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
	
public EList<Test> getTests(Scenario self) {

		EList<Test> tests = new BasicEList<Test>();
		EObject root = EcoreUtil.getRootContainer(self);
		LogicalTransformationModule theRoot = (LogicalTransformationModule) root;
		EList<TestModule> testModules = theRoot.getTestModules();
		for (TestModule testModule : testModules)
		{
		
			EList<Test> theTests = testModule.getTests();
			for (Test test : theTests) {
				
				if (test.getScenarios().equals(self))
				{
					tests.add(test);
				}
			}
		
		
		}
	
		return tests;
		
	}


}
