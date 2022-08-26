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
import org.eclipse.efbt.openregspecs.model.open_reg_specs.ActivityTag;
import org.eclipse.efbt.openregspecs.model.open_reg_specs.Scenario;
import org.eclipse.efbt.openregspecs.model.open_reg_specs.ScenarioTag;
import org.eclipse.efbt.openregspecs.model.open_reg_specs.ScriptTask;
import org.eclipse.efbt.openregspecs.model.open_reg_specs.ServiceTask;
import org.eclipse.efbt.openregspecs.model.open_reg_specs.Task;
import org.eclipse.efbt.openregspecs.model.open_reg_specs.UserTask;
import org.eclipse.efbt.openregspecs.model.open_reg_specs.XAttribute;
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
			XAttribute attr = serviceTask.getEnrichedAttribute();
			EList<Scenario> scenarios = serviceTask.getScenarios();
			for (Scenario scenario : scenarios) {
				scenario.getRequiredAttributes();
				//get the other service tasks and user tasks that contain theis atribute, then add them to the list of they dont exist.
			}
		}
		return returnTasks;
	}
	
	


}
