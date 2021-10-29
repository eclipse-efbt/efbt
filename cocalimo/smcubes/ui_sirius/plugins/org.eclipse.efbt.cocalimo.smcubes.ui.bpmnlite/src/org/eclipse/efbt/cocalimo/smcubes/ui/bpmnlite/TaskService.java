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
package org.eclipse.efbt.cocalimo.smcubes.ui.bpmnlite;

import java.util.Collection;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EStructuralFeature.Setting;
import org.eclipse.emf.ecore.util.ECrossReferenceAdapter;
import org.eclipse.sirius.diagram.DNodeContainer;

import org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.Bpmn_liteFactory;
import org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.Bpmn_litePackage;
import org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.FlowElementsContainer;
import org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.ServiceTask;
import org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.Task;
import org.eclipse.efbt.cocalimo.smcubes.model.bpmn_lite.impl.TaskImpl;

/**
 * Test and conversion services to operate on Task objects.
 * 
 * @author Neil Mackenzie
 *
 */
public class TaskService {

	public String getLabel(Task task, boolean borderedNodes) {
		String result = " \n \n" + task.getName();
		// if (borderedNodes) {
		// result = " \n" + result;
		// }
		return result;
	}

	public String trimLabel(EObject eo, String newLabel) {
		if (eo instanceof Task) {
			return newLabel.trim();
		}
		return newLabel;
	}





	public boolean isLikeTask(final EObject eObject) {
		return eObject instanceof Task && eObject.getClass().isAssignableFrom(TaskImpl.class);
	}

	

	public boolean isLikeServiceTask(final EObject eObject) {
		return eObject instanceof ServiceTask;
	}

	

	public Task convertToTask(final DNodeContainer view) {
		Task eObject = (Task) view.getTarget();
		if (!eObject.getClass().isAssignableFrom(TaskImpl.class)) {
			return (Task) convertToSpecificTask((Task) view.getTarget(), Bpmn_litePackage.eINSTANCE.getTask());
		}
		return eObject;
	}



	public ServiceTask convertToServiceTask(final DNodeContainer view) {
		return (ServiceTask) convertToSpecificTask((Task) view.getTarget(), Bpmn_litePackage.eINSTANCE.getServiceTask());
	}

	

	private Task convertToSpecificTask(Task task, EClass eClass) {
		if (eClass.equals(task.eClass())) {
			return task;
		} else {
			Task cloneTask = clone(task, (Task) Bpmn_liteFactory.eINSTANCE.create(eClass));
			return cloneTask;
		}
	}

	private Task clone(Task task, Task cloneTask) {
		
		cloneTask.getIncoming().addAll(task.getIncoming());
		cloneTask.getOutgoing().addAll(task.getOutgoing());
		cloneTask.setId(task.getId());
		cloneTask.setName(task.getName());


		updateMessageFlowsAndAssociations(task, cloneTask);

		FlowElementsContainer container = (FlowElementsContainer) task.eContainer();
		container.getFlowElements().remove(task);
		container.getFlowElements().add(cloneTask);

		return cloneTask;
	}

	private void updateMessageFlowsAndAssociations(Task task, Task cloneTask) {
		ECrossReferenceAdapter eCrossReferenceAdapter = ServiceHelper.getCrossReferenceAdapter(task);
		if (eCrossReferenceAdapter != null) {
			Collection<Setting> inverseReferences = eCrossReferenceAdapter.getInverseReferences(task);
			for (Setting setting : inverseReferences) {
				EObject object = setting.getEObject();

			}
		}
	}

}
