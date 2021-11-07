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

import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.Bpmn_liteFactory;
import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.Bpmn_litePackage;
import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.FlowElementsContainer;
import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.ServiceTask;
import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.Task;
import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.impl.TaskImpl;

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




}
