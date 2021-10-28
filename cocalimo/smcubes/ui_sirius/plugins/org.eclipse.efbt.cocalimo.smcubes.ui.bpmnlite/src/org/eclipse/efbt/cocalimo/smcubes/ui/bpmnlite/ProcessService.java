/**
 * Copyright (c) 2011-2019 Obeo.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors:
 *     Obeo - initial API and implementation
 * 
 */
package org.eclipse.efbt.cocalimo.smcubes.ui.bpmnlite;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;


import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EStructuralFeature;
import org.eclipse.emf.ecore.util.EcoreUtil;
import org.eclipse.emf.ecore.util.EcoreUtil.Copier;

import bpmn2.Bpmn2Factory;

import bpmn2.ExclusiveGateway;
import bpmn2.FlowElement;
import bpmn2.FlowElementsContainer;
import bpmn2.FlowNode;

import bpmn2.InclusiveGateway;

import bpmn2.ParallelGateway;

import bpmn2.SequenceFlow;

import bpmn2.SubProcess;
import bpmn2.Task;

public class ProcessService {


	public EList<FlowElement> getSubElements(EObject eo) {
		if (eo instanceof SubProcess) {
			return ((SubProcess) eo).getFlowElements();
		}
		return null;
	}

	public boolean isApplicableType(EObject element) {
		return isEventTaskOrGateway(element) || isLaneChoregraphyTaskOrSubProcess(element);
	}

	private boolean isEventTaskOrGateway(EObject element) {
		return   element instanceof Task || element instanceof ParallelGateway
				|| element instanceof ExclusiveGateway || element instanceof InclusiveGateway;
				
	}

	private boolean isLaneChoregraphyTaskOrSubProcess(EObject element) {
		return  element instanceof SubProcess;
	}

	public EObject createFlowElement(EObject container, String typeToCreate) {
		FlowElementsContainer flowElementsContainer = getFlowElementsContainer(container);
		FlowNode newElement = createElement(typeToCreate);
		flowElementsContainer.getFlowElements().add(newElement);

		if (container instanceof SubProcess) {
				// TODO Nothing in the odesign migrated
			}
		
		return newElement;
	}

	private FlowElementsContainer getFlowElementsContainer(EObject eo) {
		FlowElementsContainer result = null;
		if (eo instanceof FlowElementsContainer) {
			result = (FlowElementsContainer) eo;
		} else if (eo.eContainer() != null ) {
			result = getFlowElementsContainer(eo.eContainer());
		}
		return result;
	}

	private FlowNode createElement(String typeToCreate) {
		FlowNode result = null;
		if ("Task".equals(typeToCreate)) {
			result = Bpmn2Factory.eINSTANCE.createTask();
		}  else if ("ServiceTask".equals(typeToCreate)) {
			result = Bpmn2Factory.eINSTANCE.createServiceTask();
		} else if ("ParallelGateway".equals(typeToCreate)) {
			result = Bpmn2Factory.eINSTANCE.createParallelGateway();
		} else if ("ExclusiveGateway".equals(typeToCreate)) {
			result = Bpmn2Factory.eINSTANCE.createExclusiveGateway();
		} else if ("InclusiveGateway".equals(typeToCreate)) {
			result = Bpmn2Factory.eINSTANCE.createInclusiveGateway();
		} 
		return result;
	}

}