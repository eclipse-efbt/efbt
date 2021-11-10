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
package org.eclipse.efbt.cocalimo.core.ui.bpmnlite;

import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.Bpmn_liteFactory;
import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.ExclusiveGateway;
import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.FlowElement;
import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.FlowElementsContainer;
import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.FlowNode;
import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.InclusiveGateway;
import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.ParallelGateway;
import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.SubProcess;
import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.Task;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.ecore.EObject;

public class ProcessService {


	public EList<FlowElement> getSubElements(EObject eo) { 
		if (eo instanceof SubProcess) {
			return ((SubProcess) eo).getFlowElements();
		}
		return null;
	}

	public boolean isApplicableType(EObject element) {
		return isEventTaskOrGateway(element) || isSubProcess(element);
	}

	private boolean isEventTaskOrGateway(EObject element) {
		return   element instanceof Task || element instanceof ParallelGateway
				|| element instanceof ExclusiveGateway || element instanceof InclusiveGateway;
				
	}

	private boolean isSubProcess(EObject element) {
		return  element instanceof SubProcess;
	}

	public EObject createFlowElement(EObject container, String typeToCreate) {
		FlowElementsContainer flowElementsContainer = getFlowElementsContainer(container);
		FlowNode newElement = createElement(typeToCreate);
		flowElementsContainer.getFlowElements().add(newElement);		
		
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
			result = Bpmn_liteFactory.eINSTANCE.createTask();
		}  else if ("ServiceTask".equals(typeToCreate)) {
			result = Bpmn_liteFactory.eINSTANCE.createServiceTask();
		}  else if ("ScriptTask".equals(typeToCreate)) {
			result = Bpmn_liteFactory.eINSTANCE.createScriptTask();
		} else if ("ParallelGateway".equals(typeToCreate)) {
			result = Bpmn_liteFactory.eINSTANCE.createParallelGateway();
		} else if ("ExclusiveGateway".equals(typeToCreate)) {
			result = Bpmn_liteFactory.eINSTANCE.createExclusiveGateway();
		} else if ("InclusiveGateway".equals(typeToCreate)) {
			result = Bpmn_liteFactory.eINSTANCE.createInclusiveGateway();
		} 
		return result;
	}

}