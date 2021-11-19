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
package org.eclipse.efbt.controller.ldm.component.logical_transformations;

import java.util.Iterator;

import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.Activity;
import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.FlowElement;
import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.Gateway;
import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.ScriptTask;
import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.SequenceFlow;
import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.ServiceTask;
import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.SubProcess;
import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.UserTask;
import org.eclipse.efbt.cocalimo.core.model.logical_transformations.LogicalTransformationModule;
import org.eclipse.efbt.cocalimo.core.model.logical_transformations.Scenario;
import org.eclipse.efbt.cocalimo.core.model.platform_call.CreateLogicalTransformationViewForScope;
import org.eclipse.emf.common.util.BasicEList;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.TreeIterator;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EStructuralFeature;
import org.eclipse.emf.ecore.util.EcoreUtil;

public class LogicalTransformationViews {

	public static void createLogicalTransformationViewForScope(LogicalTransformationModule logicalTransformationModule,
			EList<ScriptTask> scriptTasksInScope, EList<Scenario> scenariosOutOfScope, CreateLogicalTransformationViewForScope call) {
		
		markScriptTaskAsInvisible(logicalTransformationModule, scriptTasksInScope);
		EList<EStructuralFeature> requiredAttributes = getListOfRequiredAttributes(logicalTransformationModule, scriptTasksInScope,scenariosOutOfScope);
		call.getAttributeLineage().clear();
		for (EStructuralFeature eAttribute : requiredAttributes) {
			call.getAttributeLineage().add(eAttribute);
		}
		markDependantServiceTasksAsInvisible(logicalTransformationModule,requiredAttributes);
		markDependantTasksAsInvisible(logicalTransformationModule,requiredAttributes);
		markGatewaysPointingToInvisibleTasksAsInvisible(logicalTransformationModule);
		markEmptySubProcessesAsInvisible(logicalTransformationModule.getSubProcess());

	}

	private static void markEmptySubProcessesAsInvisible(SubProcess subProcess) {


		TreeIterator<Object> subProcessContents = EcoreUtil.getAllContents(subProcess, true);
		while (subProcessContents.hasNext())
		{
			Object o = subProcessContents.next();
			if (o instanceof SubProcess)
			{
				SubProcess theSubProcess = (SubProcess) o;
				markEmptySubProcessesAsInvisible(theSubProcess);
			}
		}
		
		 EList<FlowElement> flowElements = subProcess.getFlowElements();
		 boolean allActivityInvisible = true;
		 for (FlowElement flowElement : flowElements) 
		 {
			if((flowElement instanceof Activity) && !flowElement.isInvisible())
				allActivityInvisible = false;
		 }
		 if ( allActivityInvisible)
		 {
			 subProcess.setInvisible(true);
		 }
		 else
		 {
			 subProcess.setInvisible(false);
		 }
		
	}

	private static void markDependantTasksAsInvisible(LogicalTransformationModule logicalTransformationModule, EList<EStructuralFeature> requiredAttributes) {
		SubProcess subProcess = logicalTransformationModule.getSubProcess();
		
		TreeIterator<Object> subProcessContents = EcoreUtil.getAllContents(subProcess, true);
		while (subProcessContents.hasNext())
		{
			Object o = subProcessContents.next();
			if (o instanceof UserTask)
			{
				UserTask task = (UserTask) o;
				EClass entity = task.getEntity();
				EList<EStructuralFeature> entityAttributes = entity.getEStructuralFeatures();
				boolean entityContainsRequiredAttributes = false;
				for (EStructuralFeature eAttribute : entityAttributes) 
				{
					if(requiredAttributes.contains(eAttribute))
					{
						entityContainsRequiredAttributes = true;
					}
				}
				if (entityContainsRequiredAttributes)
				{
					task.setInvisible(false);
				}
				else
				{
					task.setInvisible(true);
				}
			}
			
		}
		
	}

	private static void markDependantServiceTasksAsInvisible(LogicalTransformationModule logicalTransformationModule, EList<EStructuralFeature> requiredAttributes) {

		SubProcess subProcess = logicalTransformationModule.getSubProcess();
		
		TreeIterator<Object> subProcessContents = EcoreUtil.getAllContents(subProcess, true);
		while (subProcessContents.hasNext())
		{
			Object o = subProcessContents.next();
			if (o instanceof ServiceTask)
			{
				ServiceTask serviceTask = (ServiceTask) o;
				EStructuralFeature enrichedAttribute = serviceTask.getEnrichedAttribute();
				if(requiredAttributes.contains(enrichedAttribute))
				{
					serviceTask.setInvisible(false);
				}
				else
				{
					serviceTask.setInvisible(true);
				}
			}
		}
		
	}

	private static EList<EStructuralFeature> getListOfRequiredAttributes(
			LogicalTransformationModule logicalTransformationModule, EList<ScriptTask> scriptTasksInScope,
			EList<Scenario> scenariosOutOfScope) {
		EList<EStructuralFeature> requiredAttributesList = new BasicEList<EStructuralFeature>();
		
		SubProcess subProcess = logicalTransformationModule.getSubProcess();
		
		TreeIterator<Object> subProcessContents = EcoreUtil.getAllContents(subProcess, true);
		while (subProcessContents.hasNext())
		{
			Object o = subProcessContents.next();
			if (o instanceof ScriptTask)
			{
				if (scriptTasksInScope.contains(o))
				{
					
					EList<EStructuralFeature> scriptTaskAttributes = ( (ScriptTask) o).getLinkedAttributes();
					for (Iterator iterator = scriptTaskAttributes.iterator(); iterator.hasNext();) {
						EStructuralFeature eAttribute = (EStructuralFeature) iterator.next();
						
						if(!requiredAttributesList.contains(eAttribute))
						{
							requiredAttributesList.add(eAttribute);
							EList<EStructuralFeature> transientDependantAttributes = getTransientDependantAttributes(subProcess,eAttribute,scenariosOutOfScope);
							for (EStructuralFeature eAttribute2 : transientDependantAttributes) {
								if(!requiredAttributesList.contains(eAttribute2))
								{
									requiredAttributesList.add(eAttribute2);
								}
							}
						}
							
						
					}
					
				}
				
			}
		}
		
		
		return requiredAttributesList;
	}

	private static EList<EStructuralFeature> getTransientDependantAttributes(SubProcess subProcess, 
			EStructuralFeature eAttribute,
																		EList<Scenario> scenariosOutOfScope) {
		// find any ServiceTasks that are calculating that attribute
		// for each scenario of that attribute (unless it is out of scope), we further get the 
		// transient dependant attributes of that too and add to the list.
		
		EList<EStructuralFeature> transientDependantAttributes = new BasicEList<EStructuralFeature>();
		TreeIterator<Object> subProcessContents = EcoreUtil.getAllContents(subProcess, true);
		while (subProcessContents.hasNext())
		{
			Object o = subProcessContents.next();
			if (o instanceof ServiceTask)
			{
				ServiceTask serviceTask = (ServiceTask) o;
				EStructuralFeature enrichedAttribute = serviceTask.getEnrichedAttribute();
				if (eAttribute == enrichedAttribute)
				{
					EList<Scenario> scenarios = serviceTask.getScenarios();
					for (Scenario scenario : scenarios) {
						if (!scenariosOutOfScope.contains(scenario))
						{
							EList<EStructuralFeature> requiredAttributes = scenario.getRequiredAttributes();
							for (EStructuralFeature requiredAttribute : requiredAttributes) {
							
								if(!transientDependantAttributes.contains(requiredAttribute))
								{
									transientDependantAttributes.add(requiredAttribute);
									EList<EStructuralFeature> furtherTransientDependantAttributes = getTransientDependantAttributes(subProcess,
																	requiredAttribute,
																	scenariosOutOfScope);
											
									for (EStructuralFeature eAttribute2 : furtherTransientDependantAttributes) 
									{
										if(!transientDependantAttributes.contains(eAttribute2))
										{
											transientDependantAttributes.add(eAttribute2);
										}
									}
								}
							}
						}
						
					}
				}
			}
		}
		
		return transientDependantAttributes;
	}

	private static void markGatewaysPointingToInvisibleTasksAsInvisible(
			LogicalTransformationModule logicalTransformationModule) {

		SubProcess subProcess = logicalTransformationModule.getSubProcess();
		TreeIterator<Object> subProcessContents = EcoreUtil.getAllContents(subProcess, true);
		while (subProcessContents.hasNext())
		{
			Object o = subProcessContents.next();
			if (o instanceof Gateway)
			{
				 Gateway gateway = (Gateway) o;
				 EList<Activity> activies = getActivitiesPointedToByGateway(gateway, subProcess);
				 boolean allActivityInvisible = true;
				 for (Activity activity : activies) 
				 {
					if(!activity.isInvisible())
						allActivityInvisible = false;
				 }
				 if ( allActivityInvisible)
				 {
					 gateway.setInvisible(true);
				 }
				 else
				 {
					 gateway.setInvisible(false);
				 }
			}
		}
		
	}


	private static EList<Activity> getActivitiesPointedToByGateway(Gateway gateway, SubProcess subProcess) {
		
		TreeIterator<Object> subProcessContents = EcoreUtil.getAllContents(subProcess, true);
		EList<Activity> activities = new BasicEList<Activity>();
		while (subProcessContents.hasNext())
		{
			Object o = subProcessContents.next();
			if (o instanceof SequenceFlow)
			{
				SequenceFlow flow = (SequenceFlow) o;
				if (flow.getSourceRef().equals(gateway) && (flow.getTargetRef() instanceof Activity))
				{
					activities.add((Activity) flow.getTargetRef());
				}
			
			}
		}
		
		return activities;
		
	}

	private static void markScriptTaskAsInvisible(LogicalTransformationModule logicalTransformationModule,
			EList<ScriptTask> scriptTasksInScope) {
				//create the view as a straight copy of the subProcess if keepLayout is False
				//set visible on the script tasks
				SubProcess subProcess = logicalTransformationModule.getSubProcess();
				//get AllScript task, if they are in the list set invisable as false
				TreeIterator<Object> subProcessContents = EcoreUtil.getAllContents(subProcess, true);
				while (subProcessContents.hasNext())
				{
					Object o = subProcessContents.next();
					if (o instanceof ScriptTask)
					{
						if (scriptTasksInScope.contains(o))
						{
							( (ScriptTask) o).setInvisible(false);
						}
						else
						{
							( (ScriptTask) o).setInvisible(true);
						}
					}
				}
	}

}
