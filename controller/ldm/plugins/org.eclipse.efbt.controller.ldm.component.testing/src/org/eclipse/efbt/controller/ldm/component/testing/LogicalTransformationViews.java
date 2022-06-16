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
package org.eclipse.efbt.controller.ldm.component.testing;

import java.util.Iterator;

import org.eclipse.efbt.openregspecs.model.open_reg_specs.Activity;
import org.eclipse.efbt.openregspecs.model.open_reg_specs.FlowElement;
import org.eclipse.efbt.openregspecs.model.open_reg_specs.Gateway;
import org.eclipse.efbt.openregspecs.model.open_reg_specs.ScriptTask;
import org.eclipse.efbt.openregspecs.model.open_reg_specs.SequenceFlow;
import org.eclipse.efbt.openregspecs.model.open_reg_specs.ServiceTask;
import org.eclipse.efbt.openregspecs.model.open_reg_specs.SubProcess;
import org.eclipse.efbt.openregspecs.model.open_reg_specs.UserTask;
import org.eclipse.efbt.openregspecs.model.open_reg_specs.E2ETestScope;
//import org.eclipse.efbt.openregspecs.model.open_reg_specs.LogicalTransformationModule;
import org.eclipse.efbt.openregspecs.model.open_reg_specs.Scenario;
import org.eclipse.efbt.openregspecs.model.open_reg_specs.SelectionLayer;
//import org.eclipse.efbt.openregspecs.model.open_reg_specs.CreateLogicalTransformationViewForScope;
import org.eclipse.efbt.openregspecs.model.open_reg_specs.Attribute;
import org.eclipse.efbt.openregspecs.model.open_reg_specs.BasicEntity;
import org.eclipse.emf.common.util.BasicEList;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.TreeIterator;
import org.eclipse.emf.ecore.util.EcoreUtil;

public class LogicalTransformationViews {

	/**public static void createLogicalTransformationViewForScope(LogicalTransformationModule logicalTransformationModule,
			EList<E2ETestScope> eList, CreateLogicalTransformationViewForScope call) {
		markScriptTaskAsInvisible(logicalTransformationModule, eList);

		EList<Attribute> requiredAttributes = 
				getListOfRequiredAttributes(logicalTransformationModule,
						eList);
		call.getAttributeLineage().clear();
		
		setAllAttributesAsOrdered(logicalTransformationModule);
		
		for (Attribute eAttribute : requiredAttributes) {
			call.getAttributeLineage().add(eAttribute);
			eAttribute.setOrdered(false);
		}
		markDependantServiceTasksAsInvisible(logicalTransformationModule,requiredAttributes);
		markDependantTasksAsInvisible(logicalTransformationModule,requiredAttributes);
		markGatewaysPointingToInvisibleTasksAsInvisible(logicalTransformationModule);
		markEmptySubProcessesAsInvisible(logicalTransformationModule.getSubProcess());

	}

	

	private static void setAllAttributesAsOrdered(LogicalTransformationModule logicalTransformationModule) {
		SubProcess subProcess = logicalTransformationModule.getSubProcess();
		
		
		TreeIterator<Object> subProcessContents = EcoreUtil.getAllContents(subProcess, true);
		
			while (subProcessContents.hasNext())
			{
				Object o = subProcessContents.next();
				if (o instanceof ServiceTask)
				{
					 Attribute attr = ((ServiceTask)o).getEnrichedAttribute();
					 attr.setOrdered(true);
				}
				if (o instanceof UserTask)
				{
					 EList<Attribute> attributes = ((UserTask)o).getEntity().getAttributes();
					 for (Attribute attribute : attributes) {
						 attribute.setOrdered(true);
					 }
				}
			}	
					
		
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

	private static void markDependantTasksAsInvisible(LogicalTransformationModule logicalTransformationModule, EList<Attribute> requiredAttributes) {
		SubProcess subProcess = logicalTransformationModule.getSubProcess();
		
		TreeIterator<Object> subProcessContents = EcoreUtil.getAllContents(subProcess, true);
		while (subProcessContents.hasNext())
		{
			Object o = subProcessContents.next();
			if (o instanceof UserTask)
			{
				UserTask task = (UserTask) o;
				BasicEntity entity = task.getEntity();
				EList<Attribute> entityAttributes = entity.getAttributes();
				boolean entityContainsRequiredAttributes = false;
				for (Attribute eAttribute : entityAttributes) 
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

	private static void markDependantServiceTasksAsInvisible(LogicalTransformationModule logicalTransformationModule, EList<Attribute> requiredAttributes) {

		SubProcess subProcess = logicalTransformationModule.getSubProcess();
		
		TreeIterator<Object> subProcessContents = EcoreUtil.getAllContents(subProcess, true);
		while (subProcessContents.hasNext())
		{
			Object o = subProcessContents.next();
			if (o instanceof ServiceTask)
			{
				ServiceTask serviceTask = (ServiceTask) o;
				Attribute enrichedAttribute = serviceTask.getEnrichedAttribute();
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

	private static EList<Attribute> getListOfRequiredAttributes(
			LogicalTransformationModule logicalTransformationModule, EList<E2ETestScope> e2eTestScopes) {
		EList<Attribute> requiredAttributesList = new BasicEList<Attribute>();
		
		SubProcess subProcess = logicalTransformationModule.getSubProcess();
		
		
		TreeIterator<Object> subProcessContents = EcoreUtil.getAllContents(subProcess, true);
		for (E2ETestScope scope : e2eTestScopes) {
			
			ScriptTask scriptTaskInScope = scope.getScriptTask();
			SelectionLayer layer = scope.getLayer();
			EList<Scenario> scenariosInScope = scope.getScenarios();
			while (subProcessContents.hasNext())
			{
				Object o = subProcessContents.next();
				if (o instanceof ScriptTask)
				{
					if (scriptTaskInScope.equals(o))
					{										
							EList<Attribute> scriptTaskAttributes = layer.getGeneratedEntity().getAttributes();
							for (Iterator iterator = scriptTaskAttributes.iterator(); iterator.hasNext();) {
								Attribute eAttribute = (Attribute) iterator.next();
								
								if(!requiredAttributesList.contains(eAttribute))
								{
									requiredAttributesList.add(eAttribute);
									EList<Attribute> transientDependantAttributes = 
											getTransientDependantAttributes(subProcess,
																			eAttribute,
																			scenariosInScope);
									for (Attribute eAttribute2 : transientDependantAttributes) {
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
			}
		
		
		return requiredAttributesList;
	}

	private static EList<Attribute> getTransientDependantAttributes(SubProcess subProcess, 
			Attribute eAttribute,EList<Scenario> scenariosInScope) {
		// find any ServiceTasks that are calculating that attribute
		// for each scenario of that attribute (unless it is out of scope), we further get the 
		// transient dependant attributes of that too and add to the list.
		
		EList<Attribute> transientDependantAttributes = new BasicEList<Attribute>();
		TreeIterator<Object> subProcessContents = EcoreUtil.getAllContents(subProcess, true);
		while (subProcessContents.hasNext())
		{
			Object o = subProcessContents.next();
			if (o instanceof ServiceTask)
			{
				ServiceTask serviceTask = (ServiceTask) o;
				Attribute enrichedAttribute = serviceTask.getEnrichedAttribute();
				if (eAttribute == enrichedAttribute)
				{
					EList<Scenario> scenarios = serviceTask.getScenarios();
					for (Scenario scenario : scenarios) {
						if (scenariosInScope.contains(scenario))
						{
							EList<Attribute> requiredAttributes = scenario.getRequiredAttributes();
							for (Attribute requiredAttribute : requiredAttributes) {
							
								if(!transientDependantAttributes.contains(requiredAttribute))
								{
									transientDependantAttributes.add(requiredAttribute);
									EList<Attribute> furtherTransientDependantAttributes = getTransientDependantAttributes(subProcess,
																	requiredAttribute,
																	scenariosInScope);
											
									for (Attribute eAttribute2 : furtherTransientDependantAttributes) 
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
			EList<E2ETestScope> eList) {
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
						if (testScopesContainsScriptTask(eList, (ScriptTask) o))
						{
							EList<SelectionLayer> layers = ((ScriptTask) o).getSelectionLayers();
							for (SelectionLayer selectionLayer : layers) {
								if (testScopesContainsLayer(eList,(ScriptTask) o,  selectionLayer))
									selectionLayer.setInvisible(false);
								else
									selectionLayer.setInvisible(true);
							}
							( (ScriptTask) o).setInvisible(false);
						}
						else
						{
							( (ScriptTask) o).setInvisible(true);
							EList<SelectionLayer> layers = ((ScriptTask) o).getSelectionLayers();
							for (SelectionLayer selectionLayer : layers) {								
									selectionLayer.setInvisible(true);
							}
						}
					}
				}
	}

	private static boolean testScopesContainsScriptTask(EList<E2ETestScope> eList, ScriptTask o) {
		// TODO Auto-generated method stub
		boolean returnValue = false;
		for (E2ETestScope testScope : eList) {
			
			if ((testScope).getScriptTask().equals(o))
			{
				returnValue =true;
			}
		
		}
		return returnValue;
		
	}
	
	private static boolean testScopesContainsLayer(EList<E2ETestScope> eList, ScriptTask o, SelectionLayer layer) {
		// TODO Auto-generated method stub
		boolean returnValue = false;
		for (E2ETestScope testScope : eList) {
			
			if ((testScope).getScriptTask().equals(o))
			{
				EList<SelectionLayer> layers = ((ScriptTask) o).getSelectionLayers();
				for (SelectionLayer selectionLayer : layers) {
					if(selectionLayer.equals(layer))
						returnValue =true;
				}
				
			}
		
		}
		return returnValue;
		
	}*/

}
