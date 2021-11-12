package org.eclipse.efbt.controller.ldm.component.logical_transformations;

import java.util.Iterator;

import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.Gateway;
import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.ScriptTask;
import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.SequenceFlow;
import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.ServiceTask;
import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.SubProcess;
import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.Task;
import org.eclipse.efbt.cocalimo.core.model.logical_transformations.LogicalTransformationModule;
import org.eclipse.efbt.cocalimo.core.model.logical_transformations.Scenario;
import org.eclipse.emf.common.util.BasicEList;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.TreeIterator;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.util.EcoreUtil;

public class LogicalTransformationViews {

	public static void createLogicalTransformationViewForScope(LogicalTransformationModule logicalTransformationModule,
			EList<ScriptTask> scriptTasksInScope, EList<Scenario> scenariosOutOfScope, boolean keepLayout) {
		
		markScriptTaskAsInvisible(logicalTransformationModule, scriptTasksInScope);
		EList<EAttribute> requiredAttributes = getListOfRequiredAttributes(logicalTransformationModule, scriptTasksInScope,scenariosOutOfScope);
		markDependantServiceTasksAsInvisible(logicalTransformationModule,requiredAttributes);
		markDependantTasksAsInvisible(logicalTransformationModule,requiredAttributes);
		markGatewaysPointingToInvisibleTasksAsInvisible(logicalTransformationModule,logicalTransformationModule);

	}

	private static void markDependantTasksAsInvisible(LogicalTransformationModule logicalTransformationModule, EList<EAttribute> requiredAttributes) {
		SubProcess subProcess = logicalTransformationModule.getSubProcess();
		
		TreeIterator<Object> subProcessContents = EcoreUtil.getAllContents(subProcess, true);
		while (subProcessContents.hasNext())
		{
			Object o = subProcessContents.next();
			if ((o instanceof Task) && !(o instanceof ServiceTask) && !(o instanceof ScriptTask))
			{
				Task task = (Task) o;
				EClass entity = task.getEntity();
				EList<EAttribute> entityAttributes = entity.getEAllAttributes();
				boolean entityContainsRequiredAttributes = false;
				for (EAttribute eAttribute : entityAttributes) 
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

	private static void markDependantServiceTasksAsInvisible(LogicalTransformationModule logicalTransformationModule, EList<EAttribute> requiredAttributes) {

		SubProcess subProcess = logicalTransformationModule.getSubProcess();
		
		TreeIterator<Object> subProcessContents = EcoreUtil.getAllContents(subProcess, true);
		while (subProcessContents.hasNext())
		{
			Object o = subProcessContents.next();
			if (o instanceof ServiceTask)
			{
				ServiceTask serviceTask = (ServiceTask) o;
				EAttribute enrichedAttribute = serviceTask.getEnrichedAttribute();
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

	private static EList<EAttribute> getListOfRequiredAttributes(
			LogicalTransformationModule logicalTransformationModule, EList<ScriptTask> scriptTasksInScope,
			EList<Scenario> scenariosOutOfScope) {
		EList<EAttribute> requiredAttributesList = new BasicEList<EAttribute>();
		
		SubProcess subProcess = logicalTransformationModule.getSubProcess();
		
		TreeIterator<Object> subProcessContents = EcoreUtil.getAllContents(subProcess, true);
		while (subProcessContents.hasNext())
		{
			Object o = subProcessContents.next();
			if (o instanceof ScriptTask)
			{
				if (scriptTasksInScope.contains(o))
				{
					
					EList<EAttribute> scriptTaskAttributes = ( (ScriptTask) o).getLinkedAttributes();
					for (Iterator iterator = scriptTaskAttributes.iterator(); iterator.hasNext();) {
						EAttribute eAttribute = (EAttribute) iterator.next();
						
						if(!requiredAttributesList.contains(eAttribute))
						{
							requiredAttributesList.add(eAttribute);
							EList<EAttribute> transientDependantAttributes = getTransientDependantAttributes(subProcess,eAttribute,scenariosOutOfScope);
						}
							
						
					}
					
				}
				
			}
		}
		
		
		return requiredAttributesList;
	}

	private static EList<EAttribute> getTransientDependantAttributes(SubProcess subProcess, 
																		EAttribute eAttribute,
																		EList<Scenario> scenariosOutOfScope) {
		// find any ServiceTasks that are calculating that attribute
		// for each scenario of that attribute (unless it is out of scope), we further get the 
		// transient dependant attributes of that too and add to the list.
		
		EList<EAttribute> transientDependantAttributes = new BasicEList<EAttribute>();
		TreeIterator<Object> subProcessContents = EcoreUtil.getAllContents(subProcess, true);
		while (subProcessContents.hasNext())
		{
			Object o = subProcessContents.next();
			if (o instanceof ServiceTask)
			{
				ServiceTask serviceTask = (ServiceTask) o;
				EAttribute enrichedAttribute = serviceTask.getEnrichedAttribute();
				if (eAttribute == enrichedAttribute)
				{
					EList<Scenario> scenarios = serviceTask.getScenarios();
					for (Scenario scenario : scenarios) {
						if (!scenariosOutOfScope.contains(scenario))
						{
							EList<EAttribute> requiredAttributes = scenario.getRequiredAttributes();
							for (EAttribute requiredAttribute : requiredAttributes) {
							
								if(!transientDependantAttributes.contains(requiredAttribute))
								{
									transientDependantAttributes.add(requiredAttribute);
									EList<EAttribute> furtherTransientDependantAttributes = getTransientDependantAttributes(subProcess,
																	requiredAttribute,
																	scenariosOutOfScope);
											
									for (EAttribute eAttribute2 : furtherTransientDependantAttributes) 
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
			LogicalTransformationModule logicalTransformationModule, LogicalTransformationModule logicalTransformationModule2) {

		SubProcess subProcess = logicalTransformationModule.getSubProcess();
		TreeIterator<Object> subProcessContents = EcoreUtil.getAllContents(subProcess, true);
		while (subProcessContents.hasNext())
		{
			Object o = subProcessContents.next();
			if (o instanceof Gateway)
			{
				 Gateway gateway = (Gateway) o;
				 EList<Task> tasks = getTasksPointedToByGateway(gateway, subProcess);
				 boolean allTasksInvisible = true;
				 for (Task task : tasks) 
				 {
					if(!task.isInvisible())
						allTasksInvisible = false;
				 }
				 if ( allTasksInvisible)
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


	private static EList<Task> getTasksPointedToByGateway(Gateway gateway, SubProcess subProcess) {
		
		TreeIterator<Object> subProcessContents = EcoreUtil.getAllContents(subProcess, true);
		EList<Task> tasks = new BasicEList<Task>();
		while (subProcessContents.hasNext())
		{
			Object o = subProcessContents.next();
			if (o instanceof SequenceFlow)
			{
				SequenceFlow flow = (SequenceFlow) o;
				if (flow.getSourceRef().equals(gateway) && (flow.getTargetRef() instanceof Task))
				{
					tasks.add((Task) flow.getTargetRef());
				}
			
			}
		}
		
		return tasks;
		
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
