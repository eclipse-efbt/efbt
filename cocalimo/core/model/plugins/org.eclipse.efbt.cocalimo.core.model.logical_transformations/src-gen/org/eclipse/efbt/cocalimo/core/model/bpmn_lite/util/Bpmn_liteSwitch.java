/**
 */
package org.eclipse.efbt.cocalimo.core.model.bpmn_lite.util;

import org.eclipse.efbt.cocalimo.core.model.bpmn_lite.*;

import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;

import org.eclipse.emf.ecore.util.Switch;

/**
 * <!-- begin-user-doc -->
 * The <b>Switch</b> for the model's inheritance hierarchy.
 * It supports the call {@link #doSwitch(EObject) doSwitch(object)}
 * to invoke the <code>caseXXX</code> method for each class of the model,
 * starting with the actual class of the object
 * and proceeding up the inheritance hierarchy
 * until a non-null result is returned,
 * which is the result of the switch.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.cocalimo.core.model.bpmn_lite.Bpmn_litePackage
 * @generated
 */
public class Bpmn_liteSwitch<T> extends Switch<T> {
	/**
	 * The cached model package
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static Bpmn_litePackage modelPackage;

	/**
	 * Creates an instance of the switch.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Bpmn_liteSwitch() {
		if (modelPackage == null) {
			modelPackage = Bpmn_litePackage.eINSTANCE;
		}
	}

	/**
	 * Checks whether this is a switch for the given package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param ePackage the package in question.
	 * @return whether this is a switch for the given package.
	 * @generated
	 */
	@Override
	protected boolean isSwitchFor(EPackage ePackage) {
		return ePackage == modelPackage;
	}

	/**
	 * Calls <code>caseXXX</code> for each class of the model until one returns a non null result; it yields that result.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the first non-null result returned by a <code>caseXXX</code> call.
	 * @generated
	 */
	@Override
	protected T doSwitch(int classifierID, EObject theEObject) {
		switch (classifierID) {
			case Bpmn_litePackage.BASE_ELEMENT: {
				BaseElement baseElement = (BaseElement)theEObject;
				T result = caseBaseElement(baseElement);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bpmn_litePackage.FLOW_ELEMENTS_CONTAINER: {
				FlowElementsContainer flowElementsContainer = (FlowElementsContainer)theEObject;
				T result = caseFlowElementsContainer(flowElementsContainer);
				if (result == null) result = caseBaseElement(flowElementsContainer);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bpmn_litePackage.FLOW_ELEMENT: {
				FlowElement flowElement = (FlowElement)theEObject;
				T result = caseFlowElement(flowElement);
				if (result == null) result = caseBaseElement(flowElement);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bpmn_litePackage.FLOW_NODE: {
				FlowNode flowNode = (FlowNode)theEObject;
				T result = caseFlowNode(flowNode);
				if (result == null) result = caseFlowElement(flowNode);
				if (result == null) result = caseBaseElement(flowNode);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bpmn_litePackage.SEQUENCE_FLOW: {
				SequenceFlow sequenceFlow = (SequenceFlow)theEObject;
				T result = caseSequenceFlow(sequenceFlow);
				if (result == null) result = caseFlowElement(sequenceFlow);
				if (result == null) result = caseBaseElement(sequenceFlow);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bpmn_litePackage.TASK: {
				Task task = (Task)theEObject;
				T result = caseTask(task);
				if (result == null) result = caseActivity(task);
				if (result == null) result = caseFlowNode(task);
				if (result == null) result = caseFlowElement(task);
				if (result == null) result = caseBaseElement(task);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bpmn_litePackage.ACTIVITY: {
				Activity activity = (Activity)theEObject;
				T result = caseActivity(activity);
				if (result == null) result = caseFlowNode(activity);
				if (result == null) result = caseFlowElement(activity);
				if (result == null) result = caseBaseElement(activity);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bpmn_litePackage.GATEWAY: {
				Gateway gateway = (Gateway)theEObject;
				T result = caseGateway(gateway);
				if (result == null) result = caseFlowNode(gateway);
				if (result == null) result = caseFlowElement(gateway);
				if (result == null) result = caseBaseElement(gateway);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bpmn_litePackage.EXCLUSIVE_GATEWAY: {
				ExclusiveGateway exclusiveGateway = (ExclusiveGateway)theEObject;
				T result = caseExclusiveGateway(exclusiveGateway);
				if (result == null) result = caseGateway(exclusiveGateway);
				if (result == null) result = caseFlowNode(exclusiveGateway);
				if (result == null) result = caseFlowElement(exclusiveGateway);
				if (result == null) result = caseBaseElement(exclusiveGateway);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bpmn_litePackage.INCLUSIVE_GATEWAY: {
				InclusiveGateway inclusiveGateway = (InclusiveGateway)theEObject;
				T result = caseInclusiveGateway(inclusiveGateway);
				if (result == null) result = caseGateway(inclusiveGateway);
				if (result == null) result = caseFlowNode(inclusiveGateway);
				if (result == null) result = caseFlowElement(inclusiveGateway);
				if (result == null) result = caseBaseElement(inclusiveGateway);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bpmn_litePackage.PARALLEL_GATEWAY: {
				ParallelGateway parallelGateway = (ParallelGateway)theEObject;
				T result = caseParallelGateway(parallelGateway);
				if (result == null) result = caseGateway(parallelGateway);
				if (result == null) result = caseFlowNode(parallelGateway);
				if (result == null) result = caseFlowElement(parallelGateway);
				if (result == null) result = caseBaseElement(parallelGateway);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bpmn_litePackage.SERVICE_TASK: {
				ServiceTask serviceTask = (ServiceTask)theEObject;
				T result = caseServiceTask(serviceTask);
				if (result == null) result = caseTask(serviceTask);
				if (result == null) result = caseActivity(serviceTask);
				if (result == null) result = caseFlowNode(serviceTask);
				if (result == null) result = caseFlowElement(serviceTask);
				if (result == null) result = caseBaseElement(serviceTask);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bpmn_litePackage.SUB_PROCESS: {
				SubProcess subProcess = (SubProcess)theEObject;
				T result = caseSubProcess(subProcess);
				if (result == null) result = caseActivity(subProcess);
				if (result == null) result = caseFlowElementsContainer(subProcess);
				if (result == null) result = caseFlowNode(subProcess);
				if (result == null) result = caseFlowElement(subProcess);
				if (result == null) result = caseBaseElement(subProcess);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bpmn_litePackage.SCRIPT_TASK: {
				ScriptTask scriptTask = (ScriptTask)theEObject;
				T result = caseScriptTask(scriptTask);
				if (result == null) result = caseTask(scriptTask);
				if (result == null) result = caseActivity(scriptTask);
				if (result == null) result = caseFlowNode(scriptTask);
				if (result == null) result = caseFlowElement(scriptTask);
				if (result == null) result = caseBaseElement(scriptTask);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			default: return defaultCase(theEObject);
		}
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Base Element</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Base Element</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseBaseElement(BaseElement object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Flow Elements Container</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Flow Elements Container</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseFlowElementsContainer(FlowElementsContainer object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Flow Element</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Flow Element</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseFlowElement(FlowElement object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Flow Node</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Flow Node</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseFlowNode(FlowNode object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Sequence Flow</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Sequence Flow</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseSequenceFlow(SequenceFlow object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Task</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Task</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseTask(Task object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Activity</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Activity</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseActivity(Activity object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Gateway</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Gateway</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseGateway(Gateway object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Exclusive Gateway</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Exclusive Gateway</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseExclusiveGateway(ExclusiveGateway object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Inclusive Gateway</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Inclusive Gateway</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseInclusiveGateway(InclusiveGateway object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Parallel Gateway</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Parallel Gateway</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseParallelGateway(ParallelGateway object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Service Task</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Service Task</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseServiceTask(ServiceTask object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Sub Process</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Sub Process</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseSubProcess(SubProcess object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Script Task</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Script Task</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseScriptTask(ScriptTask object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EObject</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch, but this is the last case anyway.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EObject</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject)
	 * @generated
	 */
	@Override
	public T defaultCase(EObject object) {
		return null;
	}

} //Bpmn_liteSwitch
