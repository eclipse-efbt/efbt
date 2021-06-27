/**
 */
package org.eclipse.efbt.cocalimo.core.model.functionality_module.util;

import org.eclipse.efbt.cocalimo.core.model.functionality_module.*;

import org.eclipse.efbt.cocalimo.core.model.requirements_text.Tag;

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
 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.Functionality_modulePackage
 * @generated
 */
public class Functionality_moduleSwitch<T> extends Switch<T> {
	/**
	 * The cached model package
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static Functionality_modulePackage modelPackage;

	/**
	 * Creates an instance of the switch.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Functionality_moduleSwitch() {
		if (modelPackage == null) {
			modelPackage = Functionality_modulePackage.eINSTANCE;
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
			case Functionality_modulePackage.DATA_PROCESSING_FUNCTIONALITY_MODULE: {
				DataProcessingFunctionalityModule dataProcessingFunctionalityModule = (DataProcessingFunctionalityModule)theEObject;
				T result = caseDataProcessingFunctionalityModule(dataProcessingFunctionalityModule);
				if (result == null) result = caseLeafFunctionalityModule(dataProcessingFunctionalityModule);
				if (result == null) result = caseFunctionalityModule(dataProcessingFunctionalityModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Functionality_modulePackage.LEAF_FUNCTIONALITY_MODULE: {
				LeafFunctionalityModule leafFunctionalityModule = (LeafFunctionalityModule)theEObject;
				T result = caseLeafFunctionalityModule(leafFunctionalityModule);
				if (result == null) result = caseFunctionalityModule(leafFunctionalityModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Functionality_modulePackage.PARALLEL_FUNCTIONALITY_MODULE: {
				ParallelFunctionalityModule parallelFunctionalityModule = (ParallelFunctionalityModule)theEObject;
				T result = caseParallelFunctionalityModule(parallelFunctionalityModule);
				if (result == null) result = caseFunctionalityModuleSet(parallelFunctionalityModule);
				if (result == null) result = caseFunctionalityModule(parallelFunctionalityModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Functionality_modulePackage.SCENARIO_SET_FUNCTIONALITY_MODULE: {
				ScenarioSetFunctionalityModule scenarioSetFunctionalityModule = (ScenarioSetFunctionalityModule)theEObject;
				T result = caseScenarioSetFunctionalityModule(scenarioSetFunctionalityModule);
				if (result == null) result = caseFunctionalityModuleSet(scenarioSetFunctionalityModule);
				if (result == null) result = caseFunctionalityModule(scenarioSetFunctionalityModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Functionality_modulePackage.SERIAL_FUNCTIONALITY_MODULE: {
				SerialFunctionalityModule serialFunctionalityModule = (SerialFunctionalityModule)theEObject;
				T result = caseSerialFunctionalityModule(serialFunctionalityModule);
				if (result == null) result = caseFunctionalityModuleSet(serialFunctionalityModule);
				if (result == null) result = caseFunctionalityModule(serialFunctionalityModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Functionality_modulePackage.FUNCTIONALITY_MODULE_SET: {
				FunctionalityModuleSet functionalityModuleSet = (FunctionalityModuleSet)theEObject;
				T result = caseFunctionalityModuleSet(functionalityModuleSet);
				if (result == null) result = caseFunctionalityModule(functionalityModuleSet);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Functionality_modulePackage.READ_DATA_SOURCE_FUNCTIONALITY_MODULE: {
				ReadDataSourceFunctionalityModule readDataSourceFunctionalityModule = (ReadDataSourceFunctionalityModule)theEObject;
				T result = caseReadDataSourceFunctionalityModule(readDataSourceFunctionalityModule);
				if (result == null) result = caseLeafFunctionalityModule(readDataSourceFunctionalityModule);
				if (result == null) result = caseFunctionalityModule(readDataSourceFunctionalityModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Functionality_modulePackage.REPORT_CELL_CREATION_FUNCTIONALITY_MODULE: {
				ReportCellCreationFunctionalityModule reportCellCreationFunctionalityModule = (ReportCellCreationFunctionalityModule)theEObject;
				T result = caseReportCellCreationFunctionalityModule(reportCellCreationFunctionalityModule);
				if (result == null) result = caseLeafFunctionalityModule(reportCellCreationFunctionalityModule);
				if (result == null) result = caseFunctionalityModule(reportCellCreationFunctionalityModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Functionality_modulePackage.FUNCTIONALITY_MODULE: {
				FunctionalityModule functionalityModule = (FunctionalityModule)theEObject;
				T result = caseFunctionalityModule(functionalityModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Functionality_modulePackage.FUNCTIONALITY_MODULE_MODULE: {
				FunctionalityModuleModule functionalityModuleModule = (FunctionalityModuleModule)theEObject;
				T result = caseFunctionalityModuleModule(functionalityModuleModule);
				if (result == null) result = caseModule(functionalityModuleModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Functionality_modulePackage.FUNCTIONALITY_MODULE_TAG: {
				FunctionalityModuleTag functionalityModuleTag = (FunctionalityModuleTag)theEObject;
				T result = caseFunctionalityModuleTag(functionalityModuleTag);
				if (result == null) result = caseTag(functionalityModuleTag);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Functionality_modulePackage.VALIDATION_FUNCTIONALITY_MODULE: {
				ValidationFunctionalityModule validationFunctionalityModule = (ValidationFunctionalityModule)theEObject;
				T result = caseValidationFunctionalityModule(validationFunctionalityModule);
				if (result == null) result = caseLeafFunctionalityModule(validationFunctionalityModule);
				if (result == null) result = caseFunctionalityModule(validationFunctionalityModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Functionality_modulePackage.PARALLEL_CHOICE_FUNCTIONALITY_MODULE: {
				ParallelChoiceFunctionalityModule parallelChoiceFunctionalityModule = (ParallelChoiceFunctionalityModule)theEObject;
				T result = caseParallelChoiceFunctionalityModule(parallelChoiceFunctionalityModule);
				if (result == null) result = caseFunctionalityModuleSet(parallelChoiceFunctionalityModule);
				if (result == null) result = caseFunctionalityModule(parallelChoiceFunctionalityModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			default: return defaultCase(theEObject);
		}
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Data Processing Functionality Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Data Processing Functionality Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseDataProcessingFunctionalityModule(DataProcessingFunctionalityModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Leaf Functionality Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Leaf Functionality Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseLeafFunctionalityModule(LeafFunctionalityModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Parallel Functionality Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Parallel Functionality Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseParallelFunctionalityModule(ParallelFunctionalityModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Scenario Set Functionality Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Scenario Set Functionality Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseScenarioSetFunctionalityModule(ScenarioSetFunctionalityModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Serial Functionality Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Serial Functionality Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseSerialFunctionalityModule(SerialFunctionalityModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Functionality Module Set</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Functionality Module Set</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseFunctionalityModuleSet(FunctionalityModuleSet object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Read Data Source Functionality Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Read Data Source Functionality Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseReadDataSourceFunctionalityModule(ReadDataSourceFunctionalityModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Report Cell Creation Functionality Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Report Cell Creation Functionality Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseReportCellCreationFunctionalityModule(ReportCellCreationFunctionalityModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Functionality Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Functionality Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseFunctionalityModule(FunctionalityModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Functionality Module Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Functionality Module Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseFunctionalityModuleModule(FunctionalityModuleModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Functionality Module Tag</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Functionality Module Tag</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseFunctionalityModuleTag(FunctionalityModuleTag object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Validation Functionality Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Validation Functionality Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseValidationFunctionalityModule(ValidationFunctionalityModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Parallel Choice Functionality Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Parallel Choice Functionality Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseParallelChoiceFunctionalityModule(ParallelChoiceFunctionalityModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseModule(org.eclipse.efbt.cocalimo.core.model.module_management.Module object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Tag</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Tag</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseTag(Tag object) {
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

} //Functionality_moduleSwitch
