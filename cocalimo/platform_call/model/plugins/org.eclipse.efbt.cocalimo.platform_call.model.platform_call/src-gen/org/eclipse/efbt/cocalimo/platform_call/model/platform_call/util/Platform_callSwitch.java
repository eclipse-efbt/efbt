/**
 */
package org.eclipse.efbt.cocalimo.platform_call.model.platform_call.util;

import org.eclipse.efbt.cocalimo.platform_call.model.platform_call.*;

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
 * @see org.eclipse.efbt.cocalimo.platform_call.model.platform_call.Platform_callPackage
 * @generated
 */
public class Platform_callSwitch<T> extends Switch<T> {
	/**
	 * The cached model package
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static Platform_callPackage modelPackage;

	/**
	 * Creates an instance of the switch.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Platform_callSwitch() {
		if (modelPackage == null) {
			modelPackage = Platform_callPackage.eINSTANCE;
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
			case Platform_callPackage.PLATFORM_CALL: {
				PlatformCall platformCall = (PlatformCall)theEObject;
				T result = casePlatformCall(platformCall);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Platform_callPackage.PLATFORM_CALL_MODULE: {
				PlatformCallModule platformCallModule = (PlatformCallModule)theEObject;
				T result = casePlatformCallModule(platformCallModule);
				if (result == null) result = caseModule(platformCallModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Platform_callPackage.IMPORT_BIRD_FROM_MS_ACCESS: {
				ImportBIRDFromMSAccess importBIRDFromMSAccess = (ImportBIRDFromMSAccess)theEObject;
				T result = caseImportBIRDFromMSAccess(importBIRDFromMSAccess);
				if (result == null) result = casePlatformCall(importBIRDFromMSAccess);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Platform_callPackage.CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE: {
				CreateLogicalTransformationViewForScope createLogicalTransformationViewForScope = (CreateLogicalTransformationViewForScope)theEObject;
				T result = caseCreateLogicalTransformationViewForScope(createLogicalTransformationViewForScope);
				if (result == null) result = casePlatformCall(createLogicalTransformationViewForScope);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Platform_callPackage.CONVERT_DATA_META_MODEL_TO_ECORE: {
				ConvertDataMetaModelToEcore convertDataMetaModelToEcore = (ConvertDataMetaModelToEcore)theEObject;
				T result = caseConvertDataMetaModelToEcore(convertDataMetaModelToEcore);
				if (result == null) result = casePlatformCall(convertDataMetaModelToEcore);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			default: return defaultCase(theEObject);
		}
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Platform Call</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Platform Call</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T casePlatformCall(PlatformCall object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Platform Call Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Platform Call Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T casePlatformCallModule(PlatformCallModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Import BIRD From MS Access</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Import BIRD From MS Access</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseImportBIRDFromMSAccess(ImportBIRDFromMSAccess object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Create Logical Transformation View For Scope</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Create Logical Transformation View For Scope</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseCreateLogicalTransformationViewForScope(CreateLogicalTransformationViewForScope object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Convert Data Meta Model To Ecore</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Convert Data Meta Model To Ecore</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseConvertDataMetaModelToEcore(ConvertDataMetaModelToEcore object) {
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

} //Platform_callSwitch
