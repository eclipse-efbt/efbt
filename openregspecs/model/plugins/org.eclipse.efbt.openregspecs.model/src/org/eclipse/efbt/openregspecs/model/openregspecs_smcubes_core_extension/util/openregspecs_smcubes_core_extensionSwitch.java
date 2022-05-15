/**
 */
package org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.util;

import org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.*;

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
 * @see org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.openregspecs_smcubes_core_extensionPackage
 * @generated
 */
public class openregspecs_smcubes_core_extensionSwitch<T> extends Switch<T> {
	/**
	 * The cached model package
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static openregspecs_smcubes_core_extensionPackage modelPackage;

	/**
	 * Creates an instance of the switch.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public openregspecs_smcubes_core_extensionSwitch() {
		if (modelPackage == null) {
			modelPackage = openregspecs_smcubes_core_extensionPackage.eINSTANCE;
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
			case openregspecs_smcubes_core_extensionPackage.DOMAIN_MODULE: {
				DomainModule domainModule = (DomainModule)theEObject;
				T result = caseDomainModule(domainModule);
				if (result == null) result = caseModule(domainModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case openregspecs_smcubes_core_extensionPackage.MEMBER_HIERARCHY_MODULE: {
				MemberHierarchyModule memberHierarchyModule = (MemberHierarchyModule)theEObject;
				T result = caseMemberHierarchyModule(memberHierarchyModule);
				if (result == null) result = caseModule(memberHierarchyModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case openregspecs_smcubes_core_extensionPackage.MEMBER_MODULE: {
				MemberModule memberModule = (MemberModule)theEObject;
				T result = caseMemberModule(memberModule);
				if (result == null) result = caseModule(memberModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case openregspecs_smcubes_core_extensionPackage.VARIABLE_MODULE: {
				VariableModule variableModule = (VariableModule)theEObject;
				T result = caseVariableModule(variableModule);
				if (result == null) result = caseModule(variableModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case openregspecs_smcubes_core_extensionPackage.SUB_DOMAIN_MODULE: {
				SubDomainModule subDomainModule = (SubDomainModule)theEObject;
				T result = caseSubDomainModule(subDomainModule);
				if (result == null) result = caseModule(subDomainModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case openregspecs_smcubes_core_extensionPackage.SM_CUBES_CORE_MODEL: {
				SMCubesCoreModel smCubesCoreModel = (SMCubesCoreModel)theEObject;
				T result = caseSMCubesCoreModel(smCubesCoreModel);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			default: return defaultCase(theEObject);
		}
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Domain Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Domain Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseDomainModule(DomainModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Member Hierarchy Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Member Hierarchy Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseMemberHierarchyModule(MemberHierarchyModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Member Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Member Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseMemberModule(MemberModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Variable Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Variable Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseVariableModule(VariableModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Sub Domain Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Sub Domain Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseSubDomainModule(SubDomainModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>SM Cubes Core Model</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>SM Cubes Core Model</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseSMCubesCoreModel(SMCubesCoreModel object) {
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
	public T caseModule(org.eclipse.efbt.openregspecs.model.module_management.Module object) {
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

} //openregspecs_smcubes_core_extensionSwitch
