/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.mapping.util;

import org.eclipse.efbt.cocalimo.smcubes.model.mapping.*;

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
 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingPackage
 * @generated
 */
public class MappingSwitch<T> extends Switch<T> {
	/**
	 * The cached model package
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static MappingPackage modelPackage;

	/**
	 * Creates an instance of the switch.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public MappingSwitch() {
		if (modelPackage == null) {
			modelPackage = MappingPackage.eINSTANCE;
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
			case MappingPackage.CUBE_MAPPING: {
				CUBE_MAPPING cubE_MAPPING = (CUBE_MAPPING)theEObject;
				T result = caseCUBE_MAPPING(cubE_MAPPING);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case MappingPackage.MEMBER_MAPPING: {
				MEMBER_MAPPING membeR_MAPPING = (MEMBER_MAPPING)theEObject;
				T result = caseMEMBER_MAPPING(membeR_MAPPING);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case MappingPackage.MEMBER_MAPPING_ITEM: {
				MEMBER_MAPPING_ITEM membeR_MAPPING_ITEM = (MEMBER_MAPPING_ITEM)theEObject;
				T result = caseMEMBER_MAPPING_ITEM(membeR_MAPPING_ITEM);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case MappingPackage.VARIABLE_MAPPING_ITEM: {
				VARIABLE_MAPPING_ITEM variablE_MAPPING_ITEM = (VARIABLE_MAPPING_ITEM)theEObject;
				T result = caseVARIABLE_MAPPING_ITEM(variablE_MAPPING_ITEM);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case MappingPackage.VARIABLE_MAPPING: {
				VARIABLE_MAPPING variablE_MAPPING = (VARIABLE_MAPPING)theEObject;
				T result = caseVARIABLE_MAPPING(variablE_MAPPING);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case MappingPackage.MAPPING_TO_CUBE: {
				MAPPING_TO_CUBE mappinG_TO_CUBE = (MAPPING_TO_CUBE)theEObject;
				T result = caseMAPPING_TO_CUBE(mappinG_TO_CUBE);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case MappingPackage.VARIABLE_SET_MAPPING: {
				VARIABLE_SET_MAPPING variablE_SET_MAPPING = (VARIABLE_SET_MAPPING)theEObject;
				T result = caseVARIABLE_SET_MAPPING(variablE_SET_MAPPING);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case MappingPackage.MAPPING_DEFINITION: {
				MAPPING_DEFINITION mappinG_DEFINITION = (MAPPING_DEFINITION)theEObject;
				T result = caseMAPPING_DEFINITION(mappinG_DEFINITION);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			default: return defaultCase(theEObject);
		}
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>CUBE MAPPING</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>CUBE MAPPING</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseCUBE_MAPPING(CUBE_MAPPING object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>MEMBER MAPPING</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>MEMBER MAPPING</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseMEMBER_MAPPING(MEMBER_MAPPING object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>MEMBER MAPPING ITEM</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>MEMBER MAPPING ITEM</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseMEMBER_MAPPING_ITEM(MEMBER_MAPPING_ITEM object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>VARIABLE MAPPING ITEM</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>VARIABLE MAPPING ITEM</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseVARIABLE_MAPPING_ITEM(VARIABLE_MAPPING_ITEM object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>VARIABLE MAPPING</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>VARIABLE MAPPING</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseVARIABLE_MAPPING(VARIABLE_MAPPING object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>MAPPING TO CUBE</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>MAPPING TO CUBE</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseMAPPING_TO_CUBE(MAPPING_TO_CUBE object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>VARIABLE SET MAPPING</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>VARIABLE SET MAPPING</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseVARIABLE_SET_MAPPING(VARIABLE_SET_MAPPING object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>MAPPING DEFINITION</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>MAPPING DEFINITION</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseMAPPING_DEFINITION(MAPPING_DEFINITION object) {
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

} //MappingSwitch
