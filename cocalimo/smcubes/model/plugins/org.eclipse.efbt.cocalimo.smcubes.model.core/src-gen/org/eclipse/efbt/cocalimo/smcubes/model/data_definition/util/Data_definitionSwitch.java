/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.data_definition.util;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.*;

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
 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_definition.Data_definitionPackage
 * @generated
 */
public class Data_definitionSwitch<T> extends Switch<T>
{
	/**
	 * The cached model package
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static Data_definitionPackage modelPackage;

	/**
	 * Creates an instance of the switch.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Data_definitionSwitch()
	{
		if (modelPackage == null) {
			modelPackage = Data_definitionPackage.eINSTANCE;
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
	protected boolean isSwitchFor(EPackage ePackage)
	{
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
	protected T doSwitch(int classifierID, EObject theEObject)
	{
		switch (classifierID) {
			case Data_definitionPackage.COMBINATION: {
				COMBINATION combination = (COMBINATION)theEObject;
				T result = caseCOMBINATION(combination);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Data_definitionPackage.COMBINATION_ITEM: {
				COMBINATION_ITEM combinatioN_ITEM = (COMBINATION_ITEM)theEObject;
				T result = caseCOMBINATION_ITEM(combinatioN_ITEM);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Data_definitionPackage.CUBE: {
				CUBE cube = (CUBE)theEObject;
				T result = caseCUBE(cube);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Data_definitionPackage.CUBE_GROUP: {
				CUBE_GROUP cubE_GROUP = (CUBE_GROUP)theEObject;
				T result = caseCUBE_GROUP(cubE_GROUP);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Data_definitionPackage.CUBE_GROUP_ENUMERATION: {
				CUBE_GROUP_ENUMERATION cubE_GROUP_ENUMERATION = (CUBE_GROUP_ENUMERATION)theEObject;
				T result = caseCUBE_GROUP_ENUMERATION(cubE_GROUP_ENUMERATION);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Data_definitionPackage.CUBE_HIERARCHY: {
				CUBE_HIERARCHY cubE_HIERARCHY = (CUBE_HIERARCHY)theEObject;
				T result = caseCUBE_HIERARCHY(cubE_HIERARCHY);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Data_definitionPackage.CUBE_HIERARCHY_NODE: {
				CUBE_HIERARCHY_NODE cubE_HIERARCHY_NODE = (CUBE_HIERARCHY_NODE)theEObject;
				T result = caseCUBE_HIERARCHY_NODE(cubE_HIERARCHY_NODE);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Data_definitionPackage.CUBE_RELATIONSHIP: {
				CUBE_RELATIONSHIP cubE_RELATIONSHIP = (CUBE_RELATIONSHIP)theEObject;
				T result = caseCUBE_RELATIONSHIP(cubE_RELATIONSHIP);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Data_definitionPackage.CUBE_STRUCTURE: {
				CUBE_STRUCTURE cubE_STRUCTURE = (CUBE_STRUCTURE)theEObject;
				T result = caseCUBE_STRUCTURE(cubE_STRUCTURE);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM: {
				CUBE_STRUCTURE_ITEM cubE_STRUCTURE_ITEM = (CUBE_STRUCTURE_ITEM)theEObject;
				T result = caseCUBE_STRUCTURE_ITEM(cubE_STRUCTURE_ITEM);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Data_definitionPackage.CUBE_TO_COMBINATION: {
				CUBE_TO_COMBINATION cubE_TO_COMBINATION = (CUBE_TO_COMBINATION)theEObject;
				T result = caseCUBE_TO_COMBINATION(cubE_TO_COMBINATION);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Data_definitionPackage.FRAMEWORK: {
				FRAMEWORK framework = (FRAMEWORK)theEObject;
				T result = caseFRAMEWORK(framework);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Data_definitionPackage.FRAMEWORK_SUBDOMAIN: {
				FRAMEWORK_SUBDOMAIN frameworK_SUBDOMAIN = (FRAMEWORK_SUBDOMAIN)theEObject;
				T result = caseFRAMEWORK_SUBDOMAIN(frameworK_SUBDOMAIN);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Data_definitionPackage.FRAMEWORK_VARIABLE_SET: {
				FRAMEWORK_VARIABLE_SET frameworK_VARIABLE_SET = (FRAMEWORK_VARIABLE_SET)theEObject;
				T result = caseFRAMEWORK_VARIABLE_SET(frameworK_VARIABLE_SET);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			default: return defaultCase(theEObject);
		}
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>COMBINATION</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>COMBINATION</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseCOMBINATION(COMBINATION object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>COMBINATION ITEM</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>COMBINATION ITEM</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseCOMBINATION_ITEM(COMBINATION_ITEM object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>CUBE</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>CUBE</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseCUBE(CUBE object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>CUBE GROUP</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>CUBE GROUP</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseCUBE_GROUP(CUBE_GROUP object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>CUBE GROUP ENUMERATION</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>CUBE GROUP ENUMERATION</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseCUBE_GROUP_ENUMERATION(CUBE_GROUP_ENUMERATION object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>CUBE HIERARCHY</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>CUBE HIERARCHY</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseCUBE_HIERARCHY(CUBE_HIERARCHY object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>CUBE HIERARCHY NODE</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>CUBE HIERARCHY NODE</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseCUBE_HIERARCHY_NODE(CUBE_HIERARCHY_NODE object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>CUBE RELATIONSHIP</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>CUBE RELATIONSHIP</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseCUBE_RELATIONSHIP(CUBE_RELATIONSHIP object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>CUBE STRUCTURE</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>CUBE STRUCTURE</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseCUBE_STRUCTURE(CUBE_STRUCTURE object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>CUBE STRUCTURE ITEM</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>CUBE STRUCTURE ITEM</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseCUBE_STRUCTURE_ITEM(CUBE_STRUCTURE_ITEM object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>CUBE TO COMBINATION</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>CUBE TO COMBINATION</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseCUBE_TO_COMBINATION(CUBE_TO_COMBINATION object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>FRAMEWORK</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>FRAMEWORK</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseFRAMEWORK(FRAMEWORK object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>FRAMEWORK SUBDOMAIN</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>FRAMEWORK SUBDOMAIN</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseFRAMEWORK_SUBDOMAIN(FRAMEWORK_SUBDOMAIN object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>FRAMEWORK VARIABLE SET</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>FRAMEWORK VARIABLE SET</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseFRAMEWORK_VARIABLE_SET(FRAMEWORK_VARIABLE_SET object)
	{
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
	public T defaultCase(EObject object)
	{
		return null;
	}

} //Data_definitionSwitch
