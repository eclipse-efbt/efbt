/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.core.util;

import org.eclipse.efbt.cocalimo.smcubes.model.core.*;

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
 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage
 * @generated
 */
public class CoreSwitch<T> extends Switch<T>
{
	/**
	 * The cached model package
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static CorePackage modelPackage;

	/**
	 * Creates an instance of the switch.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CoreSwitch()
	{
		if (modelPackage == null) {
			modelPackage = CorePackage.eINSTANCE;
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
			case CorePackage.DOMAIN: {
				DOMAIN domain = (DOMAIN)theEObject;
				T result = caseDOMAIN(domain);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case CorePackage.FACET_COLLECTION: {
				FACET_COLLECTION faceT_COLLECTION = (FACET_COLLECTION)theEObject;
				T result = caseFACET_COLLECTION(faceT_COLLECTION);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case CorePackage.FACET_ENUMERATION: {
				FACET_ENUMERATION faceT_ENUMERATION = (FACET_ENUMERATION)theEObject;
				T result = caseFACET_ENUMERATION(faceT_ENUMERATION);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case CorePackage.FACET_TYPE: {
				facet_type facet_type = (facet_type)theEObject;
				T result = casefacet_type(facet_type);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case CorePackage.MAINTENANCE_AGENCY: {
				MAINTENANCE_AGENCY maintenancE_AGENCY = (MAINTENANCE_AGENCY)theEObject;
				T result = caseMAINTENANCE_AGENCY(maintenancE_AGENCY);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case CorePackage.MEMBER: {
				MEMBER member = (MEMBER)theEObject;
				T result = caseMEMBER(member);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case CorePackage.MEMBER_HIERARCHY: {
				MEMBER_HIERARCHY membeR_HIERARCHY = (MEMBER_HIERARCHY)theEObject;
				T result = caseMEMBER_HIERARCHY(membeR_HIERARCHY);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case CorePackage.MEMBER_HIERARCHY_NODE: {
				MEMBER_HIERARCHY_NODE membeR_HIERARCHY_NODE = (MEMBER_HIERARCHY_NODE)theEObject;
				T result = caseMEMBER_HIERARCHY_NODE(membeR_HIERARCHY_NODE);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case CorePackage.SUBDOMAIN: {
				SUBDOMAIN subdomain = (SUBDOMAIN)theEObject;
				T result = caseSUBDOMAIN(subdomain);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case CorePackage.SUBDOMAIN_ENUMERATION: {
				SUBDOMAIN_ENUMERATION subdomaiN_ENUMERATION = (SUBDOMAIN_ENUMERATION)theEObject;
				T result = caseSUBDOMAIN_ENUMERATION(subdomaiN_ENUMERATION);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case CorePackage.VARIABLE: {
				VARIABLE variable = (VARIABLE)theEObject;
				T result = caseVARIABLE(variable);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case CorePackage.VARIABLE_SET: {
				VARIABLE_SET variablE_SET = (VARIABLE_SET)theEObject;
				T result = caseVARIABLE_SET(variablE_SET);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case CorePackage.VARIABLE_SET_ENUMERATION: {
				VARIABLE_SET_ENUMERATION variablE_SET_ENUMERATION = (VARIABLE_SET_ENUMERATION)theEObject;
				T result = caseVARIABLE_SET_ENUMERATION(variablE_SET_ENUMERATION);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			default: return defaultCase(theEObject);
		}
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>DOMAIN</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>DOMAIN</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseDOMAIN(DOMAIN object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>FACET COLLECTION</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>FACET COLLECTION</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseFACET_COLLECTION(FACET_COLLECTION object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>FACET ENUMERATION</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>FACET ENUMERATION</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseFACET_ENUMERATION(FACET_ENUMERATION object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>facet type</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>facet type</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T casefacet_type(facet_type object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>MAINTENANCE AGENCY</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>MAINTENANCE AGENCY</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseMAINTENANCE_AGENCY(MAINTENANCE_AGENCY object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>MEMBER</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>MEMBER</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseMEMBER(MEMBER object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>MEMBER HIERARCHY</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>MEMBER HIERARCHY</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseMEMBER_HIERARCHY(MEMBER_HIERARCHY object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>MEMBER HIERARCHY NODE</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>MEMBER HIERARCHY NODE</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseMEMBER_HIERARCHY_NODE(MEMBER_HIERARCHY_NODE object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>SUBDOMAIN</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>SUBDOMAIN</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseSUBDOMAIN(SUBDOMAIN object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>SUBDOMAIN ENUMERATION</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>SUBDOMAIN ENUMERATION</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseSUBDOMAIN_ENUMERATION(SUBDOMAIN_ENUMERATION object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>VARIABLE</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>VARIABLE</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseVARIABLE(VARIABLE object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>VARIABLE SET</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>VARIABLE SET</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseVARIABLE_SET(VARIABLE_SET object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>VARIABLE SET ENUMERATION</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>VARIABLE SET ENUMERATION</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseVARIABLE_SET_ENUMERATION(VARIABLE_SET_ENUMERATION object)
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

} //CoreSwitch
