/**
 */
package org.eclipse.efbt.cocamo.smcubes.model.efbt_mapping.util;

import org.eclipse.efbt.cocamo.smcubes.model.efbt_mapping.*;

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
 * @see org.eclipse.efbt.cocamo.smcubes.model.efbt_mapping.Efbt_mappingPackage
 * @generated
 */
public class Efbt_mappingSwitch<T> extends Switch<T>
{
	/**
	 * The cached model package
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static Efbt_mappingPackage modelPackage;

	/**
	 * Creates an instance of the switch.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Efbt_mappingSwitch()
	{
		if (modelPackage == null)
		{
			modelPackage = Efbt_mappingPackage.eINSTANCE;
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
		switch (classifierID)
		{
			case Efbt_mappingPackage.MAPPING_DEFINITION_MODULE:
			{
				MappingDefinitionModule mappingDefinitionModule = (MappingDefinitionModule)theEObject;
				T result = caseMappingDefinitionModule(mappingDefinitionModule);
				if (result == null) result = caseModule(mappingDefinitionModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Efbt_mappingPackage.CUBE_MAPPING_MODULE:
			{
				CubeMappingModule cubeMappingModule = (CubeMappingModule)theEObject;
				T result = caseCubeMappingModule(cubeMappingModule);
				if (result == null) result = caseModule(cubeMappingModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Efbt_mappingPackage.MEMBER_MAPPING_MODULE:
			{
				MemberMappingModule memberMappingModule = (MemberMappingModule)theEObject;
				T result = caseMemberMappingModule(memberMappingModule);
				if (result == null) result = caseModule(memberMappingModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Efbt_mappingPackage.VARIABLE_MAPPING_MODULE:
			{
				VariableMappingModule variableMappingModule = (VariableMappingModule)theEObject;
				T result = caseVariableMappingModule(variableMappingModule);
				if (result == null) result = caseModule(variableMappingModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			default: return defaultCase(theEObject);
		}
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Mapping Definition Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Mapping Definition Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseMappingDefinitionModule(MappingDefinitionModule object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Cube Mapping Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Cube Mapping Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseCubeMappingModule(CubeMappingModule object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Member Mapping Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Member Mapping Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseMemberMappingModule(MemberMappingModule object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Variable Mapping Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Variable Mapping Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseVariableMappingModule(VariableMappingModule object)
	{
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
	public T caseModule(org.eclipse.efbt.cocamo.core.model.module_management.Module object)
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

} //Efbt_mappingSwitch
