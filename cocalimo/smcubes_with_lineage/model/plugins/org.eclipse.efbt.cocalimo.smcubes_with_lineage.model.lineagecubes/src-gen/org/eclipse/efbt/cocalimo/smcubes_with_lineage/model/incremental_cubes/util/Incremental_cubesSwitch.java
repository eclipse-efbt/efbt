/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.util;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.BaseCube;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.DerivedCube;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBirdToolsCube;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.*;

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
 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.Incremental_cubesPackage
 * @generated
 */
public class Incremental_cubesSwitch<T> extends Switch<T>
{
	/**
	 * The cached model package
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static Incremental_cubesPackage modelPackage;

	/**
	 * Creates an instance of the switch.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Incremental_cubesSwitch()
	{
		if (modelPackage == null) {
			modelPackage = Incremental_cubesPackage.eINSTANCE;
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
			case Incremental_cubesPackage.BASE_DELTA_CUBE: {
				BaseDeltaCube baseDeltaCube = (BaseDeltaCube)theEObject;
				T result = caseBaseDeltaCube(baseDeltaCube);
				if (result == null) result = caseBaseCube(baseDeltaCube);
				if (result == null) result = caseFreeBirdToolsCube(baseDeltaCube);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Incremental_cubesPackage.BASE_VIEW_CUBE: {
				BaseViewCube baseViewCube = (BaseViewCube)theEObject;
				T result = caseBaseViewCube(baseViewCube);
				if (result == null) result = caseDerivedCube(baseViewCube);
				if (result == null) result = caseFreeBirdToolsCube(baseViewCube);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Incremental_cubesPackage.DELTA_ACCUMULATION_CUBE: {
				DeltaAccumulationCube deltaAccumulationCube = (DeltaAccumulationCube)theEObject;
				T result = caseDeltaAccumulationCube(deltaAccumulationCube);
				if (result == null) result = caseDerivedCube(deltaAccumulationCube);
				if (result == null) result = caseFreeBirdToolsCube(deltaAccumulationCube);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			default: return defaultCase(theEObject);
		}
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Base Delta Cube</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Base Delta Cube</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseBaseDeltaCube(BaseDeltaCube object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Base View Cube</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Base View Cube</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseBaseViewCube(BaseViewCube object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Delta Accumulation Cube</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Delta Accumulation Cube</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseDeltaAccumulationCube(DeltaAccumulationCube object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Free Bird Tools Cube</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Free Bird Tools Cube</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseFreeBirdToolsCube(FreeBirdToolsCube object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Base Cube</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Base Cube</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseBaseCube(BaseCube object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Derived Cube</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Derived Cube</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseDerivedCube(DerivedCube object)
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

} //Incremental_cubesSwitch
