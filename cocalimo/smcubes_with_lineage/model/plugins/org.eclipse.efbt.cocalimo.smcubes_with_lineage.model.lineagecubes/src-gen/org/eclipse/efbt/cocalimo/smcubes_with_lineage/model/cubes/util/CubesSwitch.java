/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.util;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.*;

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
 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.CubesPackage
 * @generated
 */
public class CubesSwitch<T> extends Switch<T>
{
	/**
	 * The cached model package
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static CubesPackage modelPackage;

	/**
	 * Creates an instance of the switch.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CubesSwitch()
	{
		if (modelPackage == null) {
			modelPackage = CubesPackage.eINSTANCE;
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
			case CubesPackage.BASE_CUBE: {
				BaseCube baseCube = (BaseCube)theEObject;
				T result = caseBaseCube(baseCube);
				if (result == null) result = caseFreeBirdToolsCube(baseCube);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case CubesPackage.DERIVED_CUBE: {
				DerivedCube derivedCube = (DerivedCube)theEObject;
				T result = caseDerivedCube(derivedCube);
				if (result == null) result = caseFreeBirdToolsCube(derivedCube);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case CubesPackage.FREE_BIRD_TOOLS_CUBE: {
				FreeBirdToolsCube freeBirdToolsCube = (FreeBirdToolsCube)theEObject;
				T result = caseFreeBirdToolsCube(freeBirdToolsCube);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case CubesPackage.FREE_BIRD_TOOLS_CUBE_MODULE: {
				FreeBirdToolsCubeModule freeBirdToolsCubeModule = (FreeBirdToolsCubeModule)theEObject;
				T result = caseFreeBirdToolsCubeModule(freeBirdToolsCubeModule);
				if (result == null) result = caseModule(freeBirdToolsCubeModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case CubesPackage.TARGET_CUBE: {
				TargetCube targetCube = (TargetCube)theEObject;
				T result = caseTargetCube(targetCube);
				if (result == null) result = caseDerivedCube(targetCube);
				if (result == null) result = caseFreeBirdToolsCube(targetCube);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case CubesPackage.FREE_BIRD_TOOLS_CUBE_HIERARCHY_RELATIONSHIP_MODULE: {
				FreeBIRDToolsCubeHierarchyRelationshipModule freeBIRDToolsCubeHierarchyRelationshipModule = (FreeBIRDToolsCubeHierarchyRelationshipModule)theEObject;
				T result = caseFreeBIRDToolsCubeHierarchyRelationshipModule(freeBIRDToolsCubeHierarchyRelationshipModule);
				if (result == null) result = caseModule(freeBIRDToolsCubeHierarchyRelationshipModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case CubesPackage.FREE_BIRD_TOOLS_CUBE_HIERARCHY_RELATIONSHIP: {
				FreeBIRDToolsCubeHierarchyRelationship freeBIRDToolsCubeHierarchyRelationship = (FreeBIRDToolsCubeHierarchyRelationship)theEObject;
				T result = caseFreeBIRDToolsCubeHierarchyRelationship(freeBIRDToolsCubeHierarchyRelationship);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			default: return defaultCase(theEObject);
		}
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
	 * Returns the result of interpreting the object as an instance of '<em>Free Bird Tools Cube Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Free Bird Tools Cube Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseFreeBirdToolsCubeModule(FreeBirdToolsCubeModule object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Target Cube</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Target Cube</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseTargetCube(TargetCube object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Free BIRD Tools Cube Hierarchy Relationship Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Free BIRD Tools Cube Hierarchy Relationship Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseFreeBIRDToolsCubeHierarchyRelationshipModule(FreeBIRDToolsCubeHierarchyRelationshipModule object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Free BIRD Tools Cube Hierarchy Relationship</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Free BIRD Tools Cube Hierarchy Relationship</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseFreeBIRDToolsCubeHierarchyRelationship(FreeBIRDToolsCubeHierarchyRelationship object)
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
	public T caseModule(org.eclipse.efbt.cocalimo.core.model.module_management.Module object)
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

} //CubesSwitch
