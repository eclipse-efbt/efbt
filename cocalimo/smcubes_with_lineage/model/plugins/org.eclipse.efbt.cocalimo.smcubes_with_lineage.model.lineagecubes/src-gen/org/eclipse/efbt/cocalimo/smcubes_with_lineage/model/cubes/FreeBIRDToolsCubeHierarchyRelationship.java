/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Free BIRD Tools Cube Hierarchy Relationship</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBIRDToolsCubeHierarchyRelationship#getSuperCube <em>Super Cube</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBIRDToolsCubeHierarchyRelationship#getSubCube <em>Sub Cube</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.CubesPackage#getFreeBIRDToolsCubeHierarchyRelationship()
 * @model
 * @generated
 */
public interface FreeBIRDToolsCubeHierarchyRelationship extends EObject
{
	/**
	 * Returns the value of the '<em><b>Super Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Super Cube</em>' reference.
	 * @see #setSuperCube(FreeBirdToolsCube)
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.CubesPackage#getFreeBIRDToolsCubeHierarchyRelationship_SuperCube()
	 * @model
	 * @generated
	 */
	FreeBirdToolsCube getSuperCube();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBIRDToolsCubeHierarchyRelationship#getSuperCube <em>Super Cube</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Super Cube</em>' reference.
	 * @see #getSuperCube()
	 * @generated
	 */
	void setSuperCube(FreeBirdToolsCube value);

	/**
	 * Returns the value of the '<em><b>Sub Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Sub Cube</em>' reference.
	 * @see #setSubCube(FreeBirdToolsCube)
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.CubesPackage#getFreeBIRDToolsCubeHierarchyRelationship_SubCube()
	 * @model
	 * @generated
	 */
	FreeBirdToolsCube getSubCube();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBIRDToolsCubeHierarchyRelationship#getSubCube <em>Sub Cube</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Sub Cube</em>' reference.
	 * @see #getSubCube()
	 * @generated
	 */
	void setSubCube(FreeBirdToolsCube value);

} // FreeBIRDToolsCubeHierarchyRelationship
