/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Free BIRD Tools Cube Hierarchy Relationship Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBIRDToolsCubeHierarchyRelationshipModule#getHierarchyRelationships <em>Hierarchy Relationships</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.CubesPackage#getFreeBIRDToolsCubeHierarchyRelationshipModule()
 * @model
 * @generated
 */
public interface FreeBIRDToolsCubeHierarchyRelationshipModule extends org.eclipse.efbt.cocalimo.core.model.module_management.Module
{
	/**
	 * Returns the value of the '<em><b>Hierarchy Relationships</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBIRDToolsCubeHierarchyRelationship}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Hierarchy Relationships</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.CubesPackage#getFreeBIRDToolsCubeHierarchyRelationshipModule_HierarchyRelationships()
	 * @model containment="true"
	 * @generated
	 */
	EList<FreeBIRDToolsCubeHierarchyRelationship> getHierarchyRelationships();

} // FreeBIRDToolsCubeHierarchyRelationshipModule
