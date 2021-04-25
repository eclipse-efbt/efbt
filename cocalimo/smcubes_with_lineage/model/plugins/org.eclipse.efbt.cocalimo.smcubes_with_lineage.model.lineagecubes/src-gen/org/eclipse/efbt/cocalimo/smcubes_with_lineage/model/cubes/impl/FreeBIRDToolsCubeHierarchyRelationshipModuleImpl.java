/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.core.model.module_management.impl.ModuleImpl;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.CubesPackage;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBIRDToolsCubeHierarchyRelationship;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBIRDToolsCubeHierarchyRelationshipModule;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Free BIRD Tools Cube Hierarchy Relationship Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl.FreeBIRDToolsCubeHierarchyRelationshipModuleImpl#getHierarchyRelationships <em>Hierarchy Relationships</em>}</li>
 * </ul>
 *
 * @generated
 */
public class FreeBIRDToolsCubeHierarchyRelationshipModuleImpl extends ModuleImpl implements FreeBIRDToolsCubeHierarchyRelationshipModule
{
	/**
	 * The cached value of the '{@link #getHierarchyRelationships() <em>Hierarchy Relationships</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getHierarchyRelationships()
	 * @generated
	 * @ordered
	 */
	protected EList<FreeBIRDToolsCubeHierarchyRelationship> hierarchyRelationships;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected FreeBIRDToolsCubeHierarchyRelationshipModuleImpl()
	{
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass()
	{
		return CubesPackage.Literals.FREE_BIRD_TOOLS_CUBE_HIERARCHY_RELATIONSHIP_MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<FreeBIRDToolsCubeHierarchyRelationship> getHierarchyRelationships()
	{
		if (hierarchyRelationships == null) {
			hierarchyRelationships = new EObjectContainmentEList<FreeBIRDToolsCubeHierarchyRelationship>(FreeBIRDToolsCubeHierarchyRelationship.class, this, CubesPackage.FREE_BIRD_TOOLS_CUBE_HIERARCHY_RELATIONSHIP_MODULE__HIERARCHY_RELATIONSHIPS);
		}
		return hierarchyRelationships;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs)
	{
		switch (featureID) {
			case CubesPackage.FREE_BIRD_TOOLS_CUBE_HIERARCHY_RELATIONSHIP_MODULE__HIERARCHY_RELATIONSHIPS:
				return ((InternalEList<?>)getHierarchyRelationships()).basicRemove(otherEnd, msgs);
		}
		return super.eInverseRemove(otherEnd, featureID, msgs);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType)
	{
		switch (featureID) {
			case CubesPackage.FREE_BIRD_TOOLS_CUBE_HIERARCHY_RELATIONSHIP_MODULE__HIERARCHY_RELATIONSHIPS:
				return getHierarchyRelationships();
		}
		return super.eGet(featureID, resolve, coreType);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void eSet(int featureID, Object newValue)
	{
		switch (featureID) {
			case CubesPackage.FREE_BIRD_TOOLS_CUBE_HIERARCHY_RELATIONSHIP_MODULE__HIERARCHY_RELATIONSHIPS:
				getHierarchyRelationships().clear();
				getHierarchyRelationships().addAll((Collection<? extends FreeBIRDToolsCubeHierarchyRelationship>)newValue);
				return;
		}
		super.eSet(featureID, newValue);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void eUnset(int featureID)
	{
		switch (featureID) {
			case CubesPackage.FREE_BIRD_TOOLS_CUBE_HIERARCHY_RELATIONSHIP_MODULE__HIERARCHY_RELATIONSHIPS:
				getHierarchyRelationships().clear();
				return;
		}
		super.eUnset(featureID);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public boolean eIsSet(int featureID)
	{
		switch (featureID) {
			case CubesPackage.FREE_BIRD_TOOLS_CUBE_HIERARCHY_RELATIONSHIP_MODULE__HIERARCHY_RELATIONSHIPS:
				return hierarchyRelationships != null && !hierarchyRelationships.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //FreeBIRDToolsCubeHierarchyRelationshipModuleImpl
