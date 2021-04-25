/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.CubesPackage;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBIRDToolsCubeHierarchyRelationship;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBirdToolsCube;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Free BIRD Tools Cube Hierarchy Relationship</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl.FreeBIRDToolsCubeHierarchyRelationshipImpl#getSuperCube <em>Super Cube</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.impl.FreeBIRDToolsCubeHierarchyRelationshipImpl#getSubCube <em>Sub Cube</em>}</li>
 * </ul>
 *
 * @generated
 */
public class FreeBIRDToolsCubeHierarchyRelationshipImpl extends MinimalEObjectImpl.Container implements FreeBIRDToolsCubeHierarchyRelationship
{
	/**
	 * The cached value of the '{@link #getSuperCube() <em>Super Cube</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSuperCube()
	 * @generated
	 * @ordered
	 */
	protected FreeBirdToolsCube superCube;

	/**
	 * The cached value of the '{@link #getSubCube() <em>Sub Cube</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSubCube()
	 * @generated
	 * @ordered
	 */
	protected FreeBirdToolsCube subCube;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected FreeBIRDToolsCubeHierarchyRelationshipImpl()
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
		return CubesPackage.Literals.FREE_BIRD_TOOLS_CUBE_HIERARCHY_RELATIONSHIP;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public FreeBirdToolsCube getSuperCube()
	{
		if (superCube != null && superCube.eIsProxy()) {
			InternalEObject oldSuperCube = (InternalEObject)superCube;
			superCube = (FreeBirdToolsCube)eResolveProxy(oldSuperCube);
			if (superCube != oldSuperCube) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, CubesPackage.FREE_BIRD_TOOLS_CUBE_HIERARCHY_RELATIONSHIP__SUPER_CUBE, oldSuperCube, superCube));
			}
		}
		return superCube;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public FreeBirdToolsCube basicGetSuperCube()
	{
		return superCube;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setSuperCube(FreeBirdToolsCube newSuperCube)
	{
		FreeBirdToolsCube oldSuperCube = superCube;
		superCube = newSuperCube;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CubesPackage.FREE_BIRD_TOOLS_CUBE_HIERARCHY_RELATIONSHIP__SUPER_CUBE, oldSuperCube, superCube));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public FreeBirdToolsCube getSubCube()
	{
		if (subCube != null && subCube.eIsProxy()) {
			InternalEObject oldSubCube = (InternalEObject)subCube;
			subCube = (FreeBirdToolsCube)eResolveProxy(oldSubCube);
			if (subCube != oldSubCube) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, CubesPackage.FREE_BIRD_TOOLS_CUBE_HIERARCHY_RELATIONSHIP__SUB_CUBE, oldSubCube, subCube));
			}
		}
		return subCube;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public FreeBirdToolsCube basicGetSubCube()
	{
		return subCube;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setSubCube(FreeBirdToolsCube newSubCube)
	{
		FreeBirdToolsCube oldSubCube = subCube;
		subCube = newSubCube;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CubesPackage.FREE_BIRD_TOOLS_CUBE_HIERARCHY_RELATIONSHIP__SUB_CUBE, oldSubCube, subCube));
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
			case CubesPackage.FREE_BIRD_TOOLS_CUBE_HIERARCHY_RELATIONSHIP__SUPER_CUBE:
				if (resolve) return getSuperCube();
				return basicGetSuperCube();
			case CubesPackage.FREE_BIRD_TOOLS_CUBE_HIERARCHY_RELATIONSHIP__SUB_CUBE:
				if (resolve) return getSubCube();
				return basicGetSubCube();
		}
		return super.eGet(featureID, resolve, coreType);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void eSet(int featureID, Object newValue)
	{
		switch (featureID) {
			case CubesPackage.FREE_BIRD_TOOLS_CUBE_HIERARCHY_RELATIONSHIP__SUPER_CUBE:
				setSuperCube((FreeBirdToolsCube)newValue);
				return;
			case CubesPackage.FREE_BIRD_TOOLS_CUBE_HIERARCHY_RELATIONSHIP__SUB_CUBE:
				setSubCube((FreeBirdToolsCube)newValue);
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
			case CubesPackage.FREE_BIRD_TOOLS_CUBE_HIERARCHY_RELATIONSHIP__SUPER_CUBE:
				setSuperCube((FreeBirdToolsCube)null);
				return;
			case CubesPackage.FREE_BIRD_TOOLS_CUBE_HIERARCHY_RELATIONSHIP__SUB_CUBE:
				setSubCube((FreeBirdToolsCube)null);
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
			case CubesPackage.FREE_BIRD_TOOLS_CUBE_HIERARCHY_RELATIONSHIP__SUPER_CUBE:
				return superCube != null;
			case CubesPackage.FREE_BIRD_TOOLS_CUBE_HIERARCHY_RELATIONSHIP__SUB_CUBE:
				return subCube != null;
		}
		return super.eIsSet(featureID);
	}

} //FreeBIRDToolsCubeHierarchyRelationshipImpl
