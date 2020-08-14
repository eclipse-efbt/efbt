/**
 */
package org.eclipse.efbt.lineage.common.model.cube_schema.impl;

import org.eclipse.efbt.data_structures.smcubes.model.data_definition.CUBE;

import org.eclipse.efbt.lineage.common.model.cube_schema.CubeSchema;
import org.eclipse.efbt.lineage.common.model.cube_schema.Cube_schemaPackage;

import org.eclipse.efbt.lineage.common.model.cubes.BaseCube;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Cube Schema</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.lineage.common.model.cube_schema.impl.CubeSchemaImpl#getCube <em>Cube</em>}</li>
 *   <li>{@link org.eclipse.efbt.lineage.common.model.cube_schema.impl.CubeSchemaImpl#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.lineage.common.model.cube_schema.impl.CubeSchemaImpl#getBaseCube <em>Base Cube</em>}</li>
 * </ul>
 *
 * @generated
 */
public class CubeSchemaImpl extends MinimalEObjectImpl.Container implements CubeSchema
{
	/**
	 * The cached value of the '{@link #getCube() <em>Cube</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCube()
	 * @generated
	 * @ordered
	 */
	protected CUBE cube;

	/**
	 * The default value of the '{@link #getName() <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getName()
	 * @generated
	 * @ordered
	 */
	protected static final String NAME_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getName() <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getName()
	 * @generated
	 * @ordered
	 */
	protected String name = NAME_EDEFAULT;

	/**
	 * The cached value of the '{@link #getBaseCube() <em>Base Cube</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getBaseCube()
	 * @generated
	 * @ordered
	 */
	protected BaseCube baseCube;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected CubeSchemaImpl()
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
		return Cube_schemaPackage.Literals.CUBE_SCHEMA;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public CUBE getCube()
	{
		if (cube != null && cube.eIsProxy())
		{
			InternalEObject oldCube = (InternalEObject)cube;
			cube = (CUBE)eResolveProxy(oldCube);
			if (cube != oldCube)
			{
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Cube_schemaPackage.CUBE_SCHEMA__CUBE, oldCube, cube));
			}
		}
		return cube;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CUBE basicGetCube()
	{
		return cube;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setCube(CUBE newCube)
	{
		CUBE oldCube = cube;
		cube = newCube;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Cube_schemaPackage.CUBE_SCHEMA__CUBE, oldCube, cube));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getName()
	{
		return name;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setName(String newName)
	{
		String oldName = name;
		name = newName;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Cube_schemaPackage.CUBE_SCHEMA__NAME, oldName, name));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public BaseCube getBaseCube()
	{
		if (baseCube != null && baseCube.eIsProxy())
		{
			InternalEObject oldBaseCube = (InternalEObject)baseCube;
			baseCube = (BaseCube)eResolveProxy(oldBaseCube);
			if (baseCube != oldBaseCube)
			{
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Cube_schemaPackage.CUBE_SCHEMA__BASE_CUBE, oldBaseCube, baseCube));
			}
		}
		return baseCube;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public BaseCube basicGetBaseCube()
	{
		return baseCube;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setBaseCube(BaseCube newBaseCube)
	{
		BaseCube oldBaseCube = baseCube;
		baseCube = newBaseCube;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Cube_schemaPackage.CUBE_SCHEMA__BASE_CUBE, oldBaseCube, baseCube));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType)
	{
		switch (featureID)
		{
			case Cube_schemaPackage.CUBE_SCHEMA__CUBE:
				if (resolve) return getCube();
				return basicGetCube();
			case Cube_schemaPackage.CUBE_SCHEMA__NAME:
				return getName();
			case Cube_schemaPackage.CUBE_SCHEMA__BASE_CUBE:
				if (resolve) return getBaseCube();
				return basicGetBaseCube();
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
		switch (featureID)
		{
			case Cube_schemaPackage.CUBE_SCHEMA__CUBE:
				setCube((CUBE)newValue);
				return;
			case Cube_schemaPackage.CUBE_SCHEMA__NAME:
				setName((String)newValue);
				return;
			case Cube_schemaPackage.CUBE_SCHEMA__BASE_CUBE:
				setBaseCube((BaseCube)newValue);
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
		switch (featureID)
		{
			case Cube_schemaPackage.CUBE_SCHEMA__CUBE:
				setCube((CUBE)null);
				return;
			case Cube_schemaPackage.CUBE_SCHEMA__NAME:
				setName(NAME_EDEFAULT);
				return;
			case Cube_schemaPackage.CUBE_SCHEMA__BASE_CUBE:
				setBaseCube((BaseCube)null);
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
		switch (featureID)
		{
			case Cube_schemaPackage.CUBE_SCHEMA__CUBE:
				return cube != null;
			case Cube_schemaPackage.CUBE_SCHEMA__NAME:
				return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
			case Cube_schemaPackage.CUBE_SCHEMA__BASE_CUBE:
				return baseCube != null;
		}
		return super.eIsSet(featureID);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String toString()
	{
		if (eIsProxy()) return super.toString();

		StringBuilder result = new StringBuilder(super.toString());
		result.append(" (name: ");
		result.append(name);
		result.append(')');
		return result.toString();
	}

} //CubeSchemaImpl
