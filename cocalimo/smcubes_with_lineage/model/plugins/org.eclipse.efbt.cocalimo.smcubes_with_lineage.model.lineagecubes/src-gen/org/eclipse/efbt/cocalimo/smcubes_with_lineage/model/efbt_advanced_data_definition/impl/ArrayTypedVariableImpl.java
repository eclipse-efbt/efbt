/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.impl;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.BaseCube;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.ArrayTypedVariable;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.Efbt_advanced_data_definitionPackage;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Array Typed Variable</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.impl.ArrayTypedVariableImpl#getCubeType <em>Cube Type</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ArrayTypedVariableImpl extends AdvancedTypedVariableImpl implements ArrayTypedVariable
{
	/**
	 * The cached value of the '{@link #getCubeType() <em>Cube Type</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCubeType()
	 * @generated
	 * @ordered
	 */
	protected BaseCube cubeType;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ArrayTypedVariableImpl()
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
		return Efbt_advanced_data_definitionPackage.Literals.ARRAY_TYPED_VARIABLE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public BaseCube getCubeType()
	{
		if (cubeType != null && cubeType.eIsProxy()) {
			InternalEObject oldCubeType = (InternalEObject)cubeType;
			cubeType = (BaseCube)eResolveProxy(oldCubeType);
			if (cubeType != oldCubeType) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Efbt_advanced_data_definitionPackage.ARRAY_TYPED_VARIABLE__CUBE_TYPE, oldCubeType, cubeType));
			}
		}
		return cubeType;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public BaseCube basicGetCubeType()
	{
		return cubeType;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setCubeType(BaseCube newCubeType)
	{
		BaseCube oldCubeType = cubeType;
		cubeType = newCubeType;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Efbt_advanced_data_definitionPackage.ARRAY_TYPED_VARIABLE__CUBE_TYPE, oldCubeType, cubeType));
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
			case Efbt_advanced_data_definitionPackage.ARRAY_TYPED_VARIABLE__CUBE_TYPE:
				if (resolve) return getCubeType();
				return basicGetCubeType();
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
			case Efbt_advanced_data_definitionPackage.ARRAY_TYPED_VARIABLE__CUBE_TYPE:
				setCubeType((BaseCube)newValue);
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
			case Efbt_advanced_data_definitionPackage.ARRAY_TYPED_VARIABLE__CUBE_TYPE:
				setCubeType((BaseCube)null);
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
			case Efbt_advanced_data_definitionPackage.ARRAY_TYPED_VARIABLE__CUBE_TYPE:
				return cubeType != null;
		}
		return super.eIsSet(featureID);
	}

} //ArrayTypedVariableImpl
