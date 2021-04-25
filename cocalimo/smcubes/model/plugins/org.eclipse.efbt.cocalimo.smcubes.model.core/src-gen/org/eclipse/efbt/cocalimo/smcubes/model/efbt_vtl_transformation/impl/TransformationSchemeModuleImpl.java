/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.efbt_vtl_transformation.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.core.model.module_management.impl.ModuleImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.efbt_vtl_transformation.Efbt_vtl_transformationPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.efbt_vtl_transformation.TransformationSchemeModule;

import org.eclipse.efbt.cocalimo.smcubes.model.vtl_transformation.TRANSFORMATION_SCHEME;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Transformation Scheme Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.efbt_vtl_transformation.impl.TransformationSchemeModuleImpl#getSchemes <em>Schemes</em>}</li>
 * </ul>
 *
 * @generated
 */
public class TransformationSchemeModuleImpl extends ModuleImpl implements TransformationSchemeModule
{
	/**
	 * The cached value of the '{@link #getSchemes() <em>Schemes</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSchemes()
	 * @generated
	 * @ordered
	 */
	protected EList<TRANSFORMATION_SCHEME> schemes;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected TransformationSchemeModuleImpl()
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
		return Efbt_vtl_transformationPackage.Literals.TRANSFORMATION_SCHEME_MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<TRANSFORMATION_SCHEME> getSchemes()
	{
		if (schemes == null) {
			schemes = new EObjectContainmentEList<TRANSFORMATION_SCHEME>(TRANSFORMATION_SCHEME.class, this, Efbt_vtl_transformationPackage.TRANSFORMATION_SCHEME_MODULE__SCHEMES);
		}
		return schemes;
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
			case Efbt_vtl_transformationPackage.TRANSFORMATION_SCHEME_MODULE__SCHEMES:
				return ((InternalEList<?>)getSchemes()).basicRemove(otherEnd, msgs);
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
			case Efbt_vtl_transformationPackage.TRANSFORMATION_SCHEME_MODULE__SCHEMES:
				return getSchemes();
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
			case Efbt_vtl_transformationPackage.TRANSFORMATION_SCHEME_MODULE__SCHEMES:
				getSchemes().clear();
				getSchemes().addAll((Collection<? extends TRANSFORMATION_SCHEME>)newValue);
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
			case Efbt_vtl_transformationPackage.TRANSFORMATION_SCHEME_MODULE__SCHEMES:
				getSchemes().clear();
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
			case Efbt_vtl_transformationPackage.TRANSFORMATION_SCHEME_MODULE__SCHEMES:
				return schemes != null && !schemes.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //TransformationSchemeModuleImpl
