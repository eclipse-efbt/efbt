/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl;

import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Attribute;
import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Data_meta_modelPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.DerivedEntity;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Derived Entity</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.DerivedEntityImpl#getEAttributes <em>EAttributes</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.DerivedEntityImpl#getEIDAttribute <em>EID Attribute</em>}</li>
 * </ul>
 *
 * @generated
 */
public class DerivedEntityImpl extends EntityImpl implements DerivedEntity {
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected DerivedEntityImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Data_meta_modelPackage.Literals.DERIVED_ENTITY;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<Attribute> getEAttributes() {
		// TODO: implement this method to return the 'EAttributes' reference list
		// Ensure that you remove @generated or mark it @generated NOT
		// The list is expected to implement org.eclipse.emf.ecore.util.InternalEList and org.eclipse.emf.ecore.EStructuralFeature.Setting
		// so it's likely that an appropriate subclass of org.eclipse.emf.ecore.util.EcoreEList should be used.
		throw new UnsupportedOperationException();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Attribute getEIDAttribute() {
		// TODO: implement this method to return the 'EID Attribute' reference
		// Ensure that you remove @generated or mark it @generated NOT
		throw new UnsupportedOperationException();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Data_meta_modelPackage.DERIVED_ENTITY__EATTRIBUTES:
				return getEAttributes();
			case Data_meta_modelPackage.DERIVED_ENTITY__EID_ATTRIBUTE:
				return getEIDAttribute();
		}
		return super.eGet(featureID, resolve, coreType);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public boolean eIsSet(int featureID) {
		switch (featureID) {
			case Data_meta_modelPackage.DERIVED_ENTITY__EATTRIBUTES:
				return !getEAttributes().isEmpty();
			case Data_meta_modelPackage.DERIVED_ENTITY__EID_ATTRIBUTE:
				return getEIDAttribute() != null;
		}
		return super.eIsSet(featureID);
	}

} //DerivedEntityImpl
