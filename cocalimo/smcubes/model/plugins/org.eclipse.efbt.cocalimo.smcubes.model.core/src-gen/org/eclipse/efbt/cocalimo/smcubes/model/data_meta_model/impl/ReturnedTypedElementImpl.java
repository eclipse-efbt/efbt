/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl;

import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Data_meta_modelPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Entity;
import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.ReturnedTypedElement;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Returned Typed Element</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.ReturnedTypedElementImpl#getOrdered <em>Ordered</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.ReturnedTypedElementImpl#getUnique <em>Unique</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.ReturnedTypedElementImpl#getLowerBound <em>Lower Bound</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.ReturnedTypedElementImpl#getUpperBound <em>Upper Bound</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.ReturnedTypedElementImpl#getMany <em>Many</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.ReturnedTypedElementImpl#getRequired <em>Required</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.ReturnedTypedElementImpl#getEntityType <em>Entity Type</em>}</li>
 * </ul>
 *
 * @generated
 */
public abstract class ReturnedTypedElementImpl extends TypedElementImpl implements ReturnedTypedElement {
	/**
	 * The cached value of the '{@link #getOrdered() <em>Ordered</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getOrdered()
	 * @generated
	 * @ordered
	 */
	protected Object ordered;

	/**
	 * The cached value of the '{@link #getUnique() <em>Unique</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getUnique()
	 * @generated
	 * @ordered
	 */
	protected Object unique;

	/**
	 * The cached value of the '{@link #getLowerBound() <em>Lower Bound</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getLowerBound()
	 * @generated
	 * @ordered
	 */
	protected Object lowerBound;

	/**
	 * The cached value of the '{@link #getUpperBound() <em>Upper Bound</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getUpperBound()
	 * @generated
	 * @ordered
	 */
	protected Object upperBound;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ReturnedTypedElementImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Data_meta_modelPackage.Literals.RETURNED_TYPED_ELEMENT;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object getOrdered() {
		return ordered;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setOrdered(Object newOrdered) {
		Object oldOrdered = ordered;
		ordered = newOrdered;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_meta_modelPackage.RETURNED_TYPED_ELEMENT__ORDERED, oldOrdered, ordered));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object getUnique() {
		return unique;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setUnique(Object newUnique) {
		Object oldUnique = unique;
		unique = newUnique;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_meta_modelPackage.RETURNED_TYPED_ELEMENT__UNIQUE, oldUnique, unique));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object getLowerBound() {
		return lowerBound;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setLowerBound(Object newLowerBound) {
		Object oldLowerBound = lowerBound;
		lowerBound = newLowerBound;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_meta_modelPackage.RETURNED_TYPED_ELEMENT__LOWER_BOUND, oldLowerBound, lowerBound));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object getUpperBound() {
		return upperBound;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setUpperBound(Object newUpperBound) {
		Object oldUpperBound = upperBound;
		upperBound = newUpperBound;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Data_meta_modelPackage.RETURNED_TYPED_ELEMENT__UPPER_BOUND, oldUpperBound, upperBound));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object getMany() {
		// TODO: implement this method to return the 'Many' attribute
		// Ensure that you remove @generated or mark it @generated NOT
		throw new UnsupportedOperationException();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object getRequired() {
		// TODO: implement this method to return the 'Required' attribute
		// Ensure that you remove @generated or mark it @generated NOT
		throw new UnsupportedOperationException();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Entity getEntityType() {
		Entity entityType = basicGetEntityType();
		return entityType != null && entityType.eIsProxy() ? (Entity)eResolveProxy((InternalEObject)entityType) : entityType;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Entity basicGetEntityType() {
		// TODO: implement this method to return the 'Entity Type' reference
		// -> do not perform proxy resolution
		// Ensure that you remove @generated or mark it @generated NOT
		throw new UnsupportedOperationException();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setEntityType(Entity newEntityType) {
		// TODO: implement this method to set the 'Entity Type' reference
		// Ensure that you remove @generated or mark it @generated NOT
		throw new UnsupportedOperationException();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void unsetEntityType() {
		// TODO: implement this method to unset the 'Entity Type' reference
		// Ensure that you remove @generated or mark it @generated NOT
		throw new UnsupportedOperationException();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean isSetEntityType() {
		// TODO: implement this method to return whether the 'Entity Type' reference is set
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
			case Data_meta_modelPackage.RETURNED_TYPED_ELEMENT__ORDERED:
				return getOrdered();
			case Data_meta_modelPackage.RETURNED_TYPED_ELEMENT__UNIQUE:
				return getUnique();
			case Data_meta_modelPackage.RETURNED_TYPED_ELEMENT__LOWER_BOUND:
				return getLowerBound();
			case Data_meta_modelPackage.RETURNED_TYPED_ELEMENT__UPPER_BOUND:
				return getUpperBound();
			case Data_meta_modelPackage.RETURNED_TYPED_ELEMENT__MANY:
				return getMany();
			case Data_meta_modelPackage.RETURNED_TYPED_ELEMENT__REQUIRED:
				return getRequired();
			case Data_meta_modelPackage.RETURNED_TYPED_ELEMENT__ENTITY_TYPE:
				if (resolve) return getEntityType();
				return basicGetEntityType();
		}
		return super.eGet(featureID, resolve, coreType);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void eSet(int featureID, Object newValue) {
		switch (featureID) {
			case Data_meta_modelPackage.RETURNED_TYPED_ELEMENT__ORDERED:
				setOrdered(newValue);
				return;
			case Data_meta_modelPackage.RETURNED_TYPED_ELEMENT__UNIQUE:
				setUnique(newValue);
				return;
			case Data_meta_modelPackage.RETURNED_TYPED_ELEMENT__LOWER_BOUND:
				setLowerBound(newValue);
				return;
			case Data_meta_modelPackage.RETURNED_TYPED_ELEMENT__UPPER_BOUND:
				setUpperBound(newValue);
				return;
			case Data_meta_modelPackage.RETURNED_TYPED_ELEMENT__ENTITY_TYPE:
				setEntityType((Entity)newValue);
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
	public void eUnset(int featureID) {
		switch (featureID) {
			case Data_meta_modelPackage.RETURNED_TYPED_ELEMENT__ORDERED:
				setOrdered((Object)null);
				return;
			case Data_meta_modelPackage.RETURNED_TYPED_ELEMENT__UNIQUE:
				setUnique((Object)null);
				return;
			case Data_meta_modelPackage.RETURNED_TYPED_ELEMENT__LOWER_BOUND:
				setLowerBound((Object)null);
				return;
			case Data_meta_modelPackage.RETURNED_TYPED_ELEMENT__UPPER_BOUND:
				setUpperBound((Object)null);
				return;
			case Data_meta_modelPackage.RETURNED_TYPED_ELEMENT__ENTITY_TYPE:
				unsetEntityType();
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
	public boolean eIsSet(int featureID) {
		switch (featureID) {
			case Data_meta_modelPackage.RETURNED_TYPED_ELEMENT__ORDERED:
				return ordered != null;
			case Data_meta_modelPackage.RETURNED_TYPED_ELEMENT__UNIQUE:
				return unique != null;
			case Data_meta_modelPackage.RETURNED_TYPED_ELEMENT__LOWER_BOUND:
				return lowerBound != null;
			case Data_meta_modelPackage.RETURNED_TYPED_ELEMENT__UPPER_BOUND:
				return upperBound != null;
			case Data_meta_modelPackage.RETURNED_TYPED_ELEMENT__MANY:
				return getMany() != null;
			case Data_meta_modelPackage.RETURNED_TYPED_ELEMENT__REQUIRED:
				return getRequired() != null;
			case Data_meta_modelPackage.RETURNED_TYPED_ELEMENT__ENTITY_TYPE:
				return isSetEntityType();
		}
		return super.eIsSet(featureID);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String toString() {
		if (eIsProxy()) return super.toString();

		StringBuilder result = new StringBuilder(super.toString());
		result.append(" (ordered: ");
		result.append(ordered);
		result.append(", unique: ");
		result.append(unique);
		result.append(", lowerBound: ");
		result.append(lowerBound);
		result.append(", upperBound: ");
		result.append(upperBound);
		result.append(')');
		return result.toString();
	}

} //ReturnedTypedElementImpl
