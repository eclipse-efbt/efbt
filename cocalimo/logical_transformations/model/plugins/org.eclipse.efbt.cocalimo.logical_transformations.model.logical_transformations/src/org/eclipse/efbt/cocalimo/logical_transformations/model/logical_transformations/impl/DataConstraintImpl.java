/**
 */
package org.eclipse.efbt.cocalimo.logical_transformations.model.logical_transformations.impl;

import org.eclipse.efbt.cocalimo.logical_transformations.model.logical_transformations.AttrComparison;
import org.eclipse.efbt.cocalimo.logical_transformations.model.logical_transformations.DataConstraint;
import org.eclipse.efbt.cocalimo.logical_transformations.model.logical_transformations.Logical_transformationsPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Attribute;
import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Data Constraint</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.logical_transformations.model.logical_transformations.impl.DataConstraintImpl#getAttr1 <em>Attr1</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.logical_transformations.model.logical_transformations.impl.DataConstraintImpl#getAttr2 <em>Attr2</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.logical_transformations.model.logical_transformations.impl.DataConstraintImpl#getComparison <em>Comparison</em>}</li>
 * </ul>
 *
 * @generated
 */
public class DataConstraintImpl extends MinimalEObjectImpl.Container implements DataConstraint {
	/**
	 * The cached value of the '{@link #getAttr1() <em>Attr1</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getAttr1()
	 * @generated
	 * @ordered
	 */
	protected Attribute attr1;

	/**
	 * The cached value of the '{@link #getAttr2() <em>Attr2</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getAttr2()
	 * @generated
	 * @ordered
	 */
	protected Attribute attr2;

	/**
	 * The default value of the '{@link #getComparison() <em>Comparison</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getComparison()
	 * @generated
	 * @ordered
	 */
	protected static final AttrComparison COMPARISON_EDEFAULT = AttrComparison.EQUALS;

	/**
	 * The cached value of the '{@link #getComparison() <em>Comparison</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getComparison()
	 * @generated
	 * @ordered
	 */
	protected AttrComparison comparison = COMPARISON_EDEFAULT;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected DataConstraintImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Logical_transformationsPackage.Literals.DATA_CONSTRAINT;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Attribute getAttr1() {
		if (attr1 != null && attr1.eIsProxy()) {
			InternalEObject oldAttr1 = (InternalEObject)attr1;
			attr1 = (Attribute)eResolveProxy(oldAttr1);
			if (attr1 != oldAttr1) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Logical_transformationsPackage.DATA_CONSTRAINT__ATTR1, oldAttr1, attr1));
			}
		}
		return attr1;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Attribute basicGetAttr1() {
		return attr1;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setAttr1(Attribute newAttr1) {
		Attribute oldAttr1 = attr1;
		attr1 = newAttr1;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Logical_transformationsPackage.DATA_CONSTRAINT__ATTR1, oldAttr1, attr1));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Attribute getAttr2() {
		if (attr2 != null && attr2.eIsProxy()) {
			InternalEObject oldAttr2 = (InternalEObject)attr2;
			attr2 = (Attribute)eResolveProxy(oldAttr2);
			if (attr2 != oldAttr2) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Logical_transformationsPackage.DATA_CONSTRAINT__ATTR2, oldAttr2, attr2));
			}
		}
		return attr2;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Attribute basicGetAttr2() {
		return attr2;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setAttr2(Attribute newAttr2) {
		Attribute oldAttr2 = attr2;
		attr2 = newAttr2;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Logical_transformationsPackage.DATA_CONSTRAINT__ATTR2, oldAttr2, attr2));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public AttrComparison getComparison() {
		return comparison;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setComparison(AttrComparison newComparison) {
		AttrComparison oldComparison = comparison;
		comparison = newComparison == null ? COMPARISON_EDEFAULT : newComparison;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Logical_transformationsPackage.DATA_CONSTRAINT__COMPARISON, oldComparison, comparison));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Logical_transformationsPackage.DATA_CONSTRAINT__ATTR1:
				if (resolve) return getAttr1();
				return basicGetAttr1();
			case Logical_transformationsPackage.DATA_CONSTRAINT__ATTR2:
				if (resolve) return getAttr2();
				return basicGetAttr2();
			case Logical_transformationsPackage.DATA_CONSTRAINT__COMPARISON:
				return getComparison();
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
			case Logical_transformationsPackage.DATA_CONSTRAINT__ATTR1:
				setAttr1((Attribute)newValue);
				return;
			case Logical_transformationsPackage.DATA_CONSTRAINT__ATTR2:
				setAttr2((Attribute)newValue);
				return;
			case Logical_transformationsPackage.DATA_CONSTRAINT__COMPARISON:
				setComparison((AttrComparison)newValue);
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
			case Logical_transformationsPackage.DATA_CONSTRAINT__ATTR1:
				setAttr1((Attribute)null);
				return;
			case Logical_transformationsPackage.DATA_CONSTRAINT__ATTR2:
				setAttr2((Attribute)null);
				return;
			case Logical_transformationsPackage.DATA_CONSTRAINT__COMPARISON:
				setComparison(COMPARISON_EDEFAULT);
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
			case Logical_transformationsPackage.DATA_CONSTRAINT__ATTR1:
				return attr1 != null;
			case Logical_transformationsPackage.DATA_CONSTRAINT__ATTR2:
				return attr2 != null;
			case Logical_transformationsPackage.DATA_CONSTRAINT__COMPARISON:
				return comparison != COMPARISON_EDEFAULT;
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
		result.append(" (comparison: ");
		result.append(comparison);
		result.append(')');
		return result.toString();
	}

} //DataConstraintImpl
