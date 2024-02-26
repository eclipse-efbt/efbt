/**
 */
package org.eclipse.efbt.regdna.model.regdna.impl;

import org.eclipse.efbt.regdna.model.regdna.AttributePredicate;
import org.eclipse.efbt.regdna.model.regdna.Comparitor;
import org.eclipse.efbt.regdna.model.regdna.ELEnumLiteral;
import org.eclipse.efbt.regdna.model.regdna.ELStructuralFeature;
import org.eclipse.efbt.regdna.model.regdna.regdnaPackage;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Attribute Predicate</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.impl.AttributePredicateImpl#getAttribute1 <em>Attribute1</em>}</li>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.impl.AttributePredicateImpl#getComparitor <em>Comparitor</em>}</li>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.impl.AttributePredicateImpl#getMember <em>Member</em>}</li>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.impl.AttributePredicateImpl#getValue <em>Value</em>}</li>
 * </ul>
 *
 * @generated
 */
public class AttributePredicateImpl extends PredicateImpl implements AttributePredicate {
	/**
	 * The cached value of the '{@link #getAttribute1() <em>Attribute1</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getAttribute1()
	 * @generated
	 * @ordered
	 */
	protected ELStructuralFeature attribute1;

	/**
	 * The default value of the '{@link #getComparitor() <em>Comparitor</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getComparitor()
	 * @generated
	 * @ordered
	 */
	protected static final Comparitor COMPARITOR_EDEFAULT = Comparitor.LESS_THAN;

	/**
	 * The cached value of the '{@link #getComparitor() <em>Comparitor</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getComparitor()
	 * @generated
	 * @ordered
	 */
	protected Comparitor comparitor = COMPARITOR_EDEFAULT;

	/**
	 * The cached value of the '{@link #getMember() <em>Member</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getMember()
	 * @generated
	 * @ordered
	 */
	protected ELEnumLiteral member;

	/**
	 * The default value of the '{@link #getValue() <em>Value</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getValue()
	 * @generated
	 * @ordered
	 */
	protected static final String VALUE_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getValue() <em>Value</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getValue()
	 * @generated
	 * @ordered
	 */
	protected String value = VALUE_EDEFAULT;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected AttributePredicateImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return regdnaPackage.eINSTANCE.getAttributePredicate();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ELStructuralFeature getAttribute1() {
		if (attribute1 != null && attribute1.eIsProxy()) {
			InternalEObject oldAttribute1 = (InternalEObject)attribute1;
			attribute1 = (ELStructuralFeature)eResolveProxy(oldAttribute1);
			if (attribute1 != oldAttribute1) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, regdnaPackage.ATTRIBUTE_PREDICATE__ATTRIBUTE1, oldAttribute1, attribute1));
			}
		}
		return attribute1;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ELStructuralFeature basicGetAttribute1() {
		return attribute1;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setAttribute1(ELStructuralFeature newAttribute1) {
		ELStructuralFeature oldAttribute1 = attribute1;
		attribute1 = newAttribute1;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, regdnaPackage.ATTRIBUTE_PREDICATE__ATTRIBUTE1, oldAttribute1, attribute1));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Comparitor getComparitor() {
		return comparitor;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setComparitor(Comparitor newComparitor) {
		Comparitor oldComparitor = comparitor;
		comparitor = newComparitor == null ? COMPARITOR_EDEFAULT : newComparitor;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, regdnaPackage.ATTRIBUTE_PREDICATE__COMPARITOR, oldComparitor, comparitor));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ELEnumLiteral getMember() {
		if (member != null && member.eIsProxy()) {
			InternalEObject oldMember = (InternalEObject)member;
			member = (ELEnumLiteral)eResolveProxy(oldMember);
			if (member != oldMember) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, regdnaPackage.ATTRIBUTE_PREDICATE__MEMBER, oldMember, member));
			}
		}
		return member;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ELEnumLiteral basicGetMember() {
		return member;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setMember(ELEnumLiteral newMember) {
		ELEnumLiteral oldMember = member;
		member = newMember;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, regdnaPackage.ATTRIBUTE_PREDICATE__MEMBER, oldMember, member));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getValue() {
		return value;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setValue(String newValue) {
		String oldValue = value;
		value = newValue;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, regdnaPackage.ATTRIBUTE_PREDICATE__VALUE, oldValue, value));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case regdnaPackage.ATTRIBUTE_PREDICATE__ATTRIBUTE1:
				if (resolve) return getAttribute1();
				return basicGetAttribute1();
			case regdnaPackage.ATTRIBUTE_PREDICATE__COMPARITOR:
				return getComparitor();
			case regdnaPackage.ATTRIBUTE_PREDICATE__MEMBER:
				if (resolve) return getMember();
				return basicGetMember();
			case regdnaPackage.ATTRIBUTE_PREDICATE__VALUE:
				return getValue();
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
			case regdnaPackage.ATTRIBUTE_PREDICATE__ATTRIBUTE1:
				setAttribute1((ELStructuralFeature)newValue);
				return;
			case regdnaPackage.ATTRIBUTE_PREDICATE__COMPARITOR:
				setComparitor((Comparitor)newValue);
				return;
			case regdnaPackage.ATTRIBUTE_PREDICATE__MEMBER:
				setMember((ELEnumLiteral)newValue);
				return;
			case regdnaPackage.ATTRIBUTE_PREDICATE__VALUE:
				setValue((String)newValue);
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
			case regdnaPackage.ATTRIBUTE_PREDICATE__ATTRIBUTE1:
				setAttribute1((ELStructuralFeature)null);
				return;
			case regdnaPackage.ATTRIBUTE_PREDICATE__COMPARITOR:
				setComparitor(COMPARITOR_EDEFAULT);
				return;
			case regdnaPackage.ATTRIBUTE_PREDICATE__MEMBER:
				setMember((ELEnumLiteral)null);
				return;
			case regdnaPackage.ATTRIBUTE_PREDICATE__VALUE:
				setValue(VALUE_EDEFAULT);
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
			case regdnaPackage.ATTRIBUTE_PREDICATE__ATTRIBUTE1:
				return attribute1 != null;
			case regdnaPackage.ATTRIBUTE_PREDICATE__COMPARITOR:
				return comparitor != COMPARITOR_EDEFAULT;
			case regdnaPackage.ATTRIBUTE_PREDICATE__MEMBER:
				return member != null;
			case regdnaPackage.ATTRIBUTE_PREDICATE__VALUE:
				return VALUE_EDEFAULT == null ? value != null : !VALUE_EDEFAULT.equals(value);
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
		result.append(" (comparitor: ");
		result.append(comparitor);
		result.append(", value: ");
		result.append(value);
		result.append(')');
		return result.toString();
	}

} //AttributePredicateImpl
