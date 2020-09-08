/**
 */
package testmodel.impl;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.EObjectImpl;

import testmodel.Abstract;
import testmodel.Interface;
import testmodel.TestModel;
import testmodel.TestmodelPackage;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Abstract</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link testmodel.impl.AbstractImpl#isParentAttribute <em>Parent Attribute</em>}</li>
 *   <li>{@link testmodel.impl.AbstractImpl#getParentReference <em>Parent Reference</em>}</li>
 *   <li>{@link testmodel.impl.AbstractImpl#getParentContainment <em>Parent Containment</em>}</li>
 * </ul>
 *
 * @generated
 */
public abstract class AbstractImpl extends EObjectImpl implements Abstract {
	/**
	 * The default value of the '{@link #isParentAttribute() <em>Parent Attribute</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isParentAttribute()
	 * @generated
	 * @ordered
	 */
	protected static final boolean PARENT_ATTRIBUTE_EDEFAULT = false;

	/**
	 * The cached value of the '{@link #isParentAttribute() <em>Parent Attribute</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isParentAttribute()
	 * @generated
	 * @ordered
	 */
	protected boolean parentAttribute = PARENT_ATTRIBUTE_EDEFAULT;

	/**
	 * The cached value of the '{@link #getParentReference() <em>Parent Reference</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getParentReference()
	 * @generated
	 * @ordered
	 */
	protected TestModel parentReference;

	/**
	 * The cached value of the '{@link #getParentContainment() <em>Parent Containment</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getParentContainment()
	 * @generated
	 * @ordered
	 */
	protected Interface parentContainment;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected AbstractImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return TestmodelPackage.Literals.ABSTRACT;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean isParentAttribute() {
		return parentAttribute;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setParentAttribute(boolean newParentAttribute) {
		boolean oldParentAttribute = parentAttribute;
		parentAttribute = newParentAttribute;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TestmodelPackage.ABSTRACT__PARENT_ATTRIBUTE, oldParentAttribute, parentAttribute));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TestModel getParentReference() {
		if (parentReference != null && parentReference.eIsProxy()) {
			InternalEObject oldParentReference = (InternalEObject)parentReference;
			parentReference = (TestModel)eResolveProxy(oldParentReference);
			if (parentReference != oldParentReference) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, TestmodelPackage.ABSTRACT__PARENT_REFERENCE, oldParentReference, parentReference));
			}
		}
		return parentReference;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TestModel basicGetParentReference() {
		return parentReference;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setParentReference(TestModel newParentReference) {
		TestModel oldParentReference = parentReference;
		parentReference = newParentReference;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TestmodelPackage.ABSTRACT__PARENT_REFERENCE, oldParentReference, parentReference));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Interface getParentContainment() {
		if (parentContainment != null && parentContainment.eIsProxy()) {
			InternalEObject oldParentContainment = (InternalEObject)parentContainment;
			parentContainment = (Interface)eResolveProxy(oldParentContainment);
			if (parentContainment != oldParentContainment) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, TestmodelPackage.ABSTRACT__PARENT_CONTAINMENT, oldParentContainment, parentContainment));
			}
		}
		return parentContainment;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Interface basicGetParentContainment() {
		return parentContainment;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setParentContainment(Interface newParentContainment) {
		Interface oldParentContainment = parentContainment;
		parentContainment = newParentContainment;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TestmodelPackage.ABSTRACT__PARENT_CONTAINMENT, oldParentContainment, parentContainment));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case TestmodelPackage.ABSTRACT__PARENT_ATTRIBUTE:
				return isParentAttribute();
			case TestmodelPackage.ABSTRACT__PARENT_REFERENCE:
				if (resolve) return getParentReference();
				return basicGetParentReference();
			case TestmodelPackage.ABSTRACT__PARENT_CONTAINMENT:
				if (resolve) return getParentContainment();
				return basicGetParentContainment();
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
			case TestmodelPackage.ABSTRACT__PARENT_ATTRIBUTE:
				setParentAttribute((Boolean)newValue);
				return;
			case TestmodelPackage.ABSTRACT__PARENT_REFERENCE:
				setParentReference((TestModel)newValue);
				return;
			case TestmodelPackage.ABSTRACT__PARENT_CONTAINMENT:
				setParentContainment((Interface)newValue);
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
			case TestmodelPackage.ABSTRACT__PARENT_ATTRIBUTE:
				setParentAttribute(PARENT_ATTRIBUTE_EDEFAULT);
				return;
			case TestmodelPackage.ABSTRACT__PARENT_REFERENCE:
				setParentReference((TestModel)null);
				return;
			case TestmodelPackage.ABSTRACT__PARENT_CONTAINMENT:
				setParentContainment((Interface)null);
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
			case TestmodelPackage.ABSTRACT__PARENT_ATTRIBUTE:
				return parentAttribute != PARENT_ATTRIBUTE_EDEFAULT;
			case TestmodelPackage.ABSTRACT__PARENT_REFERENCE:
				return parentReference != null;
			case TestmodelPackage.ABSTRACT__PARENT_CONTAINMENT:
				return parentContainment != null;
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

		StringBuffer result = new StringBuffer(super.toString());
		result.append(" (parentAttribute: ");
		result.append(parentAttribute);
		result.append(')');
		return result.toString();
	}

} //AbstractImpl
