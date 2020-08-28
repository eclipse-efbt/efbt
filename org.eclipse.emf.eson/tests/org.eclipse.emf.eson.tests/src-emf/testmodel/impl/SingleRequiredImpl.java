/**
 */
package testmodel.impl;

import java.util.Collection;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

import testmodel.NestedElement;
import testmodel.NestedElements;
import testmodel.SingleRequired;
import testmodel.TestmodelPackage;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Single Required</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link testmodel.impl.SingleRequiredImpl#getNestedElements <em>Nested Elements</em>}</li>
 *   <li>{@link testmodel.impl.SingleRequiredImpl#getNestedElement <em>Nested Element</em>}</li>
 *   <li>{@link testmodel.impl.SingleRequiredImpl#getName <em>Name</em>}</li>
 * </ul>
 *
 * @generated
 */
public class SingleRequiredImpl extends AbstractImpl implements SingleRequired {
	/**
	 * The cached value of the '{@link #getNestedElements() <em>Nested Elements</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getNestedElements()
	 * @generated
	 * @ordered
	 */
	protected EList<NestedElements> nestedElements;

	/**
	 * The cached value of the '{@link #getNestedElement() <em>Nested Element</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getNestedElement()
	 * @generated
	 * @ordered
	 */
	protected NestedElement nestedElement;

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
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected SingleRequiredImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return TestmodelPackage.Literals.SINGLE_REQUIRED;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<NestedElements> getNestedElements() {
		if (nestedElements == null) {
			nestedElements = new EObjectContainmentEList<NestedElements>(NestedElements.class, this, TestmodelPackage.SINGLE_REQUIRED__NESTED_ELEMENTS);
		}
		return nestedElements;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NestedElement getNestedElement() {
		return nestedElement;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetNestedElement(NestedElement newNestedElement, NotificationChain msgs) {
		NestedElement oldNestedElement = nestedElement;
		nestedElement = newNestedElement;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, TestmodelPackage.SINGLE_REQUIRED__NESTED_ELEMENT, oldNestedElement, newNestedElement);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setNestedElement(NestedElement newNestedElement) {
		if (newNestedElement != nestedElement) {
			NotificationChain msgs = null;
			if (nestedElement != null)
				msgs = ((InternalEObject)nestedElement).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - TestmodelPackage.SINGLE_REQUIRED__NESTED_ELEMENT, null, msgs);
			if (newNestedElement != null)
				msgs = ((InternalEObject)newNestedElement).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - TestmodelPackage.SINGLE_REQUIRED__NESTED_ELEMENT, null, msgs);
			msgs = basicSetNestedElement(newNestedElement, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TestmodelPackage.SINGLE_REQUIRED__NESTED_ELEMENT, newNestedElement, newNestedElement));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getName() {
		return name;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setName(String newName) {
		String oldName = name;
		name = newName;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TestmodelPackage.SINGLE_REQUIRED__NAME, oldName, name));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case TestmodelPackage.SINGLE_REQUIRED__NESTED_ELEMENTS:
				return ((InternalEList<?>)getNestedElements()).basicRemove(otherEnd, msgs);
			case TestmodelPackage.SINGLE_REQUIRED__NESTED_ELEMENT:
				return basicSetNestedElement(null, msgs);
		}
		return super.eInverseRemove(otherEnd, featureID, msgs);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case TestmodelPackage.SINGLE_REQUIRED__NESTED_ELEMENTS:
				return getNestedElements();
			case TestmodelPackage.SINGLE_REQUIRED__NESTED_ELEMENT:
				return getNestedElement();
			case TestmodelPackage.SINGLE_REQUIRED__NAME:
				return getName();
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
	public void eSet(int featureID, Object newValue) {
		switch (featureID) {
			case TestmodelPackage.SINGLE_REQUIRED__NESTED_ELEMENTS:
				getNestedElements().clear();
				getNestedElements().addAll((Collection<? extends NestedElements>)newValue);
				return;
			case TestmodelPackage.SINGLE_REQUIRED__NESTED_ELEMENT:
				setNestedElement((NestedElement)newValue);
				return;
			case TestmodelPackage.SINGLE_REQUIRED__NAME:
				setName((String)newValue);
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
			case TestmodelPackage.SINGLE_REQUIRED__NESTED_ELEMENTS:
				getNestedElements().clear();
				return;
			case TestmodelPackage.SINGLE_REQUIRED__NESTED_ELEMENT:
				setNestedElement((NestedElement)null);
				return;
			case TestmodelPackage.SINGLE_REQUIRED__NAME:
				setName(NAME_EDEFAULT);
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
			case TestmodelPackage.SINGLE_REQUIRED__NESTED_ELEMENTS:
				return nestedElements != null && !nestedElements.isEmpty();
			case TestmodelPackage.SINGLE_REQUIRED__NESTED_ELEMENT:
				return nestedElement != null;
			case TestmodelPackage.SINGLE_REQUIRED__NAME:
				return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
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
		result.append(" (name: ");
		result.append(name);
		result.append(')');
		return result.toString();
	}

} //SingleRequiredImpl
