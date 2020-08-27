/**
 */
package testmodel.impl;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.EObjectImpl;

import testmodel.CustomName;
import testmodel.DefaultName;
import testmodel.NameAttributeContainer;
import testmodel.NoName;
import testmodel.TestmodelPackage;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Name Attribute Container</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link testmodel.impl.NameAttributeContainerImpl#getDefaultNameTest <em>Default Name Test</em>}</li>
 *   <li>{@link testmodel.impl.NameAttributeContainerImpl#getCustomNameTest <em>Custom Name Test</em>}</li>
 *   <li>{@link testmodel.impl.NameAttributeContainerImpl#getName <em>Name</em>}</li>
 *   <li>{@link testmodel.impl.NameAttributeContainerImpl#getNoNameTest <em>No Name Test</em>}</li>
 * </ul>
 *
 * @generated
 */
public class NameAttributeContainerImpl extends EObjectImpl implements NameAttributeContainer {
	/**
	 * The cached value of the '{@link #getDefaultNameTest() <em>Default Name Test</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getDefaultNameTest()
	 * @generated
	 * @ordered
	 */
	protected DefaultName defaultNameTest;

	/**
	 * The cached value of the '{@link #getCustomNameTest() <em>Custom Name Test</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCustomNameTest()
	 * @generated
	 * @ordered
	 */
	protected CustomName customNameTest;

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
	 * The cached value of the '{@link #getNoNameTest() <em>No Name Test</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getNoNameTest()
	 * @generated
	 * @ordered
	 */
	protected NoName noNameTest;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected NameAttributeContainerImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return TestmodelPackage.Literals.NAME_ATTRIBUTE_CONTAINER;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public DefaultName getDefaultNameTest() {
		return defaultNameTest;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetDefaultNameTest(DefaultName newDefaultNameTest, NotificationChain msgs) {
		DefaultName oldDefaultNameTest = defaultNameTest;
		defaultNameTest = newDefaultNameTest;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, TestmodelPackage.NAME_ATTRIBUTE_CONTAINER__DEFAULT_NAME_TEST, oldDefaultNameTest, newDefaultNameTest);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setDefaultNameTest(DefaultName newDefaultNameTest) {
		if (newDefaultNameTest != defaultNameTest) {
			NotificationChain msgs = null;
			if (defaultNameTest != null)
				msgs = ((InternalEObject)defaultNameTest).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - TestmodelPackage.NAME_ATTRIBUTE_CONTAINER__DEFAULT_NAME_TEST, null, msgs);
			if (newDefaultNameTest != null)
				msgs = ((InternalEObject)newDefaultNameTest).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - TestmodelPackage.NAME_ATTRIBUTE_CONTAINER__DEFAULT_NAME_TEST, null, msgs);
			msgs = basicSetDefaultNameTest(newDefaultNameTest, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TestmodelPackage.NAME_ATTRIBUTE_CONTAINER__DEFAULT_NAME_TEST, newDefaultNameTest, newDefaultNameTest));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CustomName getCustomNameTest() {
		return customNameTest;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetCustomNameTest(CustomName newCustomNameTest, NotificationChain msgs) {
		CustomName oldCustomNameTest = customNameTest;
		customNameTest = newCustomNameTest;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, TestmodelPackage.NAME_ATTRIBUTE_CONTAINER__CUSTOM_NAME_TEST, oldCustomNameTest, newCustomNameTest);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setCustomNameTest(CustomName newCustomNameTest) {
		if (newCustomNameTest != customNameTest) {
			NotificationChain msgs = null;
			if (customNameTest != null)
				msgs = ((InternalEObject)customNameTest).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - TestmodelPackage.NAME_ATTRIBUTE_CONTAINER__CUSTOM_NAME_TEST, null, msgs);
			if (newCustomNameTest != null)
				msgs = ((InternalEObject)newCustomNameTest).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - TestmodelPackage.NAME_ATTRIBUTE_CONTAINER__CUSTOM_NAME_TEST, null, msgs);
			msgs = basicSetCustomNameTest(newCustomNameTest, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TestmodelPackage.NAME_ATTRIBUTE_CONTAINER__CUSTOM_NAME_TEST, newCustomNameTest, newCustomNameTest));
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
			eNotify(new ENotificationImpl(this, Notification.SET, TestmodelPackage.NAME_ATTRIBUTE_CONTAINER__NAME, oldName, name));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NoName getNoNameTest() {
		return noNameTest;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetNoNameTest(NoName newNoNameTest, NotificationChain msgs) {
		NoName oldNoNameTest = noNameTest;
		noNameTest = newNoNameTest;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, TestmodelPackage.NAME_ATTRIBUTE_CONTAINER__NO_NAME_TEST, oldNoNameTest, newNoNameTest);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setNoNameTest(NoName newNoNameTest) {
		if (newNoNameTest != noNameTest) {
			NotificationChain msgs = null;
			if (noNameTest != null)
				msgs = ((InternalEObject)noNameTest).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - TestmodelPackage.NAME_ATTRIBUTE_CONTAINER__NO_NAME_TEST, null, msgs);
			if (newNoNameTest != null)
				msgs = ((InternalEObject)newNoNameTest).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - TestmodelPackage.NAME_ATTRIBUTE_CONTAINER__NO_NAME_TEST, null, msgs);
			msgs = basicSetNoNameTest(newNoNameTest, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TestmodelPackage.NAME_ATTRIBUTE_CONTAINER__NO_NAME_TEST, newNoNameTest, newNoNameTest));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case TestmodelPackage.NAME_ATTRIBUTE_CONTAINER__DEFAULT_NAME_TEST:
				return basicSetDefaultNameTest(null, msgs);
			case TestmodelPackage.NAME_ATTRIBUTE_CONTAINER__CUSTOM_NAME_TEST:
				return basicSetCustomNameTest(null, msgs);
			case TestmodelPackage.NAME_ATTRIBUTE_CONTAINER__NO_NAME_TEST:
				return basicSetNoNameTest(null, msgs);
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
			case TestmodelPackage.NAME_ATTRIBUTE_CONTAINER__DEFAULT_NAME_TEST:
				return getDefaultNameTest();
			case TestmodelPackage.NAME_ATTRIBUTE_CONTAINER__CUSTOM_NAME_TEST:
				return getCustomNameTest();
			case TestmodelPackage.NAME_ATTRIBUTE_CONTAINER__NAME:
				return getName();
			case TestmodelPackage.NAME_ATTRIBUTE_CONTAINER__NO_NAME_TEST:
				return getNoNameTest();
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
			case TestmodelPackage.NAME_ATTRIBUTE_CONTAINER__DEFAULT_NAME_TEST:
				setDefaultNameTest((DefaultName)newValue);
				return;
			case TestmodelPackage.NAME_ATTRIBUTE_CONTAINER__CUSTOM_NAME_TEST:
				setCustomNameTest((CustomName)newValue);
				return;
			case TestmodelPackage.NAME_ATTRIBUTE_CONTAINER__NAME:
				setName((String)newValue);
				return;
			case TestmodelPackage.NAME_ATTRIBUTE_CONTAINER__NO_NAME_TEST:
				setNoNameTest((NoName)newValue);
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
			case TestmodelPackage.NAME_ATTRIBUTE_CONTAINER__DEFAULT_NAME_TEST:
				setDefaultNameTest((DefaultName)null);
				return;
			case TestmodelPackage.NAME_ATTRIBUTE_CONTAINER__CUSTOM_NAME_TEST:
				setCustomNameTest((CustomName)null);
				return;
			case TestmodelPackage.NAME_ATTRIBUTE_CONTAINER__NAME:
				setName(NAME_EDEFAULT);
				return;
			case TestmodelPackage.NAME_ATTRIBUTE_CONTAINER__NO_NAME_TEST:
				setNoNameTest((NoName)null);
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
			case TestmodelPackage.NAME_ATTRIBUTE_CONTAINER__DEFAULT_NAME_TEST:
				return defaultNameTest != null;
			case TestmodelPackage.NAME_ATTRIBUTE_CONTAINER__CUSTOM_NAME_TEST:
				return customNameTest != null;
			case TestmodelPackage.NAME_ATTRIBUTE_CONTAINER__NAME:
				return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
			case TestmodelPackage.NAME_ATTRIBUTE_CONTAINER__NO_NAME_TEST:
				return noNameTest != null;
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

} //NameAttributeContainerImpl
