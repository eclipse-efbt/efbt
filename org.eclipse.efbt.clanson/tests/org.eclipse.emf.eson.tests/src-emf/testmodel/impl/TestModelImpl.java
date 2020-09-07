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
import org.eclipse.emf.ecore.impl.EObjectImpl;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

import testmodel.AttributeSample;
import testmodel.AttributeTestContainer;
import testmodel.Child;
import testmodel.NameAttributeContainer;
import testmodel.ReferenceTestContainer;
import testmodel.SingleOptional;
import testmodel.SingleRequired;
import testmodel.TestModel;
import testmodel.TestmodelPackage;
import testmodel.testsubmodel.TestInnerModel;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Test Model</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link testmodel.impl.TestModelImpl#getChildren <em>Children</em>}</li>
 *   <li>{@link testmodel.impl.TestModelImpl#getSingleOptional <em>Single Optional</em>}</li>
 *   <li>{@link testmodel.impl.TestModelImpl#getSingleRequired <em>Single Required</em>}</li>
 *   <li>{@link testmodel.impl.TestModelImpl#getName <em>Name</em>}</li>
 *   <li>{@link testmodel.impl.TestModelImpl#getAttributeSample <em>Attribute Sample</em>}</li>
 *   <li>{@link testmodel.impl.TestModelImpl#getLinkToSingleRequired <em>Link To Single Required</em>}</li>
 *   <li>{@link testmodel.impl.TestModelImpl#getReferenceTestContainer <em>Reference Test Container</em>}</li>
 *   <li>{@link testmodel.impl.TestModelImpl#getAttributeTest <em>Attribute Test</em>}</li>
 *   <li>{@link testmodel.impl.TestModelImpl#getNameAttributeTest <em>Name Attribute Test</em>}</li>
 *   <li>{@link testmodel.impl.TestModelImpl#getSub <em>Sub</em>}</li>
 * </ul>
 *
 * @generated
 */
public class TestModelImpl extends EObjectImpl implements TestModel {
	/**
	 * The cached value of the '{@link #getChildren() <em>Children</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getChildren()
	 * @generated
	 * @ordered
	 */
	protected EList<Child> children;

	/**
	 * The cached value of the '{@link #getSingleOptional() <em>Single Optional</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSingleOptional()
	 * @generated
	 * @ordered
	 */
	protected SingleOptional singleOptional;

	/**
	 * The cached value of the '{@link #getSingleRequired() <em>Single Required</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSingleRequired()
	 * @generated
	 * @ordered
	 */
	protected SingleRequired singleRequired;

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
	 * The cached value of the '{@link #getAttributeSample() <em>Attribute Sample</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getAttributeSample()
	 * @generated
	 * @ordered
	 */
	protected AttributeSample attributeSample;

	/**
	 * The cached value of the '{@link #getLinkToSingleRequired() <em>Link To Single Required</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getLinkToSingleRequired()
	 * @generated
	 * @ordered
	 */
	protected SingleRequired linkToSingleRequired;

	/**
	 * The cached value of the '{@link #getReferenceTestContainer() <em>Reference Test Container</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getReferenceTestContainer()
	 * @generated
	 * @ordered
	 */
	protected EList<ReferenceTestContainer> referenceTestContainer;

	/**
	 * The cached value of the '{@link #getAttributeTest() <em>Attribute Test</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getAttributeTest()
	 * @generated
	 * @ordered
	 */
	protected EList<AttributeTestContainer> attributeTest;

	/**
	 * The cached value of the '{@link #getNameAttributeTest() <em>Name Attribute Test</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getNameAttributeTest()
	 * @generated
	 * @ordered
	 */
	protected EList<NameAttributeContainer> nameAttributeTest;

	/**
	 * The cached value of the '{@link #getSub() <em>Sub</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSub()
	 * @generated
	 * @ordered
	 */
	protected TestInnerModel sub;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected TestModelImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return TestmodelPackage.Literals.TEST_MODEL;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<Child> getChildren() {
		if (children == null) {
			children = new EObjectContainmentEList<Child>(Child.class, this, TestmodelPackage.TEST_MODEL__CHILDREN);
		}
		return children;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SingleOptional getSingleOptional() {
		return singleOptional;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetSingleOptional(SingleOptional newSingleOptional, NotificationChain msgs) {
		SingleOptional oldSingleOptional = singleOptional;
		singleOptional = newSingleOptional;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, TestmodelPackage.TEST_MODEL__SINGLE_OPTIONAL, oldSingleOptional, newSingleOptional);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setSingleOptional(SingleOptional newSingleOptional) {
		if (newSingleOptional != singleOptional) {
			NotificationChain msgs = null;
			if (singleOptional != null)
				msgs = ((InternalEObject)singleOptional).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - TestmodelPackage.TEST_MODEL__SINGLE_OPTIONAL, null, msgs);
			if (newSingleOptional != null)
				msgs = ((InternalEObject)newSingleOptional).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - TestmodelPackage.TEST_MODEL__SINGLE_OPTIONAL, null, msgs);
			msgs = basicSetSingleOptional(newSingleOptional, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TestmodelPackage.TEST_MODEL__SINGLE_OPTIONAL, newSingleOptional, newSingleOptional));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SingleRequired getSingleRequired() {
		return singleRequired;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetSingleRequired(SingleRequired newSingleRequired, NotificationChain msgs) {
		SingleRequired oldSingleRequired = singleRequired;
		singleRequired = newSingleRequired;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, TestmodelPackage.TEST_MODEL__SINGLE_REQUIRED, oldSingleRequired, newSingleRequired);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setSingleRequired(SingleRequired newSingleRequired) {
		if (newSingleRequired != singleRequired) {
			NotificationChain msgs = null;
			if (singleRequired != null)
				msgs = ((InternalEObject)singleRequired).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - TestmodelPackage.TEST_MODEL__SINGLE_REQUIRED, null, msgs);
			if (newSingleRequired != null)
				msgs = ((InternalEObject)newSingleRequired).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - TestmodelPackage.TEST_MODEL__SINGLE_REQUIRED, null, msgs);
			msgs = basicSetSingleRequired(newSingleRequired, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TestmodelPackage.TEST_MODEL__SINGLE_REQUIRED, newSingleRequired, newSingleRequired));
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
			eNotify(new ENotificationImpl(this, Notification.SET, TestmodelPackage.TEST_MODEL__NAME, oldName, name));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public AttributeSample getAttributeSample() {
		return attributeSample;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetAttributeSample(AttributeSample newAttributeSample, NotificationChain msgs) {
		AttributeSample oldAttributeSample = attributeSample;
		attributeSample = newAttributeSample;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, TestmodelPackage.TEST_MODEL__ATTRIBUTE_SAMPLE, oldAttributeSample, newAttributeSample);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setAttributeSample(AttributeSample newAttributeSample) {
		if (newAttributeSample != attributeSample) {
			NotificationChain msgs = null;
			if (attributeSample != null)
				msgs = ((InternalEObject)attributeSample).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - TestmodelPackage.TEST_MODEL__ATTRIBUTE_SAMPLE, null, msgs);
			if (newAttributeSample != null)
				msgs = ((InternalEObject)newAttributeSample).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - TestmodelPackage.TEST_MODEL__ATTRIBUTE_SAMPLE, null, msgs);
			msgs = basicSetAttributeSample(newAttributeSample, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TestmodelPackage.TEST_MODEL__ATTRIBUTE_SAMPLE, newAttributeSample, newAttributeSample));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SingleRequired getLinkToSingleRequired() {
		if (linkToSingleRequired != null && linkToSingleRequired.eIsProxy()) {
			InternalEObject oldLinkToSingleRequired = (InternalEObject)linkToSingleRequired;
			linkToSingleRequired = (SingleRequired)eResolveProxy(oldLinkToSingleRequired);
			if (linkToSingleRequired != oldLinkToSingleRequired) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, TestmodelPackage.TEST_MODEL__LINK_TO_SINGLE_REQUIRED, oldLinkToSingleRequired, linkToSingleRequired));
			}
		}
		return linkToSingleRequired;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SingleRequired basicGetLinkToSingleRequired() {
		return linkToSingleRequired;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setLinkToSingleRequired(SingleRequired newLinkToSingleRequired) {
		SingleRequired oldLinkToSingleRequired = linkToSingleRequired;
		linkToSingleRequired = newLinkToSingleRequired;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TestmodelPackage.TEST_MODEL__LINK_TO_SINGLE_REQUIRED, oldLinkToSingleRequired, linkToSingleRequired));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<ReferenceTestContainer> getReferenceTestContainer() {
		if (referenceTestContainer == null) {
			referenceTestContainer = new EObjectContainmentEList<ReferenceTestContainer>(ReferenceTestContainer.class, this, TestmodelPackage.TEST_MODEL__REFERENCE_TEST_CONTAINER);
		}
		return referenceTestContainer;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<AttributeTestContainer> getAttributeTest() {
		if (attributeTest == null) {
			attributeTest = new EObjectContainmentEList<AttributeTestContainer>(AttributeTestContainer.class, this, TestmodelPackage.TEST_MODEL__ATTRIBUTE_TEST);
		}
		return attributeTest;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<NameAttributeContainer> getNameAttributeTest() {
		if (nameAttributeTest == null) {
			nameAttributeTest = new EObjectContainmentEList<NameAttributeContainer>(NameAttributeContainer.class, this, TestmodelPackage.TEST_MODEL__NAME_ATTRIBUTE_TEST);
		}
		return nameAttributeTest;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TestInnerModel getSub() {
		return sub;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetSub(TestInnerModel newSub, NotificationChain msgs) {
		TestInnerModel oldSub = sub;
		sub = newSub;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, TestmodelPackage.TEST_MODEL__SUB, oldSub, newSub);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setSub(TestInnerModel newSub) {
		if (newSub != sub) {
			NotificationChain msgs = null;
			if (sub != null)
				msgs = ((InternalEObject)sub).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - TestmodelPackage.TEST_MODEL__SUB, null, msgs);
			if (newSub != null)
				msgs = ((InternalEObject)newSub).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - TestmodelPackage.TEST_MODEL__SUB, null, msgs);
			msgs = basicSetSub(newSub, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TestmodelPackage.TEST_MODEL__SUB, newSub, newSub));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case TestmodelPackage.TEST_MODEL__CHILDREN:
				return ((InternalEList<?>)getChildren()).basicRemove(otherEnd, msgs);
			case TestmodelPackage.TEST_MODEL__SINGLE_OPTIONAL:
				return basicSetSingleOptional(null, msgs);
			case TestmodelPackage.TEST_MODEL__SINGLE_REQUIRED:
				return basicSetSingleRequired(null, msgs);
			case TestmodelPackage.TEST_MODEL__ATTRIBUTE_SAMPLE:
				return basicSetAttributeSample(null, msgs);
			case TestmodelPackage.TEST_MODEL__REFERENCE_TEST_CONTAINER:
				return ((InternalEList<?>)getReferenceTestContainer()).basicRemove(otherEnd, msgs);
			case TestmodelPackage.TEST_MODEL__ATTRIBUTE_TEST:
				return ((InternalEList<?>)getAttributeTest()).basicRemove(otherEnd, msgs);
			case TestmodelPackage.TEST_MODEL__NAME_ATTRIBUTE_TEST:
				return ((InternalEList<?>)getNameAttributeTest()).basicRemove(otherEnd, msgs);
			case TestmodelPackage.TEST_MODEL__SUB:
				return basicSetSub(null, msgs);
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
			case TestmodelPackage.TEST_MODEL__CHILDREN:
				return getChildren();
			case TestmodelPackage.TEST_MODEL__SINGLE_OPTIONAL:
				return getSingleOptional();
			case TestmodelPackage.TEST_MODEL__SINGLE_REQUIRED:
				return getSingleRequired();
			case TestmodelPackage.TEST_MODEL__NAME:
				return getName();
			case TestmodelPackage.TEST_MODEL__ATTRIBUTE_SAMPLE:
				return getAttributeSample();
			case TestmodelPackage.TEST_MODEL__LINK_TO_SINGLE_REQUIRED:
				if (resolve) return getLinkToSingleRequired();
				return basicGetLinkToSingleRequired();
			case TestmodelPackage.TEST_MODEL__REFERENCE_TEST_CONTAINER:
				return getReferenceTestContainer();
			case TestmodelPackage.TEST_MODEL__ATTRIBUTE_TEST:
				return getAttributeTest();
			case TestmodelPackage.TEST_MODEL__NAME_ATTRIBUTE_TEST:
				return getNameAttributeTest();
			case TestmodelPackage.TEST_MODEL__SUB:
				return getSub();
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
			case TestmodelPackage.TEST_MODEL__CHILDREN:
				getChildren().clear();
				getChildren().addAll((Collection<? extends Child>)newValue);
				return;
			case TestmodelPackage.TEST_MODEL__SINGLE_OPTIONAL:
				setSingleOptional((SingleOptional)newValue);
				return;
			case TestmodelPackage.TEST_MODEL__SINGLE_REQUIRED:
				setSingleRequired((SingleRequired)newValue);
				return;
			case TestmodelPackage.TEST_MODEL__NAME:
				setName((String)newValue);
				return;
			case TestmodelPackage.TEST_MODEL__ATTRIBUTE_SAMPLE:
				setAttributeSample((AttributeSample)newValue);
				return;
			case TestmodelPackage.TEST_MODEL__LINK_TO_SINGLE_REQUIRED:
				setLinkToSingleRequired((SingleRequired)newValue);
				return;
			case TestmodelPackage.TEST_MODEL__REFERENCE_TEST_CONTAINER:
				getReferenceTestContainer().clear();
				getReferenceTestContainer().addAll((Collection<? extends ReferenceTestContainer>)newValue);
				return;
			case TestmodelPackage.TEST_MODEL__ATTRIBUTE_TEST:
				getAttributeTest().clear();
				getAttributeTest().addAll((Collection<? extends AttributeTestContainer>)newValue);
				return;
			case TestmodelPackage.TEST_MODEL__NAME_ATTRIBUTE_TEST:
				getNameAttributeTest().clear();
				getNameAttributeTest().addAll((Collection<? extends NameAttributeContainer>)newValue);
				return;
			case TestmodelPackage.TEST_MODEL__SUB:
				setSub((TestInnerModel)newValue);
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
			case TestmodelPackage.TEST_MODEL__CHILDREN:
				getChildren().clear();
				return;
			case TestmodelPackage.TEST_MODEL__SINGLE_OPTIONAL:
				setSingleOptional((SingleOptional)null);
				return;
			case TestmodelPackage.TEST_MODEL__SINGLE_REQUIRED:
				setSingleRequired((SingleRequired)null);
				return;
			case TestmodelPackage.TEST_MODEL__NAME:
				setName(NAME_EDEFAULT);
				return;
			case TestmodelPackage.TEST_MODEL__ATTRIBUTE_SAMPLE:
				setAttributeSample((AttributeSample)null);
				return;
			case TestmodelPackage.TEST_MODEL__LINK_TO_SINGLE_REQUIRED:
				setLinkToSingleRequired((SingleRequired)null);
				return;
			case TestmodelPackage.TEST_MODEL__REFERENCE_TEST_CONTAINER:
				getReferenceTestContainer().clear();
				return;
			case TestmodelPackage.TEST_MODEL__ATTRIBUTE_TEST:
				getAttributeTest().clear();
				return;
			case TestmodelPackage.TEST_MODEL__NAME_ATTRIBUTE_TEST:
				getNameAttributeTest().clear();
				return;
			case TestmodelPackage.TEST_MODEL__SUB:
				setSub((TestInnerModel)null);
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
			case TestmodelPackage.TEST_MODEL__CHILDREN:
				return children != null && !children.isEmpty();
			case TestmodelPackage.TEST_MODEL__SINGLE_OPTIONAL:
				return singleOptional != null;
			case TestmodelPackage.TEST_MODEL__SINGLE_REQUIRED:
				return singleRequired != null;
			case TestmodelPackage.TEST_MODEL__NAME:
				return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
			case TestmodelPackage.TEST_MODEL__ATTRIBUTE_SAMPLE:
				return attributeSample != null;
			case TestmodelPackage.TEST_MODEL__LINK_TO_SINGLE_REQUIRED:
				return linkToSingleRequired != null;
			case TestmodelPackage.TEST_MODEL__REFERENCE_TEST_CONTAINER:
				return referenceTestContainer != null && !referenceTestContainer.isEmpty();
			case TestmodelPackage.TEST_MODEL__ATTRIBUTE_TEST:
				return attributeTest != null && !attributeTest.isEmpty();
			case TestmodelPackage.TEST_MODEL__NAME_ATTRIBUTE_TEST:
				return nameAttributeTest != null && !nameAttributeTest.isEmpty();
			case TestmodelPackage.TEST_MODEL__SUB:
				return sub != null;
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

} //TestModelImpl
