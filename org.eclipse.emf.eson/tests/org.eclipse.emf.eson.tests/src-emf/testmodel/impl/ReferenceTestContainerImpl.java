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

import org.eclipse.emf.ecore.util.EDataTypeUniqueEList;
import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.EObjectResolvingEList;
import org.eclipse.emf.ecore.util.InternalEList;

import testmodel.ReferenceTarget;
import testmodel.ReferenceTestContainer;
import testmodel.TestmodelPackage;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Reference Test Container</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link testmodel.impl.ReferenceTestContainerImpl#getContainments <em>Containments</em>}</li>
 *   <li>{@link testmodel.impl.ReferenceTestContainerImpl#getReferenceToOne <em>Reference To One</em>}</li>
 *   <li>{@link testmodel.impl.ReferenceTestContainerImpl#getReferenceToMany <em>Reference To Many</em>}</li>
 *   <li>{@link testmodel.impl.ReferenceTestContainerImpl#getName <em>Name</em>}</li>
 *   <li>{@link testmodel.impl.ReferenceTestContainerImpl#getContainment <em>Containment</em>}</li>
 *   <li>{@link testmodel.impl.ReferenceTestContainerImpl#isOneAttribute <em>One Attribute</em>}</li>
 *   <li>{@link testmodel.impl.ReferenceTestContainerImpl#getManyAttributes <em>Many Attributes</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ReferenceTestContainerImpl extends EObjectImpl implements ReferenceTestContainer {
	/**
	 * The cached value of the '{@link #getContainments() <em>Containments</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getContainments()
	 * @generated
	 * @ordered
	 */
	protected EList<ReferenceTarget> containments;

	/**
	 * The cached value of the '{@link #getReferenceToOne() <em>Reference To One</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getReferenceToOne()
	 * @generated
	 * @ordered
	 */
	protected ReferenceTarget referenceToOne;

	/**
	 * The cached value of the '{@link #getReferenceToMany() <em>Reference To Many</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getReferenceToMany()
	 * @generated
	 * @ordered
	 */
	protected EList<ReferenceTarget> referenceToMany;

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
	 * The cached value of the '{@link #getContainment() <em>Containment</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getContainment()
	 * @generated
	 * @ordered
	 */
	protected ReferenceTarget containment;

	/**
	 * The default value of the '{@link #isOneAttribute() <em>One Attribute</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isOneAttribute()
	 * @generated
	 * @ordered
	 */
	protected static final boolean ONE_ATTRIBUTE_EDEFAULT = false;

	/**
	 * The cached value of the '{@link #isOneAttribute() <em>One Attribute</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isOneAttribute()
	 * @generated
	 * @ordered
	 */
	protected boolean oneAttribute = ONE_ATTRIBUTE_EDEFAULT;

	/**
	 * The cached value of the '{@link #getManyAttributes() <em>Many Attributes</em>}' attribute list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getManyAttributes()
	 * @generated
	 * @ordered
	 */
	protected EList<Boolean> manyAttributes;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ReferenceTestContainerImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return TestmodelPackage.Literals.REFERENCE_TEST_CONTAINER;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<ReferenceTarget> getContainments() {
		if (containments == null) {
			containments = new EObjectContainmentEList<ReferenceTarget>(ReferenceTarget.class, this, TestmodelPackage.REFERENCE_TEST_CONTAINER__CONTAINMENTS);
		}
		return containments;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ReferenceTarget getReferenceToOne() {
		if (referenceToOne != null && referenceToOne.eIsProxy()) {
			InternalEObject oldReferenceToOne = (InternalEObject)referenceToOne;
			referenceToOne = (ReferenceTarget)eResolveProxy(oldReferenceToOne);
			if (referenceToOne != oldReferenceToOne) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, TestmodelPackage.REFERENCE_TEST_CONTAINER__REFERENCE_TO_ONE, oldReferenceToOne, referenceToOne));
			}
		}
		return referenceToOne;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ReferenceTarget basicGetReferenceToOne() {
		return referenceToOne;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setReferenceToOne(ReferenceTarget newReferenceToOne) {
		ReferenceTarget oldReferenceToOne = referenceToOne;
		referenceToOne = newReferenceToOne;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TestmodelPackage.REFERENCE_TEST_CONTAINER__REFERENCE_TO_ONE, oldReferenceToOne, referenceToOne));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<ReferenceTarget> getReferenceToMany() {
		if (referenceToMany == null) {
			referenceToMany = new EObjectResolvingEList<ReferenceTarget>(ReferenceTarget.class, this, TestmodelPackage.REFERENCE_TEST_CONTAINER__REFERENCE_TO_MANY);
		}
		return referenceToMany;
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
			eNotify(new ENotificationImpl(this, Notification.SET, TestmodelPackage.REFERENCE_TEST_CONTAINER__NAME, oldName, name));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ReferenceTarget getContainment() {
		return containment;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetContainment(ReferenceTarget newContainment, NotificationChain msgs) {
		ReferenceTarget oldContainment = containment;
		containment = newContainment;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, TestmodelPackage.REFERENCE_TEST_CONTAINER__CONTAINMENT, oldContainment, newContainment);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setContainment(ReferenceTarget newContainment) {
		if (newContainment != containment) {
			NotificationChain msgs = null;
			if (containment != null)
				msgs = ((InternalEObject)containment).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - TestmodelPackage.REFERENCE_TEST_CONTAINER__CONTAINMENT, null, msgs);
			if (newContainment != null)
				msgs = ((InternalEObject)newContainment).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - TestmodelPackage.REFERENCE_TEST_CONTAINER__CONTAINMENT, null, msgs);
			msgs = basicSetContainment(newContainment, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TestmodelPackage.REFERENCE_TEST_CONTAINER__CONTAINMENT, newContainment, newContainment));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean isOneAttribute() {
		return oneAttribute;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setOneAttribute(boolean newOneAttribute) {
		boolean oldOneAttribute = oneAttribute;
		oneAttribute = newOneAttribute;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TestmodelPackage.REFERENCE_TEST_CONTAINER__ONE_ATTRIBUTE, oldOneAttribute, oneAttribute));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<Boolean> getManyAttributes() {
		if (manyAttributes == null) {
			manyAttributes = new EDataTypeUniqueEList<Boolean>(Boolean.class, this, TestmodelPackage.REFERENCE_TEST_CONTAINER__MANY_ATTRIBUTES);
		}
		return manyAttributes;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case TestmodelPackage.REFERENCE_TEST_CONTAINER__CONTAINMENTS:
				return ((InternalEList<?>)getContainments()).basicRemove(otherEnd, msgs);
			case TestmodelPackage.REFERENCE_TEST_CONTAINER__CONTAINMENT:
				return basicSetContainment(null, msgs);
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
			case TestmodelPackage.REFERENCE_TEST_CONTAINER__CONTAINMENTS:
				return getContainments();
			case TestmodelPackage.REFERENCE_TEST_CONTAINER__REFERENCE_TO_ONE:
				if (resolve) return getReferenceToOne();
				return basicGetReferenceToOne();
			case TestmodelPackage.REFERENCE_TEST_CONTAINER__REFERENCE_TO_MANY:
				return getReferenceToMany();
			case TestmodelPackage.REFERENCE_TEST_CONTAINER__NAME:
				return getName();
			case TestmodelPackage.REFERENCE_TEST_CONTAINER__CONTAINMENT:
				return getContainment();
			case TestmodelPackage.REFERENCE_TEST_CONTAINER__ONE_ATTRIBUTE:
				return isOneAttribute();
			case TestmodelPackage.REFERENCE_TEST_CONTAINER__MANY_ATTRIBUTES:
				return getManyAttributes();
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
			case TestmodelPackage.REFERENCE_TEST_CONTAINER__CONTAINMENTS:
				getContainments().clear();
				getContainments().addAll((Collection<? extends ReferenceTarget>)newValue);
				return;
			case TestmodelPackage.REFERENCE_TEST_CONTAINER__REFERENCE_TO_ONE:
				setReferenceToOne((ReferenceTarget)newValue);
				return;
			case TestmodelPackage.REFERENCE_TEST_CONTAINER__REFERENCE_TO_MANY:
				getReferenceToMany().clear();
				getReferenceToMany().addAll((Collection<? extends ReferenceTarget>)newValue);
				return;
			case TestmodelPackage.REFERENCE_TEST_CONTAINER__NAME:
				setName((String)newValue);
				return;
			case TestmodelPackage.REFERENCE_TEST_CONTAINER__CONTAINMENT:
				setContainment((ReferenceTarget)newValue);
				return;
			case TestmodelPackage.REFERENCE_TEST_CONTAINER__ONE_ATTRIBUTE:
				setOneAttribute((Boolean)newValue);
				return;
			case TestmodelPackage.REFERENCE_TEST_CONTAINER__MANY_ATTRIBUTES:
				getManyAttributes().clear();
				getManyAttributes().addAll((Collection<? extends Boolean>)newValue);
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
			case TestmodelPackage.REFERENCE_TEST_CONTAINER__CONTAINMENTS:
				getContainments().clear();
				return;
			case TestmodelPackage.REFERENCE_TEST_CONTAINER__REFERENCE_TO_ONE:
				setReferenceToOne((ReferenceTarget)null);
				return;
			case TestmodelPackage.REFERENCE_TEST_CONTAINER__REFERENCE_TO_MANY:
				getReferenceToMany().clear();
				return;
			case TestmodelPackage.REFERENCE_TEST_CONTAINER__NAME:
				setName(NAME_EDEFAULT);
				return;
			case TestmodelPackage.REFERENCE_TEST_CONTAINER__CONTAINMENT:
				setContainment((ReferenceTarget)null);
				return;
			case TestmodelPackage.REFERENCE_TEST_CONTAINER__ONE_ATTRIBUTE:
				setOneAttribute(ONE_ATTRIBUTE_EDEFAULT);
				return;
			case TestmodelPackage.REFERENCE_TEST_CONTAINER__MANY_ATTRIBUTES:
				getManyAttributes().clear();
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
			case TestmodelPackage.REFERENCE_TEST_CONTAINER__CONTAINMENTS:
				return containments != null && !containments.isEmpty();
			case TestmodelPackage.REFERENCE_TEST_CONTAINER__REFERENCE_TO_ONE:
				return referenceToOne != null;
			case TestmodelPackage.REFERENCE_TEST_CONTAINER__REFERENCE_TO_MANY:
				return referenceToMany != null && !referenceToMany.isEmpty();
			case TestmodelPackage.REFERENCE_TEST_CONTAINER__NAME:
				return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
			case TestmodelPackage.REFERENCE_TEST_CONTAINER__CONTAINMENT:
				return containment != null;
			case TestmodelPackage.REFERENCE_TEST_CONTAINER__ONE_ATTRIBUTE:
				return oneAttribute != ONE_ATTRIBUTE_EDEFAULT;
			case TestmodelPackage.REFERENCE_TEST_CONTAINER__MANY_ATTRIBUTES:
				return manyAttributes != null && !manyAttributes.isEmpty();
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
		result.append(", oneAttribute: ");
		result.append(oneAttribute);
		result.append(", manyAttributes: ");
		result.append(manyAttributes);
		result.append(')');
		return result.toString();
	}

} //ReferenceTestContainerImpl
