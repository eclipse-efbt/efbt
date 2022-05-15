/**
 */
package org.eclipse.efbt.openregspecs.model.testing.impl;

import java.util.Collection;

import org.eclipse.efbt.openregspecs.model.data_meta_model.Attribute;

import org.eclipse.efbt.openregspecs.model.testing.DataConstraint;
import org.eclipse.efbt.openregspecs.model.testing.Scenario;
import org.eclipse.efbt.openregspecs.model.testing.TestingPackage;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

import org.eclipse.emf.ecore.util.EObjectResolvingEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Scenario</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.testing.impl.ScenarioImpl#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.testing.impl.ScenarioImpl#isInvisible <em>Invisible</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.testing.impl.ScenarioImpl#getDescription <em>Description</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.testing.impl.ScenarioImpl#getRequiredAttributes <em>Required Attributes</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.testing.impl.ScenarioImpl#getData_constraints <em>Data constraints</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ScenarioImpl extends MinimalEObjectImpl.Container implements Scenario {
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
	 * The default value of the '{@link #isInvisible() <em>Invisible</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isInvisible()
	 * @generated
	 * @ordered
	 */
	protected static final boolean INVISIBLE_EDEFAULT = false;

	/**
	 * The cached value of the '{@link #isInvisible() <em>Invisible</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isInvisible()
	 * @generated
	 * @ordered
	 */
	protected boolean invisible = INVISIBLE_EDEFAULT;

	/**
	 * The default value of the '{@link #getDescription() <em>Description</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getDescription()
	 * @generated
	 * @ordered
	 */
	protected static final String DESCRIPTION_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getDescription() <em>Description</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getDescription()
	 * @generated
	 * @ordered
	 */
	protected String description = DESCRIPTION_EDEFAULT;

	/**
	 * The cached value of the '{@link #getRequiredAttributes() <em>Required Attributes</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getRequiredAttributes()
	 * @generated
	 * @ordered
	 */
	protected EList<Attribute> requiredAttributes;

	/**
	 * The cached value of the '{@link #getData_constraints() <em>Data constraints</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getData_constraints()
	 * @generated
	 * @ordered
	 */
	protected DataConstraint data_constraints;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ScenarioImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return TestingPackage.Literals.SCENARIO;
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
			eNotify(new ENotificationImpl(this, Notification.SET, TestingPackage.SCENARIO__NAME, oldName, name));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean isInvisible() {
		return invisible;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setInvisible(boolean newInvisible) {
		boolean oldInvisible = invisible;
		invisible = newInvisible;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TestingPackage.SCENARIO__INVISIBLE, oldInvisible, invisible));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getDescription() {
		return description;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setDescription(String newDescription) {
		String oldDescription = description;
		description = newDescription;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TestingPackage.SCENARIO__DESCRIPTION, oldDescription, description));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<Attribute> getRequiredAttributes() {
		if (requiredAttributes == null) {
			requiredAttributes = new EObjectResolvingEList<Attribute>(Attribute.class, this, TestingPackage.SCENARIO__REQUIRED_ATTRIBUTES);
		}
		return requiredAttributes;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public DataConstraint getData_constraints() {
		return data_constraints;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetData_constraints(DataConstraint newData_constraints, NotificationChain msgs) {
		DataConstraint oldData_constraints = data_constraints;
		data_constraints = newData_constraints;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, TestingPackage.SCENARIO__DATA_CONSTRAINTS, oldData_constraints, newData_constraints);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setData_constraints(DataConstraint newData_constraints) {
		if (newData_constraints != data_constraints) {
			NotificationChain msgs = null;
			if (data_constraints != null)
				msgs = ((InternalEObject)data_constraints).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - TestingPackage.SCENARIO__DATA_CONSTRAINTS, null, msgs);
			if (newData_constraints != null)
				msgs = ((InternalEObject)newData_constraints).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - TestingPackage.SCENARIO__DATA_CONSTRAINTS, null, msgs);
			msgs = basicSetData_constraints(newData_constraints, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TestingPackage.SCENARIO__DATA_CONSTRAINTS, newData_constraints, newData_constraints));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case TestingPackage.SCENARIO__DATA_CONSTRAINTS:
				return basicSetData_constraints(null, msgs);
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
			case TestingPackage.SCENARIO__NAME:
				return getName();
			case TestingPackage.SCENARIO__INVISIBLE:
				return isInvisible();
			case TestingPackage.SCENARIO__DESCRIPTION:
				return getDescription();
			case TestingPackage.SCENARIO__REQUIRED_ATTRIBUTES:
				return getRequiredAttributes();
			case TestingPackage.SCENARIO__DATA_CONSTRAINTS:
				return getData_constraints();
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
			case TestingPackage.SCENARIO__NAME:
				setName((String)newValue);
				return;
			case TestingPackage.SCENARIO__INVISIBLE:
				setInvisible((Boolean)newValue);
				return;
			case TestingPackage.SCENARIO__DESCRIPTION:
				setDescription((String)newValue);
				return;
			case TestingPackage.SCENARIO__REQUIRED_ATTRIBUTES:
				getRequiredAttributes().clear();
				getRequiredAttributes().addAll((Collection<? extends Attribute>)newValue);
				return;
			case TestingPackage.SCENARIO__DATA_CONSTRAINTS:
				setData_constraints((DataConstraint)newValue);
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
			case TestingPackage.SCENARIO__NAME:
				setName(NAME_EDEFAULT);
				return;
			case TestingPackage.SCENARIO__INVISIBLE:
				setInvisible(INVISIBLE_EDEFAULT);
				return;
			case TestingPackage.SCENARIO__DESCRIPTION:
				setDescription(DESCRIPTION_EDEFAULT);
				return;
			case TestingPackage.SCENARIO__REQUIRED_ATTRIBUTES:
				getRequiredAttributes().clear();
				return;
			case TestingPackage.SCENARIO__DATA_CONSTRAINTS:
				setData_constraints((DataConstraint)null);
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
			case TestingPackage.SCENARIO__NAME:
				return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
			case TestingPackage.SCENARIO__INVISIBLE:
				return invisible != INVISIBLE_EDEFAULT;
			case TestingPackage.SCENARIO__DESCRIPTION:
				return DESCRIPTION_EDEFAULT == null ? description != null : !DESCRIPTION_EDEFAULT.equals(description);
			case TestingPackage.SCENARIO__REQUIRED_ATTRIBUTES:
				return requiredAttributes != null && !requiredAttributes.isEmpty();
			case TestingPackage.SCENARIO__DATA_CONSTRAINTS:
				return data_constraints != null;
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
		result.append(" (name: ");
		result.append(name);
		result.append(", invisible: ");
		result.append(invisible);
		result.append(", description: ");
		result.append(description);
		result.append(')');
		return result.toString();
	}

} //ScenarioImpl
