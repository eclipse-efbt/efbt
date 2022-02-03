/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FunctionSpec;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ParamaterSpec;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Function Spec</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.FunctionSpecImpl#getParameterSpecs <em>Parameter Specs</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.FunctionSpecImpl#getFunctionalName <em>Functional Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.FunctionSpecImpl#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.FunctionSpecImpl#getSqlName <em>Sql Name</em>}</li>
 * </ul>
 *
 * @generated
 */
public class FunctionSpecImpl extends MinimalEObjectImpl.Container implements FunctionSpec {
	/**
	 * The cached value of the '{@link #getParameterSpecs() <em>Parameter Specs</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getParameterSpecs()
	 * @generated
	 * @ordered
	 */
	protected EList<ParamaterSpec> parameterSpecs;

	/**
	 * The default value of the '{@link #getFunctionalName() <em>Functional Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getFunctionalName()
	 * @generated
	 * @ordered
	 */
	protected static final String FUNCTIONAL_NAME_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getFunctionalName() <em>Functional Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getFunctionalName()
	 * @generated
	 * @ordered
	 */
	protected String functionalName = FUNCTIONAL_NAME_EDEFAULT;

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
	 * The default value of the '{@link #getSqlName() <em>Sql Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSqlName()
	 * @generated
	 * @ordered
	 */
	protected static final String SQL_NAME_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getSqlName() <em>Sql Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSqlName()
	 * @generated
	 * @ordered
	 */
	protected String sqlName = SQL_NAME_EDEFAULT;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected FunctionSpecImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Ecore_plus_logicPackage.Literals.FUNCTION_SPEC;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<ParamaterSpec> getParameterSpecs() {
		if (parameterSpecs == null) {
			parameterSpecs = new EObjectContainmentEList<ParamaterSpec>(ParamaterSpec.class, this, Ecore_plus_logicPackage.FUNCTION_SPEC__PARAMETER_SPECS);
		}
		return parameterSpecs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getFunctionalName() {
		return functionalName;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setFunctionalName(String newFunctionalName) {
		String oldFunctionalName = functionalName;
		functionalName = newFunctionalName;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore_plus_logicPackage.FUNCTION_SPEC__FUNCTIONAL_NAME, oldFunctionalName, functionalName));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore_plus_logicPackage.FUNCTION_SPEC__NAME, oldName, name));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getSqlName() {
		return sqlName;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setSqlName(String newSqlName) {
		String oldSqlName = sqlName;
		sqlName = newSqlName;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore_plus_logicPackage.FUNCTION_SPEC__SQL_NAME, oldSqlName, sqlName));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Ecore_plus_logicPackage.FUNCTION_SPEC__PARAMETER_SPECS:
				return ((InternalEList<?>)getParameterSpecs()).basicRemove(otherEnd, msgs);
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
			case Ecore_plus_logicPackage.FUNCTION_SPEC__PARAMETER_SPECS:
				return getParameterSpecs();
			case Ecore_plus_logicPackage.FUNCTION_SPEC__FUNCTIONAL_NAME:
				return getFunctionalName();
			case Ecore_plus_logicPackage.FUNCTION_SPEC__NAME:
				return getName();
			case Ecore_plus_logicPackage.FUNCTION_SPEC__SQL_NAME:
				return getSqlName();
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
			case Ecore_plus_logicPackage.FUNCTION_SPEC__PARAMETER_SPECS:
				getParameterSpecs().clear();
				getParameterSpecs().addAll((Collection<? extends ParamaterSpec>)newValue);
				return;
			case Ecore_plus_logicPackage.FUNCTION_SPEC__FUNCTIONAL_NAME:
				setFunctionalName((String)newValue);
				return;
			case Ecore_plus_logicPackage.FUNCTION_SPEC__NAME:
				setName((String)newValue);
				return;
			case Ecore_plus_logicPackage.FUNCTION_SPEC__SQL_NAME:
				setSqlName((String)newValue);
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
			case Ecore_plus_logicPackage.FUNCTION_SPEC__PARAMETER_SPECS:
				getParameterSpecs().clear();
				return;
			case Ecore_plus_logicPackage.FUNCTION_SPEC__FUNCTIONAL_NAME:
				setFunctionalName(FUNCTIONAL_NAME_EDEFAULT);
				return;
			case Ecore_plus_logicPackage.FUNCTION_SPEC__NAME:
				setName(NAME_EDEFAULT);
				return;
			case Ecore_plus_logicPackage.FUNCTION_SPEC__SQL_NAME:
				setSqlName(SQL_NAME_EDEFAULT);
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
			case Ecore_plus_logicPackage.FUNCTION_SPEC__PARAMETER_SPECS:
				return parameterSpecs != null && !parameterSpecs.isEmpty();
			case Ecore_plus_logicPackage.FUNCTION_SPEC__FUNCTIONAL_NAME:
				return FUNCTIONAL_NAME_EDEFAULT == null ? functionalName != null : !FUNCTIONAL_NAME_EDEFAULT.equals(functionalName);
			case Ecore_plus_logicPackage.FUNCTION_SPEC__NAME:
				return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
			case Ecore_plus_logicPackage.FUNCTION_SPEC__SQL_NAME:
				return SQL_NAME_EDEFAULT == null ? sqlName != null : !SQL_NAME_EDEFAULT.equals(sqlName);
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
		result.append(" (functionalName: ");
		result.append(functionalName);
		result.append(", name: ");
		result.append(name);
		result.append(", sqlName: ");
		result.append(sqlName);
		result.append(')');
		return result.toString();
	}

} //FunctionSpecImpl
