/**
 */
package org.eclipse.efbt.regdna.model.regdna.impl;

import java.util.Collection;

import org.eclipse.efbt.regdna.model.regdna.regdnaPackage;
import org.eclipse.efbt.regdna.model.regdna.RuleForILTablePart;
import org.eclipse.efbt.regdna.model.regdna.SelectColumn;
import org.eclipse.efbt.regdna.model.regdna.TableFilter;

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
 * An implementation of the model object '<em><b>Rule For IL Table Part</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.impl.RuleForILTablePartImpl#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.impl.RuleForILTablePartImpl#getColumns <em>Columns</em>}</li>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.impl.RuleForILTablePartImpl#getWhereClause <em>Where Clause</em>}</li>
 * </ul>
 *
 * @generated
 */
public class RuleForILTablePartImpl extends MinimalEObjectImpl.Container implements RuleForILTablePart {
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
	 * The cached value of the '{@link #getColumns() <em>Columns</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getColumns()
	 * @generated
	 * @ordered
	 */
	protected EList<SelectColumn> columns;

	/**
	 * The cached value of the '{@link #getWhereClause() <em>Where Clause</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getWhereClause()
	 * @generated
	 * @ordered
	 */
	protected TableFilter whereClause;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected RuleForILTablePartImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return regdnaPackage.eINSTANCE.getRuleForILTablePart();
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
			eNotify(new ENotificationImpl(this, Notification.SET, regdnaPackage.RULE_FOR_IL_TABLE_PART__NAME, oldName, name));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<SelectColumn> getColumns() {
		if (columns == null) {
			columns = new EObjectContainmentEList<SelectColumn>(SelectColumn.class, this, regdnaPackage.RULE_FOR_IL_TABLE_PART__COLUMNS);
		}
		return columns;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TableFilter getWhereClause() {
		return whereClause;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetWhereClause(TableFilter newWhereClause, NotificationChain msgs) {
		TableFilter oldWhereClause = whereClause;
		whereClause = newWhereClause;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, regdnaPackage.RULE_FOR_IL_TABLE_PART__WHERE_CLAUSE, oldWhereClause, newWhereClause);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setWhereClause(TableFilter newWhereClause) {
		if (newWhereClause != whereClause) {
			NotificationChain msgs = null;
			if (whereClause != null)
				msgs = ((InternalEObject)whereClause).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - regdnaPackage.RULE_FOR_IL_TABLE_PART__WHERE_CLAUSE, null, msgs);
			if (newWhereClause != null)
				msgs = ((InternalEObject)newWhereClause).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - regdnaPackage.RULE_FOR_IL_TABLE_PART__WHERE_CLAUSE, null, msgs);
			msgs = basicSetWhereClause(newWhereClause, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, regdnaPackage.RULE_FOR_IL_TABLE_PART__WHERE_CLAUSE, newWhereClause, newWhereClause));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case regdnaPackage.RULE_FOR_IL_TABLE_PART__COLUMNS:
				return ((InternalEList<?>)getColumns()).basicRemove(otherEnd, msgs);
			case regdnaPackage.RULE_FOR_IL_TABLE_PART__WHERE_CLAUSE:
				return basicSetWhereClause(null, msgs);
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
			case regdnaPackage.RULE_FOR_IL_TABLE_PART__NAME:
				return getName();
			case regdnaPackage.RULE_FOR_IL_TABLE_PART__COLUMNS:
				return getColumns();
			case regdnaPackage.RULE_FOR_IL_TABLE_PART__WHERE_CLAUSE:
				return getWhereClause();
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
			case regdnaPackage.RULE_FOR_IL_TABLE_PART__NAME:
				setName((String)newValue);
				return;
			case regdnaPackage.RULE_FOR_IL_TABLE_PART__COLUMNS:
				getColumns().clear();
				getColumns().addAll((Collection<? extends SelectColumn>)newValue);
				return;
			case regdnaPackage.RULE_FOR_IL_TABLE_PART__WHERE_CLAUSE:
				setWhereClause((TableFilter)newValue);
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
			case regdnaPackage.RULE_FOR_IL_TABLE_PART__NAME:
				setName(NAME_EDEFAULT);
				return;
			case regdnaPackage.RULE_FOR_IL_TABLE_PART__COLUMNS:
				getColumns().clear();
				return;
			case regdnaPackage.RULE_FOR_IL_TABLE_PART__WHERE_CLAUSE:
				setWhereClause((TableFilter)null);
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
			case regdnaPackage.RULE_FOR_IL_TABLE_PART__NAME:
				return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
			case regdnaPackage.RULE_FOR_IL_TABLE_PART__COLUMNS:
				return columns != null && !columns.isEmpty();
			case regdnaPackage.RULE_FOR_IL_TABLE_PART__WHERE_CLAUSE:
				return whereClause != null;
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
		result.append(')');
		return result.toString();
	}

} //RuleForILTablePartImpl
