/**
 */
package org.eclipse.efbt.ecore4reg.model.ecore4reg.impl;

import java.util.Collection;

import org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.RuleForILTablePart;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.SelectColumn;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.TableFilter;

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
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.RuleForILTablePartImpl#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.RuleForILTablePartImpl#getColumns <em>Columns</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.RuleForILTablePartImpl#getWhereClause <em>Where Clause</em>}</li>
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
	 * The cached value of the '{@link #getWhereClause() <em>Where Clause</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getWhereClause()
	 * @generated
	 * @ordered
	 */
	protected EList<TableFilter> whereClause;

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
		return Ecore4regPackage.eINSTANCE.getRuleForILTablePart();
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
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore4regPackage.RULE_FOR_IL_TABLE_PART__NAME, oldName, name));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<SelectColumn> getColumns() {
		if (columns == null) {
			columns = new EObjectContainmentEList<SelectColumn>(SelectColumn.class, this, Ecore4regPackage.RULE_FOR_IL_TABLE_PART__COLUMNS);
		}
		return columns;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<TableFilter> getWhereClause() {
		if (whereClause == null) {
			whereClause = new EObjectContainmentEList<TableFilter>(TableFilter.class, this, Ecore4regPackage.RULE_FOR_IL_TABLE_PART__WHERE_CLAUSE);
		}
		return whereClause;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Ecore4regPackage.RULE_FOR_IL_TABLE_PART__COLUMNS:
				return ((InternalEList<?>)getColumns()).basicRemove(otherEnd, msgs);
			case Ecore4regPackage.RULE_FOR_IL_TABLE_PART__WHERE_CLAUSE:
				return ((InternalEList<?>)getWhereClause()).basicRemove(otherEnd, msgs);
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
			case Ecore4regPackage.RULE_FOR_IL_TABLE_PART__NAME:
				return getName();
			case Ecore4regPackage.RULE_FOR_IL_TABLE_PART__COLUMNS:
				return getColumns();
			case Ecore4regPackage.RULE_FOR_IL_TABLE_PART__WHERE_CLAUSE:
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
			case Ecore4regPackage.RULE_FOR_IL_TABLE_PART__NAME:
				setName((String)newValue);
				return;
			case Ecore4regPackage.RULE_FOR_IL_TABLE_PART__COLUMNS:
				getColumns().clear();
				getColumns().addAll((Collection<? extends SelectColumn>)newValue);
				return;
			case Ecore4regPackage.RULE_FOR_IL_TABLE_PART__WHERE_CLAUSE:
				getWhereClause().clear();
				getWhereClause().addAll((Collection<? extends TableFilter>)newValue);
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
			case Ecore4regPackage.RULE_FOR_IL_TABLE_PART__NAME:
				setName(NAME_EDEFAULT);
				return;
			case Ecore4regPackage.RULE_FOR_IL_TABLE_PART__COLUMNS:
				getColumns().clear();
				return;
			case Ecore4regPackage.RULE_FOR_IL_TABLE_PART__WHERE_CLAUSE:
				getWhereClause().clear();
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
			case Ecore4regPackage.RULE_FOR_IL_TABLE_PART__NAME:
				return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
			case Ecore4regPackage.RULE_FOR_IL_TABLE_PART__COLUMNS:
				return columns != null && !columns.isEmpty();
			case Ecore4regPackage.RULE_FOR_IL_TABLE_PART__WHERE_CLAUSE:
				return whereClause != null && !whereClause.isEmpty();
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
