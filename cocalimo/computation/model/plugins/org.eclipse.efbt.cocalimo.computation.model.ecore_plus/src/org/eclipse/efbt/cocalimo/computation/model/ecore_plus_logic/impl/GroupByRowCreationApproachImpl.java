/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.GroupByRowCreationApproach;
import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.ETypedElement;
import org.eclipse.emf.ecore.util.EDataTypeUniqueEList;
import org.eclipse.emf.ecore.util.EObjectResolvingEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Group By Row Creation Approach</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.GroupByRowCreationApproachImpl#getGroupByColumns <em>Group By Columns</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.GroupByRowCreationApproachImpl#getGroupByCubeColumnNames <em>Group By Cube Column Names</em>}</li>
 * </ul>
 *
 * @generated
 */
public class GroupByRowCreationApproachImpl extends RowCreationApproachImpl implements GroupByRowCreationApproach {
	/**
	 * The cached value of the '{@link #getGroupByColumns() <em>Group By Columns</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getGroupByColumns()
	 * @generated
	 * @ordered
	 */
	protected EList<ETypedElement> groupByColumns;

	/**
	 * The cached value of the '{@link #getGroupByCubeColumnNames() <em>Group By Cube Column Names</em>}' attribute list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getGroupByCubeColumnNames()
	 * @generated
	 * @ordered
	 */
	protected EList<String> groupByCubeColumnNames;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected GroupByRowCreationApproachImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Ecore_plus_logicPackage.Literals.GROUP_BY_ROW_CREATION_APPROACH;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<ETypedElement> getGroupByColumns() {
		if (groupByColumns == null) {
			groupByColumns = new EObjectResolvingEList<ETypedElement>(ETypedElement.class, this, Ecore_plus_logicPackage.GROUP_BY_ROW_CREATION_APPROACH__GROUP_BY_COLUMNS);
		}
		return groupByColumns;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<String> getGroupByCubeColumnNames() {
		if (groupByCubeColumnNames == null) {
			groupByCubeColumnNames = new EDataTypeUniqueEList<String>(String.class, this, Ecore_plus_logicPackage.GROUP_BY_ROW_CREATION_APPROACH__GROUP_BY_CUBE_COLUMN_NAMES);
		}
		return groupByCubeColumnNames;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Ecore_plus_logicPackage.GROUP_BY_ROW_CREATION_APPROACH__GROUP_BY_COLUMNS:
				return getGroupByColumns();
			case Ecore_plus_logicPackage.GROUP_BY_ROW_CREATION_APPROACH__GROUP_BY_CUBE_COLUMN_NAMES:
				return getGroupByCubeColumnNames();
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
			case Ecore_plus_logicPackage.GROUP_BY_ROW_CREATION_APPROACH__GROUP_BY_COLUMNS:
				getGroupByColumns().clear();
				getGroupByColumns().addAll((Collection<? extends ETypedElement>)newValue);
				return;
			case Ecore_plus_logicPackage.GROUP_BY_ROW_CREATION_APPROACH__GROUP_BY_CUBE_COLUMN_NAMES:
				getGroupByCubeColumnNames().clear();
				getGroupByCubeColumnNames().addAll((Collection<? extends String>)newValue);
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
			case Ecore_plus_logicPackage.GROUP_BY_ROW_CREATION_APPROACH__GROUP_BY_COLUMNS:
				getGroupByColumns().clear();
				return;
			case Ecore_plus_logicPackage.GROUP_BY_ROW_CREATION_APPROACH__GROUP_BY_CUBE_COLUMN_NAMES:
				getGroupByCubeColumnNames().clear();
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
			case Ecore_plus_logicPackage.GROUP_BY_ROW_CREATION_APPROACH__GROUP_BY_COLUMNS:
				return groupByColumns != null && !groupByColumns.isEmpty();
			case Ecore_plus_logicPackage.GROUP_BY_ROW_CREATION_APPROACH__GROUP_BY_CUBE_COLUMN_NAMES:
				return groupByCubeColumnNames != null && !groupByCubeColumnNames.isEmpty();
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
		result.append(" (groupByCubeColumnNames: ");
		result.append(groupByCubeColumnNames);
		result.append(')');
		return result.toString();
	}

} //GroupByRowCreationApproachImpl
