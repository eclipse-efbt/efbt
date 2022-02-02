/**
 */
package row_transformation_logic.impl;

import java.util.Collection;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

import org.eclipse.emf.ecore.util.EObjectResolvingEList;

import row_transformation_logic.GroupByRowCreationApproach;
import row_transformation_logic.Row_transformation_logicPackage;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Group By Row Creation Approach</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link row_transformation_logic.impl.GroupByRowCreationApproachImpl#getGroupByCubeColumnNames <em>Group By Cube Column Names</em>}</li>
 *   <li>{@link row_transformation_logic.impl.GroupByRowCreationApproachImpl#getGroupByColumns <em>Group By Columns</em>}</li>
 * </ul>
 *
 * @generated
 */
public class GroupByRowCreationApproachImpl extends RowCreationApproachImpl implements GroupByRowCreationApproach {
	/**
	 * The cached value of the '{@link #getGroupByCubeColumnNames() <em>Group By Cube Column Names</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getGroupByCubeColumnNames()
	 * @generated
	 * @ordered
	 */
	protected EList<EAttribute> groupByCubeColumnNames;

	/**
	 * The cached value of the '{@link #getGroupByColumns() <em>Group By Columns</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getGroupByColumns()
	 * @generated
	 * @ordered
	 */
	protected EAttribute groupByColumns;

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
		return Row_transformation_logicPackage.Literals.GROUP_BY_ROW_CREATION_APPROACH;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<EAttribute> getGroupByCubeColumnNames() {
		if (groupByCubeColumnNames == null) {
			groupByCubeColumnNames = new EObjectResolvingEList<EAttribute>(EAttribute.class, this, Row_transformation_logicPackage.GROUP_BY_ROW_CREATION_APPROACH__GROUP_BY_CUBE_COLUMN_NAMES);
		}
		return groupByCubeColumnNames;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getGroupByColumns() {
		if (groupByColumns != null && groupByColumns.eIsProxy()) {
			InternalEObject oldGroupByColumns = (InternalEObject)groupByColumns;
			groupByColumns = (EAttribute)eResolveProxy(oldGroupByColumns);
			if (groupByColumns != oldGroupByColumns) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Row_transformation_logicPackage.GROUP_BY_ROW_CREATION_APPROACH__GROUP_BY_COLUMNS, oldGroupByColumns, groupByColumns));
			}
		}
		return groupByColumns;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute basicGetGroupByColumns() {
		return groupByColumns;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setGroupByColumns(EAttribute newGroupByColumns) {
		EAttribute oldGroupByColumns = groupByColumns;
		groupByColumns = newGroupByColumns;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Row_transformation_logicPackage.GROUP_BY_ROW_CREATION_APPROACH__GROUP_BY_COLUMNS, oldGroupByColumns, groupByColumns));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Row_transformation_logicPackage.GROUP_BY_ROW_CREATION_APPROACH__GROUP_BY_CUBE_COLUMN_NAMES:
				return getGroupByCubeColumnNames();
			case Row_transformation_logicPackage.GROUP_BY_ROW_CREATION_APPROACH__GROUP_BY_COLUMNS:
				if (resolve) return getGroupByColumns();
				return basicGetGroupByColumns();
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
			case Row_transformation_logicPackage.GROUP_BY_ROW_CREATION_APPROACH__GROUP_BY_CUBE_COLUMN_NAMES:
				getGroupByCubeColumnNames().clear();
				getGroupByCubeColumnNames().addAll((Collection<? extends EAttribute>)newValue);
				return;
			case Row_transformation_logicPackage.GROUP_BY_ROW_CREATION_APPROACH__GROUP_BY_COLUMNS:
				setGroupByColumns((EAttribute)newValue);
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
			case Row_transformation_logicPackage.GROUP_BY_ROW_CREATION_APPROACH__GROUP_BY_CUBE_COLUMN_NAMES:
				getGroupByCubeColumnNames().clear();
				return;
			case Row_transformation_logicPackage.GROUP_BY_ROW_CREATION_APPROACH__GROUP_BY_COLUMNS:
				setGroupByColumns((EAttribute)null);
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
			case Row_transformation_logicPackage.GROUP_BY_ROW_CREATION_APPROACH__GROUP_BY_CUBE_COLUMN_NAMES:
				return groupByCubeColumnNames != null && !groupByCubeColumnNames.isEmpty();
			case Row_transformation_logicPackage.GROUP_BY_ROW_CREATION_APPROACH__GROUP_BY_COLUMNS:
				return groupByColumns != null;
		}
		return super.eIsSet(featureID);
	}

} //GroupByRowCreationApproachImpl
