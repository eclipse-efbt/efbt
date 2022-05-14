/**
 */
package org.eclipse.efbt.openregspecs.model.rendering.impl;

import org.eclipse.efbt.openregspecs.model.data_definition.COMBINATION;

import org.eclipse.efbt.openregspecs.model.rendering.RenderingPackage;
import org.eclipse.efbt.openregspecs.model.rendering.TABLE;
import org.eclipse.efbt.openregspecs.model.rendering.TABLE_CELL;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>TABLE CELL</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.rendering.impl.TABLE_CELLImpl#getCell_id <em>Cell id</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.rendering.impl.TABLE_CELLImpl#getCombination_id <em>Combination id</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.rendering.impl.TABLE_CELLImpl#getIs_shaded <em>Is shaded</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.rendering.impl.TABLE_CELLImpl#getTable_id <em>Table id</em>}</li>
 * </ul>
 *
 * @generated
 */
public class TABLE_CELLImpl extends MinimalEObjectImpl.Container implements TABLE_CELL {
	/**
	 * The default value of the '{@link #getCell_id() <em>Cell id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCell_id()
	 * @generated
	 * @ordered
	 */
	protected static final String CELL_ID_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getCell_id() <em>Cell id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCell_id()
	 * @generated
	 * @ordered
	 */
	protected String cell_id = CELL_ID_EDEFAULT;

	/**
	 * The cached value of the '{@link #getCombination_id() <em>Combination id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCombination_id()
	 * @generated
	 * @ordered
	 */
	protected COMBINATION combination_id;

	/**
	 * The default value of the '{@link #getIs_shaded() <em>Is shaded</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getIs_shaded()
	 * @generated
	 * @ordered
	 */
	protected static final Boolean IS_SHADED_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getIs_shaded() <em>Is shaded</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getIs_shaded()
	 * @generated
	 * @ordered
	 */
	protected Boolean is_shaded = IS_SHADED_EDEFAULT;

	/**
	 * The cached value of the '{@link #getTable_id() <em>Table id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTable_id()
	 * @generated
	 * @ordered
	 */
	protected TABLE table_id;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected TABLE_CELLImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return RenderingPackage.Literals.TABLE_CELL;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getCell_id() {
		return cell_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setCell_id(String newCell_id) {
		String oldCell_id = cell_id;
		cell_id = newCell_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, RenderingPackage.TABLE_CELL__CELL_ID, oldCell_id, cell_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public COMBINATION getCombination_id() {
		if (combination_id != null && combination_id.eIsProxy()) {
			InternalEObject oldCombination_id = (InternalEObject)combination_id;
			combination_id = (COMBINATION)eResolveProxy(oldCombination_id);
			if (combination_id != oldCombination_id) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, RenderingPackage.TABLE_CELL__COMBINATION_ID, oldCombination_id, combination_id));
			}
		}
		return combination_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public COMBINATION basicGetCombination_id() {
		return combination_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setCombination_id(COMBINATION newCombination_id) {
		COMBINATION oldCombination_id = combination_id;
		combination_id = newCombination_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, RenderingPackage.TABLE_CELL__COMBINATION_ID, oldCombination_id, combination_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Boolean getIs_shaded() {
		return is_shaded;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setIs_shaded(Boolean newIs_shaded) {
		Boolean oldIs_shaded = is_shaded;
		is_shaded = newIs_shaded;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, RenderingPackage.TABLE_CELL__IS_SHADED, oldIs_shaded, is_shaded));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TABLE getTable_id() {
		if (table_id != null && table_id.eIsProxy()) {
			InternalEObject oldTable_id = (InternalEObject)table_id;
			table_id = (TABLE)eResolveProxy(oldTable_id);
			if (table_id != oldTable_id) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, RenderingPackage.TABLE_CELL__TABLE_ID, oldTable_id, table_id));
			}
		}
		return table_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TABLE basicGetTable_id() {
		return table_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setTable_id(TABLE newTable_id) {
		TABLE oldTable_id = table_id;
		table_id = newTable_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, RenderingPackage.TABLE_CELL__TABLE_ID, oldTable_id, table_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case RenderingPackage.TABLE_CELL__CELL_ID:
				return getCell_id();
			case RenderingPackage.TABLE_CELL__COMBINATION_ID:
				if (resolve) return getCombination_id();
				return basicGetCombination_id();
			case RenderingPackage.TABLE_CELL__IS_SHADED:
				return getIs_shaded();
			case RenderingPackage.TABLE_CELL__TABLE_ID:
				if (resolve) return getTable_id();
				return basicGetTable_id();
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
			case RenderingPackage.TABLE_CELL__CELL_ID:
				setCell_id((String)newValue);
				return;
			case RenderingPackage.TABLE_CELL__COMBINATION_ID:
				setCombination_id((COMBINATION)newValue);
				return;
			case RenderingPackage.TABLE_CELL__IS_SHADED:
				setIs_shaded((Boolean)newValue);
				return;
			case RenderingPackage.TABLE_CELL__TABLE_ID:
				setTable_id((TABLE)newValue);
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
			case RenderingPackage.TABLE_CELL__CELL_ID:
				setCell_id(CELL_ID_EDEFAULT);
				return;
			case RenderingPackage.TABLE_CELL__COMBINATION_ID:
				setCombination_id((COMBINATION)null);
				return;
			case RenderingPackage.TABLE_CELL__IS_SHADED:
				setIs_shaded(IS_SHADED_EDEFAULT);
				return;
			case RenderingPackage.TABLE_CELL__TABLE_ID:
				setTable_id((TABLE)null);
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
			case RenderingPackage.TABLE_CELL__CELL_ID:
				return CELL_ID_EDEFAULT == null ? cell_id != null : !CELL_ID_EDEFAULT.equals(cell_id);
			case RenderingPackage.TABLE_CELL__COMBINATION_ID:
				return combination_id != null;
			case RenderingPackage.TABLE_CELL__IS_SHADED:
				return IS_SHADED_EDEFAULT == null ? is_shaded != null : !IS_SHADED_EDEFAULT.equals(is_shaded);
			case RenderingPackage.TABLE_CELL__TABLE_ID:
				return table_id != null;
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
		result.append(" (cell_id: ");
		result.append(cell_id);
		result.append(", is_shaded: ");
		result.append(is_shaded);
		result.append(')');
		return result.toString();
	}

} //TABLE_CELLImpl
