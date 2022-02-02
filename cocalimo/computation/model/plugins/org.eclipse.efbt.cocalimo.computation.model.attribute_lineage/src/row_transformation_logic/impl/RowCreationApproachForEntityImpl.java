/**
 */
package row_transformation_logic.impl;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

import row_transformation_logic.RowCreationApproach;
import row_transformation_logic.RowCreationApproachForEntity;
import row_transformation_logic.Row_transformation_logicPackage;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Row Creation Approach For Entity</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link row_transformation_logic.impl.RowCreationApproachForEntityImpl#getRowCreationApproach <em>Row Creation Approach</em>}</li>
 *   <li>{@link row_transformation_logic.impl.RowCreationApproachForEntityImpl#getEntity <em>Entity</em>}</li>
 *   <li>{@link row_transformation_logic.impl.RowCreationApproachForEntityImpl#getName <em>Name</em>}</li>
 * </ul>
 *
 * @generated
 */
public class RowCreationApproachForEntityImpl extends MinimalEObjectImpl.Container implements RowCreationApproachForEntity {
	/**
	 * The cached value of the '{@link #getRowCreationApproach() <em>Row Creation Approach</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getRowCreationApproach()
	 * @generated
	 * @ordered
	 */
	protected RowCreationApproach rowCreationApproach;

	/**
	 * The cached value of the '{@link #getEntity() <em>Entity</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getEntity()
	 * @generated
	 * @ordered
	 */
	protected EClass entity;

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
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected RowCreationApproachForEntityImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Row_transformation_logicPackage.Literals.ROW_CREATION_APPROACH_FOR_ENTITY;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public RowCreationApproach getRowCreationApproach() {
		return rowCreationApproach;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetRowCreationApproach(RowCreationApproach newRowCreationApproach, NotificationChain msgs) {
		RowCreationApproach oldRowCreationApproach = rowCreationApproach;
		rowCreationApproach = newRowCreationApproach;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Row_transformation_logicPackage.ROW_CREATION_APPROACH_FOR_ENTITY__ROW_CREATION_APPROACH, oldRowCreationApproach, newRowCreationApproach);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setRowCreationApproach(RowCreationApproach newRowCreationApproach) {
		if (newRowCreationApproach != rowCreationApproach) {
			NotificationChain msgs = null;
			if (rowCreationApproach != null)
				msgs = ((InternalEObject)rowCreationApproach).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Row_transformation_logicPackage.ROW_CREATION_APPROACH_FOR_ENTITY__ROW_CREATION_APPROACH, null, msgs);
			if (newRowCreationApproach != null)
				msgs = ((InternalEObject)newRowCreationApproach).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Row_transformation_logicPackage.ROW_CREATION_APPROACH_FOR_ENTITY__ROW_CREATION_APPROACH, null, msgs);
			msgs = basicSetRowCreationApproach(newRowCreationApproach, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Row_transformation_logicPackage.ROW_CREATION_APPROACH_FOR_ENTITY__ROW_CREATION_APPROACH, newRowCreationApproach, newRowCreationApproach));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getEntity() {
		if (entity != null && entity.eIsProxy()) {
			InternalEObject oldEntity = (InternalEObject)entity;
			entity = (EClass)eResolveProxy(oldEntity);
			if (entity != oldEntity) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Row_transformation_logicPackage.ROW_CREATION_APPROACH_FOR_ENTITY__ENTITY, oldEntity, entity));
			}
		}
		return entity;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass basicGetEntity() {
		return entity;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setEntity(EClass newEntity) {
		EClass oldEntity = entity;
		entity = newEntity;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Row_transformation_logicPackage.ROW_CREATION_APPROACH_FOR_ENTITY__ENTITY, oldEntity, entity));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Row_transformation_logicPackage.ROW_CREATION_APPROACH_FOR_ENTITY__NAME, oldName, name));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Row_transformation_logicPackage.ROW_CREATION_APPROACH_FOR_ENTITY__ROW_CREATION_APPROACH:
				return basicSetRowCreationApproach(null, msgs);
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
			case Row_transformation_logicPackage.ROW_CREATION_APPROACH_FOR_ENTITY__ROW_CREATION_APPROACH:
				return getRowCreationApproach();
			case Row_transformation_logicPackage.ROW_CREATION_APPROACH_FOR_ENTITY__ENTITY:
				if (resolve) return getEntity();
				return basicGetEntity();
			case Row_transformation_logicPackage.ROW_CREATION_APPROACH_FOR_ENTITY__NAME:
				return getName();
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
			case Row_transformation_logicPackage.ROW_CREATION_APPROACH_FOR_ENTITY__ROW_CREATION_APPROACH:
				setRowCreationApproach((RowCreationApproach)newValue);
				return;
			case Row_transformation_logicPackage.ROW_CREATION_APPROACH_FOR_ENTITY__ENTITY:
				setEntity((EClass)newValue);
				return;
			case Row_transformation_logicPackage.ROW_CREATION_APPROACH_FOR_ENTITY__NAME:
				setName((String)newValue);
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
			case Row_transformation_logicPackage.ROW_CREATION_APPROACH_FOR_ENTITY__ROW_CREATION_APPROACH:
				setRowCreationApproach((RowCreationApproach)null);
				return;
			case Row_transformation_logicPackage.ROW_CREATION_APPROACH_FOR_ENTITY__ENTITY:
				setEntity((EClass)null);
				return;
			case Row_transformation_logicPackage.ROW_CREATION_APPROACH_FOR_ENTITY__NAME:
				setName(NAME_EDEFAULT);
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
			case Row_transformation_logicPackage.ROW_CREATION_APPROACH_FOR_ENTITY__ROW_CREATION_APPROACH:
				return rowCreationApproach != null;
			case Row_transformation_logicPackage.ROW_CREATION_APPROACH_FOR_ENTITY__ENTITY:
				return entity != null;
			case Row_transformation_logicPackage.ROW_CREATION_APPROACH_FOR_ENTITY__NAME:
				return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
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

} //RowCreationApproachForEntityImpl
