/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl;

import java.util.Collection;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntity;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntityTable;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.EntityTable;
import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;
import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.util.EObjectResolvingEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Derived Entity Table</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.DerivedEntityTableImpl#getSourceTables <em>Source Tables</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.DerivedEntityTableImpl#getContainedEntityType <em>Contained Entity Type</em>}</li>
 * </ul>
 *
 * @generated
 */
public class DerivedEntityTableImpl extends EntityTableImpl implements DerivedEntityTable {
	/**
	 * The cached value of the '{@link #getSourceTables() <em>Source Tables</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSourceTables()
	 * @generated
	 * @ordered
	 */
	protected EList<EntityTable> sourceTables;

	/**
	 * The cached value of the '{@link #getContainedEntityType() <em>Contained Entity Type</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getContainedEntityType()
	 * @generated
	 * @ordered
	 */
	protected DerivedEntity containedEntityType;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected DerivedEntityTableImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Ecore_plus_logicPackage.Literals.DERIVED_ENTITY_TABLE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<EntityTable> getSourceTables() {
		if (sourceTables == null) {
			sourceTables = new EObjectResolvingEList<EntityTable>(EntityTable.class, this, Ecore_plus_logicPackage.DERIVED_ENTITY_TABLE__SOURCE_TABLES);
		}
		return sourceTables;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public DerivedEntity getContainedEntityType() {
		if (containedEntityType != null && containedEntityType.eIsProxy()) {
			InternalEObject oldContainedEntityType = (InternalEObject)containedEntityType;
			containedEntityType = (DerivedEntity)eResolveProxy(oldContainedEntityType);
			if (containedEntityType != oldContainedEntityType) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Ecore_plus_logicPackage.DERIVED_ENTITY_TABLE__CONTAINED_ENTITY_TYPE, oldContainedEntityType, containedEntityType));
			}
		}
		return containedEntityType;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public DerivedEntity basicGetContainedEntityType() {
		return containedEntityType;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setContainedEntityType(DerivedEntity newContainedEntityType) {
		DerivedEntity oldContainedEntityType = containedEntityType;
		containedEntityType = newContainedEntityType;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore_plus_logicPackage.DERIVED_ENTITY_TABLE__CONTAINED_ENTITY_TYPE, oldContainedEntityType, containedEntityType));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Ecore_plus_logicPackage.DERIVED_ENTITY_TABLE__SOURCE_TABLES:
				return getSourceTables();
			case Ecore_plus_logicPackage.DERIVED_ENTITY_TABLE__CONTAINED_ENTITY_TYPE:
				if (resolve) return getContainedEntityType();
				return basicGetContainedEntityType();
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
			case Ecore_plus_logicPackage.DERIVED_ENTITY_TABLE__SOURCE_TABLES:
				getSourceTables().clear();
				getSourceTables().addAll((Collection<? extends EntityTable>)newValue);
				return;
			case Ecore_plus_logicPackage.DERIVED_ENTITY_TABLE__CONTAINED_ENTITY_TYPE:
				setContainedEntityType((DerivedEntity)newValue);
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
			case Ecore_plus_logicPackage.DERIVED_ENTITY_TABLE__SOURCE_TABLES:
				getSourceTables().clear();
				return;
			case Ecore_plus_logicPackage.DERIVED_ENTITY_TABLE__CONTAINED_ENTITY_TYPE:
				setContainedEntityType((DerivedEntity)null);
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
			case Ecore_plus_logicPackage.DERIVED_ENTITY_TABLE__SOURCE_TABLES:
				return sourceTables != null && !sourceTables.isEmpty();
			case Ecore_plus_logicPackage.DERIVED_ENTITY_TABLE__CONTAINED_ENTITY_TYPE:
				return containedEntityType != null;
		}
		return super.eIsSet(featureID);
	}

} //DerivedEntityTableImpl
