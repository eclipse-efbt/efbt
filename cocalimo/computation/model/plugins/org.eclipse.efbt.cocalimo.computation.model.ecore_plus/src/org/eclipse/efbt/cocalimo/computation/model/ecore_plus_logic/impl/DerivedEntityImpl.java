/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedAttribute;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntity;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedStruct;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.RowCreationApproachForEntity;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.Entity;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.impl.EntityImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.CUBE;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.EObjectResolvingEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Derived Entity</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.DerivedEntityImpl#getSourceEntities <em>Source Entities</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.DerivedEntityImpl#getDerivedAttributes <em>Derived Attributes</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.DerivedEntityImpl#getRowCreationApproachForEntity <em>Row Creation Approach For Entity</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.DerivedEntityImpl#getCube <em>Cube</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.DerivedEntityImpl#getDerivedStructs <em>Derived Structs</em>}</li>
 * </ul>
 *
 * @generated
 */
public class DerivedEntityImpl extends EntityImpl implements DerivedEntity {
	/**
	 * The cached value of the '{@link #getSourceEntities() <em>Source Entities</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSourceEntities()
	 * @generated
	 * @ordered
	 */
	protected EList<Entity> sourceEntities;

	/**
	 * The cached value of the '{@link #getDerivedAttributes() <em>Derived Attributes</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getDerivedAttributes()
	 * @generated
	 * @ordered
	 */
	protected EList<DerivedAttribute> derivedAttributes;

	/**
	 * The cached value of the '{@link #getRowCreationApproachForEntity() <em>Row Creation Approach For Entity</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getRowCreationApproachForEntity()
	 * @generated
	 * @ordered
	 */
	protected RowCreationApproachForEntity rowCreationApproachForEntity;

	/**
	 * The cached value of the '{@link #getCube() <em>Cube</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCube()
	 * @generated
	 * @ordered
	 */
	protected CUBE cube;

	/**
	 * The cached value of the '{@link #getDerivedStructs() <em>Derived Structs</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getDerivedStructs()
	 * @generated
	 * @ordered
	 */
	protected DerivedStruct derivedStructs;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected DerivedEntityImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Ecore_plus_logicPackage.Literals.DERIVED_ENTITY;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<Entity> getSourceEntities() {
		if (sourceEntities == null) {
			sourceEntities = new EObjectResolvingEList<Entity>(Entity.class, this, Ecore_plus_logicPackage.DERIVED_ENTITY__SOURCE_ENTITIES);
		}
		return sourceEntities;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<DerivedAttribute> getDerivedAttributes() {
		if (derivedAttributes == null) {
			derivedAttributes = new EObjectContainmentEList<DerivedAttribute>(DerivedAttribute.class, this, Ecore_plus_logicPackage.DERIVED_ENTITY__DERIVED_ATTRIBUTES);
		}
		return derivedAttributes;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public RowCreationApproachForEntity getRowCreationApproachForEntity() {
		return rowCreationApproachForEntity;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetRowCreationApproachForEntity(RowCreationApproachForEntity newRowCreationApproachForEntity, NotificationChain msgs) {
		RowCreationApproachForEntity oldRowCreationApproachForEntity = rowCreationApproachForEntity;
		rowCreationApproachForEntity = newRowCreationApproachForEntity;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Ecore_plus_logicPackage.DERIVED_ENTITY__ROW_CREATION_APPROACH_FOR_ENTITY, oldRowCreationApproachForEntity, newRowCreationApproachForEntity);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setRowCreationApproachForEntity(RowCreationApproachForEntity newRowCreationApproachForEntity) {
		if (newRowCreationApproachForEntity != rowCreationApproachForEntity) {
			NotificationChain msgs = null;
			if (rowCreationApproachForEntity != null)
				msgs = ((InternalEObject)rowCreationApproachForEntity).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Ecore_plus_logicPackage.DERIVED_ENTITY__ROW_CREATION_APPROACH_FOR_ENTITY, null, msgs);
			if (newRowCreationApproachForEntity != null)
				msgs = ((InternalEObject)newRowCreationApproachForEntity).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Ecore_plus_logicPackage.DERIVED_ENTITY__ROW_CREATION_APPROACH_FOR_ENTITY, null, msgs);
			msgs = basicSetRowCreationApproachForEntity(newRowCreationApproachForEntity, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore_plus_logicPackage.DERIVED_ENTITY__ROW_CREATION_APPROACH_FOR_ENTITY, newRowCreationApproachForEntity, newRowCreationApproachForEntity));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CUBE getCube() {
		if (cube != null && cube.eIsProxy()) {
			InternalEObject oldCube = (InternalEObject)cube;
			cube = (CUBE)eResolveProxy(oldCube);
			if (cube != oldCube) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Ecore_plus_logicPackage.DERIVED_ENTITY__CUBE, oldCube, cube));
			}
		}
		return cube;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CUBE basicGetCube() {
		return cube;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setCube(CUBE newCube) {
		CUBE oldCube = cube;
		cube = newCube;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore_plus_logicPackage.DERIVED_ENTITY__CUBE, oldCube, cube));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public DerivedStruct getDerivedStructs() {
		return derivedStructs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetDerivedStructs(DerivedStruct newDerivedStructs, NotificationChain msgs) {
		DerivedStruct oldDerivedStructs = derivedStructs;
		derivedStructs = newDerivedStructs;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Ecore_plus_logicPackage.DERIVED_ENTITY__DERIVED_STRUCTS, oldDerivedStructs, newDerivedStructs);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setDerivedStructs(DerivedStruct newDerivedStructs) {
		if (newDerivedStructs != derivedStructs) {
			NotificationChain msgs = null;
			if (derivedStructs != null)
				msgs = ((InternalEObject)derivedStructs).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Ecore_plus_logicPackage.DERIVED_ENTITY__DERIVED_STRUCTS, null, msgs);
			if (newDerivedStructs != null)
				msgs = ((InternalEObject)newDerivedStructs).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Ecore_plus_logicPackage.DERIVED_ENTITY__DERIVED_STRUCTS, null, msgs);
			msgs = basicSetDerivedStructs(newDerivedStructs, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore_plus_logicPackage.DERIVED_ENTITY__DERIVED_STRUCTS, newDerivedStructs, newDerivedStructs));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Ecore_plus_logicPackage.DERIVED_ENTITY__DERIVED_ATTRIBUTES:
				return ((InternalEList<?>)getDerivedAttributes()).basicRemove(otherEnd, msgs);
			case Ecore_plus_logicPackage.DERIVED_ENTITY__ROW_CREATION_APPROACH_FOR_ENTITY:
				return basicSetRowCreationApproachForEntity(null, msgs);
			case Ecore_plus_logicPackage.DERIVED_ENTITY__DERIVED_STRUCTS:
				return basicSetDerivedStructs(null, msgs);
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
			case Ecore_plus_logicPackage.DERIVED_ENTITY__SOURCE_ENTITIES:
				return getSourceEntities();
			case Ecore_plus_logicPackage.DERIVED_ENTITY__DERIVED_ATTRIBUTES:
				return getDerivedAttributes();
			case Ecore_plus_logicPackage.DERIVED_ENTITY__ROW_CREATION_APPROACH_FOR_ENTITY:
				return getRowCreationApproachForEntity();
			case Ecore_plus_logicPackage.DERIVED_ENTITY__CUBE:
				if (resolve) return getCube();
				return basicGetCube();
			case Ecore_plus_logicPackage.DERIVED_ENTITY__DERIVED_STRUCTS:
				return getDerivedStructs();
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
			case Ecore_plus_logicPackage.DERIVED_ENTITY__SOURCE_ENTITIES:
				getSourceEntities().clear();
				getSourceEntities().addAll((Collection<? extends Entity>)newValue);
				return;
			case Ecore_plus_logicPackage.DERIVED_ENTITY__DERIVED_ATTRIBUTES:
				getDerivedAttributes().clear();
				getDerivedAttributes().addAll((Collection<? extends DerivedAttribute>)newValue);
				return;
			case Ecore_plus_logicPackage.DERIVED_ENTITY__ROW_CREATION_APPROACH_FOR_ENTITY:
				setRowCreationApproachForEntity((RowCreationApproachForEntity)newValue);
				return;
			case Ecore_plus_logicPackage.DERIVED_ENTITY__CUBE:
				setCube((CUBE)newValue);
				return;
			case Ecore_plus_logicPackage.DERIVED_ENTITY__DERIVED_STRUCTS:
				setDerivedStructs((DerivedStruct)newValue);
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
			case Ecore_plus_logicPackage.DERIVED_ENTITY__SOURCE_ENTITIES:
				getSourceEntities().clear();
				return;
			case Ecore_plus_logicPackage.DERIVED_ENTITY__DERIVED_ATTRIBUTES:
				getDerivedAttributes().clear();
				return;
			case Ecore_plus_logicPackage.DERIVED_ENTITY__ROW_CREATION_APPROACH_FOR_ENTITY:
				setRowCreationApproachForEntity((RowCreationApproachForEntity)null);
				return;
			case Ecore_plus_logicPackage.DERIVED_ENTITY__CUBE:
				setCube((CUBE)null);
				return;
			case Ecore_plus_logicPackage.DERIVED_ENTITY__DERIVED_STRUCTS:
				setDerivedStructs((DerivedStruct)null);
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
			case Ecore_plus_logicPackage.DERIVED_ENTITY__SOURCE_ENTITIES:
				return sourceEntities != null && !sourceEntities.isEmpty();
			case Ecore_plus_logicPackage.DERIVED_ENTITY__DERIVED_ATTRIBUTES:
				return derivedAttributes != null && !derivedAttributes.isEmpty();
			case Ecore_plus_logicPackage.DERIVED_ENTITY__ROW_CREATION_APPROACH_FOR_ENTITY:
				return rowCreationApproachForEntity != null;
			case Ecore_plus_logicPackage.DERIVED_ENTITY__CUBE:
				return cube != null;
			case Ecore_plus_logicPackage.DERIVED_ENTITY__DERIVED_STRUCTS:
				return derivedStructs != null;
		}
		return super.eIsSet(featureID);
	}

} //DerivedEntityImpl
