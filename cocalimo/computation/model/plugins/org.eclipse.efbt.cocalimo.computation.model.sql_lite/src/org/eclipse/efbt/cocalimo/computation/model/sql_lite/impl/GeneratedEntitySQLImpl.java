/**
 */
package org.eclipse.efbt.cocalimo.computation.model.sql_lite.impl;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.GeneratedEntity;

import org.eclipse.efbt.cocalimo.computation.model.sql_lite.GeneratedEntitySQL;
import org.eclipse.efbt.cocalimo.computation.model.sql_lite.Sql_litePackage;
import org.eclipse.efbt.cocalimo.computation.model.sql_lite.View;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Generated Entity SQL</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.sql_lite.impl.GeneratedEntitySQLImpl#getGeneratedEntity <em>Generated Entity</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.sql_lite.impl.GeneratedEntitySQLImpl#getView <em>View</em>}</li>
 * </ul>
 *
 * @generated
 */
public class GeneratedEntitySQLImpl extends MinimalEObjectImpl.Container implements GeneratedEntitySQL {
	/**
	 * The cached value of the '{@link #getGeneratedEntity() <em>Generated Entity</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getGeneratedEntity()
	 * @generated
	 * @ordered
	 */
	protected GeneratedEntity generatedEntity;

	/**
	 * The cached value of the '{@link #getView() <em>View</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getView()
	 * @generated
	 * @ordered
	 */
	protected View view;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected GeneratedEntitySQLImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Sql_litePackage.Literals.GENERATED_ENTITY_SQL;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public GeneratedEntity getGeneratedEntity() {
		if (generatedEntity != null && generatedEntity.eIsProxy()) {
			InternalEObject oldGeneratedEntity = (InternalEObject)generatedEntity;
			generatedEntity = (GeneratedEntity)eResolveProxy(oldGeneratedEntity);
			if (generatedEntity != oldGeneratedEntity) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Sql_litePackage.GENERATED_ENTITY_SQL__GENERATED_ENTITY, oldGeneratedEntity, generatedEntity));
			}
		}
		return generatedEntity;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public GeneratedEntity basicGetGeneratedEntity() {
		return generatedEntity;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setGeneratedEntity(GeneratedEntity newGeneratedEntity) {
		GeneratedEntity oldGeneratedEntity = generatedEntity;
		generatedEntity = newGeneratedEntity;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Sql_litePackage.GENERATED_ENTITY_SQL__GENERATED_ENTITY, oldGeneratedEntity, generatedEntity));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public View getView() {
		if (view != null && view.eIsProxy()) {
			InternalEObject oldView = (InternalEObject)view;
			view = (View)eResolveProxy(oldView);
			if (view != oldView) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Sql_litePackage.GENERATED_ENTITY_SQL__VIEW, oldView, view));
			}
		}
		return view;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public View basicGetView() {
		return view;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setView(View newView) {
		View oldView = view;
		view = newView;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Sql_litePackage.GENERATED_ENTITY_SQL__VIEW, oldView, view));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Sql_litePackage.GENERATED_ENTITY_SQL__GENERATED_ENTITY:
				if (resolve) return getGeneratedEntity();
				return basicGetGeneratedEntity();
			case Sql_litePackage.GENERATED_ENTITY_SQL__VIEW:
				if (resolve) return getView();
				return basicGetView();
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
			case Sql_litePackage.GENERATED_ENTITY_SQL__GENERATED_ENTITY:
				setGeneratedEntity((GeneratedEntity)newValue);
				return;
			case Sql_litePackage.GENERATED_ENTITY_SQL__VIEW:
				setView((View)newValue);
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
			case Sql_litePackage.GENERATED_ENTITY_SQL__GENERATED_ENTITY:
				setGeneratedEntity((GeneratedEntity)null);
				return;
			case Sql_litePackage.GENERATED_ENTITY_SQL__VIEW:
				setView((View)null);
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
			case Sql_litePackage.GENERATED_ENTITY_SQL__GENERATED_ENTITY:
				return generatedEntity != null;
			case Sql_litePackage.GENERATED_ENTITY_SQL__VIEW:
				return view != null;
		}
		return super.eIsSet(featureID);
	}

} //GeneratedEntitySQLImpl
