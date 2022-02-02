/**
 */
package entities.impl;

import entities.DerivedEntity;
import entities.EntitiesPackage;

import java.util.Collection;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;

import org.eclipse.emf.ecore.impl.EClassImpl;

import org.eclipse.emf.ecore.util.EObjectResolvingEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Derived Entity</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link entities.impl.DerivedEntityImpl#getSourceEntities <em>Source Entities</em>}</li>
 * </ul>
 *
 * @generated
 */
public class DerivedEntityImpl extends EClassImpl implements DerivedEntity {
	/**
	 * The cached value of the '{@link #getSourceEntities() <em>Source Entities</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSourceEntities()
	 * @generated
	 * @ordered
	 */
	protected EList<EClass> sourceEntities;

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
		return EntitiesPackage.Literals.DERIVED_ENTITY;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<EClass> getSourceEntities() {
		if (sourceEntities == null) {
			sourceEntities = new EObjectResolvingEList<EClass>(EClass.class, this, EntitiesPackage.DERIVED_ENTITY__SOURCE_ENTITIES);
		}
		return sourceEntities;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case EntitiesPackage.DERIVED_ENTITY__SOURCE_ENTITIES:
				return getSourceEntities();
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
			case EntitiesPackage.DERIVED_ENTITY__SOURCE_ENTITIES:
				getSourceEntities().clear();
				getSourceEntities().addAll((Collection<? extends EClass>)newValue);
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
			case EntitiesPackage.DERIVED_ENTITY__SOURCE_ENTITIES:
				getSourceEntities().clear();
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
			case EntitiesPackage.DERIVED_ENTITY__SOURCE_ENTITIES:
				return sourceEntities != null && !sourceEntities.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //DerivedEntityImpl
