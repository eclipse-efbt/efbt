/**
 */
package org.eclipse.efbt.cocalimo.logical_transformations.model.logical_transformations.impl;

import org.eclipse.efbt.cocalimo.logical_transformations.model.logical_transformations.Logical_transformationsPackage;
import org.eclipse.efbt.cocalimo.logical_transformations.model.logical_transformations.SelectionLayer;
import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.GeneratedEntity;
import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.ecore.EClass;

import org.eclipse.emf.ecore.InternalEObject;
import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Selection Layer</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.logical_transformations.model.logical_transformations.impl.SelectionLayerImpl#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.logical_transformations.model.logical_transformations.impl.SelectionLayerImpl#getInvisible <em>Invisible</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.logical_transformations.model.logical_transformations.impl.SelectionLayerImpl#getGeneratedEntity <em>Generated Entity</em>}</li>
 * </ul>
 *
 * @generated
 */
public class SelectionLayerImpl extends MinimalEObjectImpl.Container implements SelectionLayer {
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
	 * The default value of the '{@link #getInvisible() <em>Invisible</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getInvisible()
	 * @generated
	 * @ordered
	 */
	protected static final Boolean INVISIBLE_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getInvisible() <em>Invisible</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getInvisible()
	 * @generated
	 * @ordered
	 */
	protected Boolean invisible = INVISIBLE_EDEFAULT;

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
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected SelectionLayerImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Logical_transformationsPackage.Literals.SELECTION_LAYER;
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
			eNotify(new ENotificationImpl(this, Notification.SET, Logical_transformationsPackage.SELECTION_LAYER__NAME, oldName, name));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Boolean getInvisible() {
		return invisible;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setInvisible(Boolean newInvisible) {
		Boolean oldInvisible = invisible;
		invisible = newInvisible;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Logical_transformationsPackage.SELECTION_LAYER__INVISIBLE, oldInvisible, invisible));
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
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Logical_transformationsPackage.SELECTION_LAYER__GENERATED_ENTITY, oldGeneratedEntity, generatedEntity));
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
			eNotify(new ENotificationImpl(this, Notification.SET, Logical_transformationsPackage.SELECTION_LAYER__GENERATED_ENTITY, oldGeneratedEntity, generatedEntity));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Logical_transformationsPackage.SELECTION_LAYER__NAME:
				return getName();
			case Logical_transformationsPackage.SELECTION_LAYER__INVISIBLE:
				return getInvisible();
			case Logical_transformationsPackage.SELECTION_LAYER__GENERATED_ENTITY:
				if (resolve) return getGeneratedEntity();
				return basicGetGeneratedEntity();
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
			case Logical_transformationsPackage.SELECTION_LAYER__NAME:
				setName((String)newValue);
				return;
			case Logical_transformationsPackage.SELECTION_LAYER__INVISIBLE:
				setInvisible((Boolean)newValue);
				return;
			case Logical_transformationsPackage.SELECTION_LAYER__GENERATED_ENTITY:
				setGeneratedEntity((GeneratedEntity)newValue);
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
			case Logical_transformationsPackage.SELECTION_LAYER__NAME:
				setName(NAME_EDEFAULT);
				return;
			case Logical_transformationsPackage.SELECTION_LAYER__INVISIBLE:
				setInvisible(INVISIBLE_EDEFAULT);
				return;
			case Logical_transformationsPackage.SELECTION_LAYER__GENERATED_ENTITY:
				setGeneratedEntity((GeneratedEntity)null);
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
			case Logical_transformationsPackage.SELECTION_LAYER__NAME:
				return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
			case Logical_transformationsPackage.SELECTION_LAYER__INVISIBLE:
				return INVISIBLE_EDEFAULT == null ? invisible != null : !INVISIBLE_EDEFAULT.equals(invisible);
			case Logical_transformationsPackage.SELECTION_LAYER__GENERATED_ENTITY:
				return generatedEntity != null;
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
		result.append(", invisible: ");
		result.append(invisible);
		result.append(')');
		return result.toString();
	}

} //SelectionLayerImpl
