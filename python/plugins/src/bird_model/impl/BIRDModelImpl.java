/**
 */
package bird_model.impl;

import bird_model.BIRDModel;
import bird_model.Bird_modelPackage;
import bird_model.EntityModule;
import bird_model.SMCubesCoreModel;

import java.util.Collection;

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
 * An implementation of the model object '<em><b>BIRD Model</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link bird_model.impl.BIRDModelImpl#getEntityModule <em>Entity Module</em>}</li>
 *   <li>{@link bird_model.impl.BIRDModelImpl#getSmcubesCoreModel <em>Smcubes Core Model</em>}</li>
 * </ul>
 *
 * @generated
 */
public class BIRDModelImpl extends MinimalEObjectImpl.Container implements BIRDModel {
	/**
	 * The cached value of the '{@link #getEntityModule() <em>Entity Module</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getEntityModule()
	 * @generated
	 * @ordered
	 */
	protected EList<EntityModule> entityModule;

	/**
	 * The cached value of the '{@link #getSmcubesCoreModel() <em>Smcubes Core Model</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSmcubesCoreModel()
	 * @generated
	 * @ordered
	 */
	protected SMCubesCoreModel smcubesCoreModel;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected BIRDModelImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Bird_modelPackage.Literals.BIRD_MODEL;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<EntityModule> getEntityModule() {
		if (entityModule == null) {
			entityModule = new EObjectContainmentEList<EntityModule>(EntityModule.class, this, Bird_modelPackage.BIRD_MODEL__ENTITY_MODULE);
		}
		return entityModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SMCubesCoreModel getSmcubesCoreModel() {
		return smcubesCoreModel;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetSmcubesCoreModel(SMCubesCoreModel newSmcubesCoreModel, NotificationChain msgs) {
		SMCubesCoreModel oldSmcubesCoreModel = smcubesCoreModel;
		smcubesCoreModel = newSmcubesCoreModel;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Bird_modelPackage.BIRD_MODEL__SMCUBES_CORE_MODEL, oldSmcubesCoreModel, newSmcubesCoreModel);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setSmcubesCoreModel(SMCubesCoreModel newSmcubesCoreModel) {
		if (newSmcubesCoreModel != smcubesCoreModel) {
			NotificationChain msgs = null;
			if (smcubesCoreModel != null)
				msgs = ((InternalEObject)smcubesCoreModel).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Bird_modelPackage.BIRD_MODEL__SMCUBES_CORE_MODEL, null, msgs);
			if (newSmcubesCoreModel != null)
				msgs = ((InternalEObject)newSmcubesCoreModel).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Bird_modelPackage.BIRD_MODEL__SMCUBES_CORE_MODEL, null, msgs);
			msgs = basicSetSmcubesCoreModel(newSmcubesCoreModel, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Bird_modelPackage.BIRD_MODEL__SMCUBES_CORE_MODEL, newSmcubesCoreModel, newSmcubesCoreModel));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Bird_modelPackage.BIRD_MODEL__ENTITY_MODULE:
				return ((InternalEList<?>)getEntityModule()).basicRemove(otherEnd, msgs);
			case Bird_modelPackage.BIRD_MODEL__SMCUBES_CORE_MODEL:
				return basicSetSmcubesCoreModel(null, msgs);
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
			case Bird_modelPackage.BIRD_MODEL__ENTITY_MODULE:
				return getEntityModule();
			case Bird_modelPackage.BIRD_MODEL__SMCUBES_CORE_MODEL:
				return getSmcubesCoreModel();
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
			case Bird_modelPackage.BIRD_MODEL__ENTITY_MODULE:
				getEntityModule().clear();
				getEntityModule().addAll((Collection<? extends EntityModule>)newValue);
				return;
			case Bird_modelPackage.BIRD_MODEL__SMCUBES_CORE_MODEL:
				setSmcubesCoreModel((SMCubesCoreModel)newValue);
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
			case Bird_modelPackage.BIRD_MODEL__ENTITY_MODULE:
				getEntityModule().clear();
				return;
			case Bird_modelPackage.BIRD_MODEL__SMCUBES_CORE_MODEL:
				setSmcubesCoreModel((SMCubesCoreModel)null);
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
			case Bird_modelPackage.BIRD_MODEL__ENTITY_MODULE:
				return entityModule != null && !entityModule.isEmpty();
			case Bird_modelPackage.BIRD_MODEL__SMCUBES_CORE_MODEL:
				return smcubesCoreModel != null;
		}
		return super.eIsSet(featureID);
	}

} //BIRDModelImpl
