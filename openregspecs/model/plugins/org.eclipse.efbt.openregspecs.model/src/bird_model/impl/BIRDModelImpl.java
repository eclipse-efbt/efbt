/**
 */
package bird_model.impl;

import bird_model.BIRDModel;
import bird_model.Bird_modelPackage;

import java.util.Collection;

import org.eclipse.efbt.openregspecs.model.bpmn_lite.SubProcess;
import org.eclipse.efbt.openregspecs.model.data_meta_model.EntityModule;

import org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.SMCubesCoreModel;

import org.eclipse.efbt.openregspecs.model.requirements_text.RequirementsModule;
import org.eclipse.efbt.openregspecs.model.sql_lite.SQLEntityModule;
import org.eclipse.efbt.openregspecs.model.testing.TestModule;
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
 *   <li>{@link bird_model.impl.BIRDModelImpl#getSmCubesCoreModel <em>Sm Cubes Core Model</em>}</li>
 *   <li>{@link bird_model.impl.BIRDModelImpl#getEntityModules <em>Entity Modules</em>}</li>
 *   <li>{@link bird_model.impl.BIRDModelImpl#getSQLEntityModules <em>SQL Entity Modules</em>}</li>
 *   <li>{@link bird_model.impl.BIRDModelImpl#getSubProcesses <em>Sub Processes</em>}</li>
 *   <li>{@link bird_model.impl.BIRDModelImpl#getRequirements <em>Requirements</em>}</li>
 *   <li>{@link bird_model.impl.BIRDModelImpl#getTests <em>Tests</em>}</li>
 * </ul>
 *
 * @generated
 */
public class BIRDModelImpl extends MinimalEObjectImpl.Container implements BIRDModel {
	/**
	 * The cached value of the '{@link #getSmCubesCoreModel() <em>Sm Cubes Core Model</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSmCubesCoreModel()
	 * @generated
	 * @ordered
	 */
	protected SMCubesCoreModel smCubesCoreModel;

	/**
	 * The cached value of the '{@link #getEntityModules() <em>Entity Modules</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getEntityModules()
	 * @generated
	 * @ordered
	 */
	protected EList<EntityModule> entityModules;

	/**
	 * The cached value of the '{@link #getSQLEntityModules() <em>SQL Entity Modules</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSQLEntityModules()
	 * @generated
	 * @ordered
	 */
	protected EList<SQLEntityModule> sqlEntityModules;

	/**
	 * The cached value of the '{@link #getSubProcesses() <em>Sub Processes</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSubProcesses()
	 * @generated
	 * @ordered
	 */
	protected EList<SubProcess> subProcesses;

	/**
	 * The cached value of the '{@link #getRequirements() <em>Requirements</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getRequirements()
	 * @generated
	 * @ordered
	 */
	protected EList<RequirementsModule> requirements;

	/**
	 * The cached value of the '{@link #getTests() <em>Tests</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTests()
	 * @generated
	 * @ordered
	 */
	protected EList<TestModule> tests;

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
	public SMCubesCoreModel getSmCubesCoreModel() {
		return smCubesCoreModel;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetSmCubesCoreModel(SMCubesCoreModel newSmCubesCoreModel, NotificationChain msgs) {
		SMCubesCoreModel oldSmCubesCoreModel = smCubesCoreModel;
		smCubesCoreModel = newSmCubesCoreModel;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Bird_modelPackage.BIRD_MODEL__SM_CUBES_CORE_MODEL, oldSmCubesCoreModel, newSmCubesCoreModel);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setSmCubesCoreModel(SMCubesCoreModel newSmCubesCoreModel) {
		if (newSmCubesCoreModel != smCubesCoreModel) {
			NotificationChain msgs = null;
			if (smCubesCoreModel != null)
				msgs = ((InternalEObject)smCubesCoreModel).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Bird_modelPackage.BIRD_MODEL__SM_CUBES_CORE_MODEL, null, msgs);
			if (newSmCubesCoreModel != null)
				msgs = ((InternalEObject)newSmCubesCoreModel).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Bird_modelPackage.BIRD_MODEL__SM_CUBES_CORE_MODEL, null, msgs);
			msgs = basicSetSmCubesCoreModel(newSmCubesCoreModel, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Bird_modelPackage.BIRD_MODEL__SM_CUBES_CORE_MODEL, newSmCubesCoreModel, newSmCubesCoreModel));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<EntityModule> getEntityModules() {
		if (entityModules == null) {
			entityModules = new EObjectContainmentEList<EntityModule>(EntityModule.class, this, Bird_modelPackage.BIRD_MODEL__ENTITY_MODULES);
		}
		return entityModules;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<SQLEntityModule> getSQLEntityModules() {
		if (sqlEntityModules == null) {
			sqlEntityModules = new EObjectContainmentEList<SQLEntityModule>(SQLEntityModule.class, this, Bird_modelPackage.BIRD_MODEL__SQL_ENTITY_MODULES);
		}
		return sqlEntityModules;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<SubProcess> getSubProcesses() {
		if (subProcesses == null) {
			subProcesses = new EObjectContainmentEList<SubProcess>(SubProcess.class, this, Bird_modelPackage.BIRD_MODEL__SUB_PROCESSES);
		}
		return subProcesses;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<RequirementsModule> getRequirements() {
		if (requirements == null) {
			requirements = new EObjectContainmentEList<RequirementsModule>(RequirementsModule.class, this, Bird_modelPackage.BIRD_MODEL__REQUIREMENTS);
		}
		return requirements;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<TestModule> getTests() {
		if (tests == null) {
			tests = new EObjectContainmentEList<TestModule>(TestModule.class, this, Bird_modelPackage.BIRD_MODEL__TESTS);
		}
		return tests;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Bird_modelPackage.BIRD_MODEL__SM_CUBES_CORE_MODEL:
				return basicSetSmCubesCoreModel(null, msgs);
			case Bird_modelPackage.BIRD_MODEL__ENTITY_MODULES:
				return ((InternalEList<?>)getEntityModules()).basicRemove(otherEnd, msgs);
			case Bird_modelPackage.BIRD_MODEL__SQL_ENTITY_MODULES:
				return ((InternalEList<?>)getSQLEntityModules()).basicRemove(otherEnd, msgs);
			case Bird_modelPackage.BIRD_MODEL__SUB_PROCESSES:
				return ((InternalEList<?>)getSubProcesses()).basicRemove(otherEnd, msgs);
			case Bird_modelPackage.BIRD_MODEL__REQUIREMENTS:
				return ((InternalEList<?>)getRequirements()).basicRemove(otherEnd, msgs);
			case Bird_modelPackage.BIRD_MODEL__TESTS:
				return ((InternalEList<?>)getTests()).basicRemove(otherEnd, msgs);
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
			case Bird_modelPackage.BIRD_MODEL__SM_CUBES_CORE_MODEL:
				return getSmCubesCoreModel();
			case Bird_modelPackage.BIRD_MODEL__ENTITY_MODULES:
				return getEntityModules();
			case Bird_modelPackage.BIRD_MODEL__SQL_ENTITY_MODULES:
				return getSQLEntityModules();
			case Bird_modelPackage.BIRD_MODEL__SUB_PROCESSES:
				return getSubProcesses();
			case Bird_modelPackage.BIRD_MODEL__REQUIREMENTS:
				return getRequirements();
			case Bird_modelPackage.BIRD_MODEL__TESTS:
				return getTests();
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
			case Bird_modelPackage.BIRD_MODEL__SM_CUBES_CORE_MODEL:
				setSmCubesCoreModel((SMCubesCoreModel)newValue);
				return;
			case Bird_modelPackage.BIRD_MODEL__ENTITY_MODULES:
				getEntityModules().clear();
				getEntityModules().addAll((Collection<? extends EntityModule>)newValue);
				return;
			case Bird_modelPackage.BIRD_MODEL__SQL_ENTITY_MODULES:
				getSQLEntityModules().clear();
				getSQLEntityModules().addAll((Collection<? extends SQLEntityModule>)newValue);
				return;
			case Bird_modelPackage.BIRD_MODEL__SUB_PROCESSES:
				getSubProcesses().clear();
				getSubProcesses().addAll((Collection<? extends SubProcess>)newValue);
				return;
			case Bird_modelPackage.BIRD_MODEL__REQUIREMENTS:
				getRequirements().clear();
				getRequirements().addAll((Collection<? extends RequirementsModule>)newValue);
				return;
			case Bird_modelPackage.BIRD_MODEL__TESTS:
				getTests().clear();
				getTests().addAll((Collection<? extends TestModule>)newValue);
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
			case Bird_modelPackage.BIRD_MODEL__SM_CUBES_CORE_MODEL:
				setSmCubesCoreModel((SMCubesCoreModel)null);
				return;
			case Bird_modelPackage.BIRD_MODEL__ENTITY_MODULES:
				getEntityModules().clear();
				return;
			case Bird_modelPackage.BIRD_MODEL__SQL_ENTITY_MODULES:
				getSQLEntityModules().clear();
				return;
			case Bird_modelPackage.BIRD_MODEL__SUB_PROCESSES:
				getSubProcesses().clear();
				return;
			case Bird_modelPackage.BIRD_MODEL__REQUIREMENTS:
				getRequirements().clear();
				return;
			case Bird_modelPackage.BIRD_MODEL__TESTS:
				getTests().clear();
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
			case Bird_modelPackage.BIRD_MODEL__SM_CUBES_CORE_MODEL:
				return smCubesCoreModel != null;
			case Bird_modelPackage.BIRD_MODEL__ENTITY_MODULES:
				return entityModules != null && !entityModules.isEmpty();
			case Bird_modelPackage.BIRD_MODEL__SQL_ENTITY_MODULES:
				return sqlEntityModules != null && !sqlEntityModules.isEmpty();
			case Bird_modelPackage.BIRD_MODEL__SUB_PROCESSES:
				return subProcesses != null && !subProcesses.isEmpty();
			case Bird_modelPackage.BIRD_MODEL__REQUIREMENTS:
				return requirements != null && !requirements.isEmpty();
			case Bird_modelPackage.BIRD_MODEL__TESTS:
				return tests != null && !tests.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //BIRDModelImpl
