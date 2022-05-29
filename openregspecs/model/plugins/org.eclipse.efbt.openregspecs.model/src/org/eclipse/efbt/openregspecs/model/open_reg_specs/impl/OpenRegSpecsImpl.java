/**
 */
package org.eclipse.efbt.openregspecs.model.open_reg_specs.impl;

import java.util.Collection;

import org.eclipse.efbt.openregspecs.model.bpmn_lite.SubProcess;

import org.eclipse.efbt.openregspecs.model.data_meta_model.EntityModule;

import org.eclipse.efbt.openregspecs.model.open_reg_specs.OpenRegSpecs;
import org.eclipse.efbt.openregspecs.model.open_reg_specs.Open_reg_specsPackage;

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
 * An implementation of the model object '<em><b>Open Reg Specs</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.open_reg_specs.impl.OpenRegSpecsImpl#getRequirements <em>Requirements</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.open_reg_specs.impl.OpenRegSpecsImpl#getTypes_and_concepts <em>Types and concepts</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.open_reg_specs.impl.OpenRegSpecsImpl#getData_model <em>Data model</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.open_reg_specs.impl.OpenRegSpecsImpl#getProcess_workflow <em>Process workflow</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.open_reg_specs.impl.OpenRegSpecsImpl#getReport_generation <em>Report generation</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.open_reg_specs.impl.OpenRegSpecsImpl#getTests <em>Tests</em>}</li>
 * </ul>
 *
 * @generated
 */
public class OpenRegSpecsImpl extends MinimalEObjectImpl.Container implements OpenRegSpecs {
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
	 * The cached value of the '{@link #getTypes_and_concepts() <em>Types and concepts</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTypes_and_concepts()
	 * @generated
	 * @ordered
	 */
	protected SMCubesCoreModel types_and_concepts;

	/**
	 * The cached value of the '{@link #getData_model() <em>Data model</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getData_model()
	 * @generated
	 * @ordered
	 */
	protected EList<EntityModule> data_model;

	/**
	 * The cached value of the '{@link #getProcess_workflow() <em>Process workflow</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getProcess_workflow()
	 * @generated
	 * @ordered
	 */
	protected EList<SubProcess> process_workflow;

	/**
	 * The cached value of the '{@link #getReport_generation() <em>Report generation</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getReport_generation()
	 * @generated
	 * @ordered
	 */
	protected EList<SQLEntityModule> report_generation;

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
	protected OpenRegSpecsImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Open_reg_specsPackage.Literals.OPEN_REG_SPECS;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<RequirementsModule> getRequirements() {
		if (requirements == null) {
			requirements = new EObjectContainmentEList<RequirementsModule>(RequirementsModule.class, this, Open_reg_specsPackage.OPEN_REG_SPECS__REQUIREMENTS);
		}
		return requirements;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SMCubesCoreModel getTypes_and_concepts() {
		return types_and_concepts;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetTypes_and_concepts(SMCubesCoreModel newTypes_and_concepts, NotificationChain msgs) {
		SMCubesCoreModel oldTypes_and_concepts = types_and_concepts;
		types_and_concepts = newTypes_and_concepts;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Open_reg_specsPackage.OPEN_REG_SPECS__TYPES_AND_CONCEPTS, oldTypes_and_concepts, newTypes_and_concepts);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setTypes_and_concepts(SMCubesCoreModel newTypes_and_concepts) {
		if (newTypes_and_concepts != types_and_concepts) {
			NotificationChain msgs = null;
			if (types_and_concepts != null)
				msgs = ((InternalEObject)types_and_concepts).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Open_reg_specsPackage.OPEN_REG_SPECS__TYPES_AND_CONCEPTS, null, msgs);
			if (newTypes_and_concepts != null)
				msgs = ((InternalEObject)newTypes_and_concepts).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Open_reg_specsPackage.OPEN_REG_SPECS__TYPES_AND_CONCEPTS, null, msgs);
			msgs = basicSetTypes_and_concepts(newTypes_and_concepts, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Open_reg_specsPackage.OPEN_REG_SPECS__TYPES_AND_CONCEPTS, newTypes_and_concepts, newTypes_and_concepts));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<EntityModule> getData_model() {
		if (data_model == null) {
			data_model = new EObjectContainmentEList<EntityModule>(EntityModule.class, this, Open_reg_specsPackage.OPEN_REG_SPECS__DATA_MODEL);
		}
		return data_model;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<SubProcess> getProcess_workflow() {
		if (process_workflow == null) {
			process_workflow = new EObjectContainmentEList<SubProcess>(SubProcess.class, this, Open_reg_specsPackage.OPEN_REG_SPECS__PROCESS_WORKFLOW);
		}
		return process_workflow;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<SQLEntityModule> getReport_generation() {
		if (report_generation == null) {
			report_generation = new EObjectContainmentEList<SQLEntityModule>(SQLEntityModule.class, this, Open_reg_specsPackage.OPEN_REG_SPECS__REPORT_GENERATION);
		}
		return report_generation;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<TestModule> getTests() {
		if (tests == null) {
			tests = new EObjectContainmentEList<TestModule>(TestModule.class, this, Open_reg_specsPackage.OPEN_REG_SPECS__TESTS);
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
			case Open_reg_specsPackage.OPEN_REG_SPECS__REQUIREMENTS:
				return ((InternalEList<?>)getRequirements()).basicRemove(otherEnd, msgs);
			case Open_reg_specsPackage.OPEN_REG_SPECS__TYPES_AND_CONCEPTS:
				return basicSetTypes_and_concepts(null, msgs);
			case Open_reg_specsPackage.OPEN_REG_SPECS__DATA_MODEL:
				return ((InternalEList<?>)getData_model()).basicRemove(otherEnd, msgs);
			case Open_reg_specsPackage.OPEN_REG_SPECS__PROCESS_WORKFLOW:
				return ((InternalEList<?>)getProcess_workflow()).basicRemove(otherEnd, msgs);
			case Open_reg_specsPackage.OPEN_REG_SPECS__REPORT_GENERATION:
				return ((InternalEList<?>)getReport_generation()).basicRemove(otherEnd, msgs);
			case Open_reg_specsPackage.OPEN_REG_SPECS__TESTS:
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
			case Open_reg_specsPackage.OPEN_REG_SPECS__REQUIREMENTS:
				return getRequirements();
			case Open_reg_specsPackage.OPEN_REG_SPECS__TYPES_AND_CONCEPTS:
				return getTypes_and_concepts();
			case Open_reg_specsPackage.OPEN_REG_SPECS__DATA_MODEL:
				return getData_model();
			case Open_reg_specsPackage.OPEN_REG_SPECS__PROCESS_WORKFLOW:
				return getProcess_workflow();
			case Open_reg_specsPackage.OPEN_REG_SPECS__REPORT_GENERATION:
				return getReport_generation();
			case Open_reg_specsPackage.OPEN_REG_SPECS__TESTS:
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
			case Open_reg_specsPackage.OPEN_REG_SPECS__REQUIREMENTS:
				getRequirements().clear();
				getRequirements().addAll((Collection<? extends RequirementsModule>)newValue);
				return;
			case Open_reg_specsPackage.OPEN_REG_SPECS__TYPES_AND_CONCEPTS:
				setTypes_and_concepts((SMCubesCoreModel)newValue);
				return;
			case Open_reg_specsPackage.OPEN_REG_SPECS__DATA_MODEL:
				getData_model().clear();
				getData_model().addAll((Collection<? extends EntityModule>)newValue);
				return;
			case Open_reg_specsPackage.OPEN_REG_SPECS__PROCESS_WORKFLOW:
				getProcess_workflow().clear();
				getProcess_workflow().addAll((Collection<? extends SubProcess>)newValue);
				return;
			case Open_reg_specsPackage.OPEN_REG_SPECS__REPORT_GENERATION:
				getReport_generation().clear();
				getReport_generation().addAll((Collection<? extends SQLEntityModule>)newValue);
				return;
			case Open_reg_specsPackage.OPEN_REG_SPECS__TESTS:
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
			case Open_reg_specsPackage.OPEN_REG_SPECS__REQUIREMENTS:
				getRequirements().clear();
				return;
			case Open_reg_specsPackage.OPEN_REG_SPECS__TYPES_AND_CONCEPTS:
				setTypes_and_concepts((SMCubesCoreModel)null);
				return;
			case Open_reg_specsPackage.OPEN_REG_SPECS__DATA_MODEL:
				getData_model().clear();
				return;
			case Open_reg_specsPackage.OPEN_REG_SPECS__PROCESS_WORKFLOW:
				getProcess_workflow().clear();
				return;
			case Open_reg_specsPackage.OPEN_REG_SPECS__REPORT_GENERATION:
				getReport_generation().clear();
				return;
			case Open_reg_specsPackage.OPEN_REG_SPECS__TESTS:
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
			case Open_reg_specsPackage.OPEN_REG_SPECS__REQUIREMENTS:
				return requirements != null && !requirements.isEmpty();
			case Open_reg_specsPackage.OPEN_REG_SPECS__TYPES_AND_CONCEPTS:
				return types_and_concepts != null;
			case Open_reg_specsPackage.OPEN_REG_SPECS__DATA_MODEL:
				return data_model != null && !data_model.isEmpty();
			case Open_reg_specsPackage.OPEN_REG_SPECS__PROCESS_WORKFLOW:
				return process_workflow != null && !process_workflow.isEmpty();
			case Open_reg_specsPackage.OPEN_REG_SPECS__REPORT_GENERATION:
				return report_generation != null && !report_generation.isEmpty();
			case Open_reg_specsPackage.OPEN_REG_SPECS__TESTS:
				return tests != null && !tests.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //OpenRegSpecsImpl
