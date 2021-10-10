/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.core.model.scenarios.Scenario;

import org.eclipse.efbt.cocalimo.core.model.task.DataProcessingTask;
import org.eclipse.efbt.cocalimo.core.model.task.ReadDataSourceTask;
import org.eclipse.efbt.cocalimo.core.model.task.ReportCellCreationTask;
import org.eclipse.efbt.cocalimo.core.model.task.TaskSet;

import org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.Aorta_smcubesPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.TestDefinition;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

import org.eclipse.emf.ecore.util.EObjectResolvingEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Test Definition</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.TestDefinitionImpl#getReadTasks <em>Read Tasks</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.TestDefinitionImpl#getProcessingTasks <em>Processing Tasks</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.TestDefinitionImpl#getReportCellCreationTasks <em>Report Cell Creation Tasks</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.TestDefinitionImpl#getScenarios <em>Scenarios</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.TestDefinitionImpl#getPicture <em>Picture</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.aorta_smcubes.impl.TestDefinitionImpl#getName <em>Name</em>}</li>
 * </ul>
 *
 * @generated
 */
public class TestDefinitionImpl extends MinimalEObjectImpl.Container implements TestDefinition {
	/**
	 * The cached value of the '{@link #getReadTasks() <em>Read Tasks</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getReadTasks()
	 * @generated
	 * @ordered
	 */
	protected EList<ReadDataSourceTask> readTasks;

	/**
	 * The cached value of the '{@link #getProcessingTasks() <em>Processing Tasks</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getProcessingTasks()
	 * @generated
	 * @ordered
	 */
	protected EList<DataProcessingTask> processingTasks;

	/**
	 * The cached value of the '{@link #getReportCellCreationTasks() <em>Report Cell Creation Tasks</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getReportCellCreationTasks()
	 * @generated
	 * @ordered
	 */
	protected EList<ReportCellCreationTask> reportCellCreationTasks;

	/**
	 * The cached value of the '{@link #getScenarios() <em>Scenarios</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getScenarios()
	 * @generated
	 * @ordered
	 */
	protected EList<Scenario> scenarios;

	/**
	 * The cached value of the '{@link #getPicture() <em>Picture</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getPicture()
	 * @generated
	 * @ordered
	 */
	protected TaskSet picture;

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
	protected TestDefinitionImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Aorta_smcubesPackage.Literals.TEST_DEFINITION;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<ReadDataSourceTask> getReadTasks() {
		if (readTasks == null) {
			readTasks = new EObjectResolvingEList<ReadDataSourceTask>(ReadDataSourceTask.class, this, Aorta_smcubesPackage.TEST_DEFINITION__READ_TASKS);
		}
		return readTasks;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<DataProcessingTask> getProcessingTasks() {
		if (processingTasks == null) {
			processingTasks = new EObjectResolvingEList<DataProcessingTask>(DataProcessingTask.class, this, Aorta_smcubesPackage.TEST_DEFINITION__PROCESSING_TASKS);
		}
		return processingTasks;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<ReportCellCreationTask> getReportCellCreationTasks() {
		if (reportCellCreationTasks == null) {
			reportCellCreationTasks = new EObjectResolvingEList<ReportCellCreationTask>(ReportCellCreationTask.class, this, Aorta_smcubesPackage.TEST_DEFINITION__REPORT_CELL_CREATION_TASKS);
		}
		return reportCellCreationTasks;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<Scenario> getScenarios() {
		if (scenarios == null) {
			scenarios = new EObjectResolvingEList<Scenario>(Scenario.class, this, Aorta_smcubesPackage.TEST_DEFINITION__SCENARIOS);
		}
		return scenarios;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public TaskSet getPicture() {
		if (picture != null && picture.eIsProxy()) {
			InternalEObject oldPicture = (InternalEObject)picture;
			picture = (TaskSet)eResolveProxy(oldPicture);
			if (picture != oldPicture) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Aorta_smcubesPackage.TEST_DEFINITION__PICTURE, oldPicture, picture));
			}
		}
		return picture;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TaskSet basicGetPicture() {
		return picture;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setPicture(TaskSet newPicture) {
		TaskSet oldPicture = picture;
		picture = newPicture;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Aorta_smcubesPackage.TEST_DEFINITION__PICTURE, oldPicture, picture));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getName() {
		return name;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setName(String newName) {
		String oldName = name;
		name = newName;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Aorta_smcubesPackage.TEST_DEFINITION__NAME, oldName, name));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Aorta_smcubesPackage.TEST_DEFINITION__READ_TASKS:
				return getReadTasks();
			case Aorta_smcubesPackage.TEST_DEFINITION__PROCESSING_TASKS:
				return getProcessingTasks();
			case Aorta_smcubesPackage.TEST_DEFINITION__REPORT_CELL_CREATION_TASKS:
				return getReportCellCreationTasks();
			case Aorta_smcubesPackage.TEST_DEFINITION__SCENARIOS:
				return getScenarios();
			case Aorta_smcubesPackage.TEST_DEFINITION__PICTURE:
				if (resolve) return getPicture();
				return basicGetPicture();
			case Aorta_smcubesPackage.TEST_DEFINITION__NAME:
				return getName();
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
			case Aorta_smcubesPackage.TEST_DEFINITION__READ_TASKS:
				getReadTasks().clear();
				getReadTasks().addAll((Collection<? extends ReadDataSourceTask>)newValue);
				return;
			case Aorta_smcubesPackage.TEST_DEFINITION__PROCESSING_TASKS:
				getProcessingTasks().clear();
				getProcessingTasks().addAll((Collection<? extends DataProcessingTask>)newValue);
				return;
			case Aorta_smcubesPackage.TEST_DEFINITION__REPORT_CELL_CREATION_TASKS:
				getReportCellCreationTasks().clear();
				getReportCellCreationTasks().addAll((Collection<? extends ReportCellCreationTask>)newValue);
				return;
			case Aorta_smcubesPackage.TEST_DEFINITION__SCENARIOS:
				getScenarios().clear();
				getScenarios().addAll((Collection<? extends Scenario>)newValue);
				return;
			case Aorta_smcubesPackage.TEST_DEFINITION__PICTURE:
				setPicture((TaskSet)newValue);
				return;
			case Aorta_smcubesPackage.TEST_DEFINITION__NAME:
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
			case Aorta_smcubesPackage.TEST_DEFINITION__READ_TASKS:
				getReadTasks().clear();
				return;
			case Aorta_smcubesPackage.TEST_DEFINITION__PROCESSING_TASKS:
				getProcessingTasks().clear();
				return;
			case Aorta_smcubesPackage.TEST_DEFINITION__REPORT_CELL_CREATION_TASKS:
				getReportCellCreationTasks().clear();
				return;
			case Aorta_smcubesPackage.TEST_DEFINITION__SCENARIOS:
				getScenarios().clear();
				return;
			case Aorta_smcubesPackage.TEST_DEFINITION__PICTURE:
				setPicture((TaskSet)null);
				return;
			case Aorta_smcubesPackage.TEST_DEFINITION__NAME:
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
			case Aorta_smcubesPackage.TEST_DEFINITION__READ_TASKS:
				return readTasks != null && !readTasks.isEmpty();
			case Aorta_smcubesPackage.TEST_DEFINITION__PROCESSING_TASKS:
				return processingTasks != null && !processingTasks.isEmpty();
			case Aorta_smcubesPackage.TEST_DEFINITION__REPORT_CELL_CREATION_TASKS:
				return reportCellCreationTasks != null && !reportCellCreationTasks.isEmpty();
			case Aorta_smcubesPackage.TEST_DEFINITION__SCENARIOS:
				return scenarios != null && !scenarios.isEmpty();
			case Aorta_smcubesPackage.TEST_DEFINITION__PICTURE:
				return picture != null;
			case Aorta_smcubesPackage.TEST_DEFINITION__NAME:
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

} //TestDefinitionImpl
