/**
 */
package org.eclipse.efbt.language.dtm.model.transformation.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBIRDToolsCubeHierarchyRelationshipModule;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBirdToolsCubeModule;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.FunctionSpecModule;

import org.eclipse.efbt.language.dtm.model.dtm_report_cell_views.ReportCellViewModule;

import org.eclipse.efbt.language.dtm.model.dtm_sql_views.VersionedSQLViewsModule;

import org.eclipse.efbt.language.dtm.model.transformation.TransformationPackage;
import org.eclipse.efbt.language.dtm.model.transformation.VersionedCubeSchemaModule;
import org.eclipse.efbt.language.dtm.model.transformation.dtmExecutableLogic;

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
 * An implementation of the model object '<em><b>dtm Executable Logic</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.transformation.impl.dtmExecutableLogicImpl#getCubesModule <em>Cubes Module</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.transformation.impl.dtmExecutableLogicImpl#getFunctionSpecModule <em>Function Spec Module</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.transformation.impl.dtmExecutableLogicImpl#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.transformation.impl.dtmExecutableLogicImpl#getCubesHierarchyModule <em>Cubes Hierarchy Module</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.transformation.impl.dtmExecutableLogicImpl#getCubeSchemaModules <em>Cube Schema Modules</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.transformation.impl.dtmExecutableLogicImpl#getDatasetTransformationModules <em>Dataset Transformation Modules</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.transformation.impl.dtmExecutableLogicImpl#getReportCellViewModules <em>Report Cell View Modules</em>}</li>
 * </ul>
 *
 * @generated
 */
public class dtmExecutableLogicImpl extends MinimalEObjectImpl.Container implements dtmExecutableLogic
{
	/**
	 * The cached value of the '{@link #getCubesModule() <em>Cubes Module</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCubesModule()
	 * @generated
	 * @ordered
	 */
	protected FreeBirdToolsCubeModule cubesModule;

	/**
	 * The cached value of the '{@link #getFunctionSpecModule() <em>Function Spec Module</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getFunctionSpecModule()
	 * @generated
	 * @ordered
	 */
	protected FunctionSpecModule functionSpecModule;

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
	 * The cached value of the '{@link #getCubesHierarchyModule() <em>Cubes Hierarchy Module</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCubesHierarchyModule()
	 * @generated
	 * @ordered
	 */
	protected FreeBIRDToolsCubeHierarchyRelationshipModule cubesHierarchyModule;

	/**
	 * The cached value of the '{@link #getCubeSchemaModules() <em>Cube Schema Modules</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCubeSchemaModules()
	 * @generated
	 * @ordered
	 */
	protected EList<VersionedCubeSchemaModule> cubeSchemaModules;

	/**
	 * The cached value of the '{@link #getDatasetTransformationModules() <em>Dataset Transformation Modules</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getDatasetTransformationModules()
	 * @generated
	 * @ordered
	 */
	protected EList<VersionedSQLViewsModule> datasetTransformationModules;

	/**
	 * The cached value of the '{@link #getReportCellViewModules() <em>Report Cell View Modules</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getReportCellViewModules()
	 * @generated
	 * @ordered
	 */
	protected EList<ReportCellViewModule> reportCellViewModules;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected dtmExecutableLogicImpl()
	{
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass()
	{
		return TransformationPackage.Literals.DTM_EXECUTABLE_LOGIC;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public FreeBirdToolsCubeModule getCubesModule()
	{
		return cubesModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetCubesModule(FreeBirdToolsCubeModule newCubesModule, NotificationChain msgs)
	{
		FreeBirdToolsCubeModule oldCubesModule = cubesModule;
		cubesModule = newCubesModule;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, TransformationPackage.DTM_EXECUTABLE_LOGIC__CUBES_MODULE, oldCubesModule, newCubesModule);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setCubesModule(FreeBirdToolsCubeModule newCubesModule)
	{
		if (newCubesModule != cubesModule) {
			NotificationChain msgs = null;
			if (cubesModule != null)
				msgs = ((InternalEObject)cubesModule).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - TransformationPackage.DTM_EXECUTABLE_LOGIC__CUBES_MODULE, null, msgs);
			if (newCubesModule != null)
				msgs = ((InternalEObject)newCubesModule).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - TransformationPackage.DTM_EXECUTABLE_LOGIC__CUBES_MODULE, null, msgs);
			msgs = basicSetCubesModule(newCubesModule, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TransformationPackage.DTM_EXECUTABLE_LOGIC__CUBES_MODULE, newCubesModule, newCubesModule));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public FunctionSpecModule getFunctionSpecModule()
	{
		return functionSpecModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetFunctionSpecModule(FunctionSpecModule newFunctionSpecModule, NotificationChain msgs)
	{
		FunctionSpecModule oldFunctionSpecModule = functionSpecModule;
		functionSpecModule = newFunctionSpecModule;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, TransformationPackage.DTM_EXECUTABLE_LOGIC__FUNCTION_SPEC_MODULE, oldFunctionSpecModule, newFunctionSpecModule);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setFunctionSpecModule(FunctionSpecModule newFunctionSpecModule)
	{
		if (newFunctionSpecModule != functionSpecModule) {
			NotificationChain msgs = null;
			if (functionSpecModule != null)
				msgs = ((InternalEObject)functionSpecModule).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - TransformationPackage.DTM_EXECUTABLE_LOGIC__FUNCTION_SPEC_MODULE, null, msgs);
			if (newFunctionSpecModule != null)
				msgs = ((InternalEObject)newFunctionSpecModule).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - TransformationPackage.DTM_EXECUTABLE_LOGIC__FUNCTION_SPEC_MODULE, null, msgs);
			msgs = basicSetFunctionSpecModule(newFunctionSpecModule, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TransformationPackage.DTM_EXECUTABLE_LOGIC__FUNCTION_SPEC_MODULE, newFunctionSpecModule, newFunctionSpecModule));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getName()
	{
		return name;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setName(String newName)
	{
		String oldName = name;
		name = newName;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TransformationPackage.DTM_EXECUTABLE_LOGIC__NAME, oldName, name));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public FreeBIRDToolsCubeHierarchyRelationshipModule getCubesHierarchyModule()
	{
		return cubesHierarchyModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetCubesHierarchyModule(FreeBIRDToolsCubeHierarchyRelationshipModule newCubesHierarchyModule, NotificationChain msgs)
	{
		FreeBIRDToolsCubeHierarchyRelationshipModule oldCubesHierarchyModule = cubesHierarchyModule;
		cubesHierarchyModule = newCubesHierarchyModule;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, TransformationPackage.DTM_EXECUTABLE_LOGIC__CUBES_HIERARCHY_MODULE, oldCubesHierarchyModule, newCubesHierarchyModule);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setCubesHierarchyModule(FreeBIRDToolsCubeHierarchyRelationshipModule newCubesHierarchyModule)
	{
		if (newCubesHierarchyModule != cubesHierarchyModule) {
			NotificationChain msgs = null;
			if (cubesHierarchyModule != null)
				msgs = ((InternalEObject)cubesHierarchyModule).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - TransformationPackage.DTM_EXECUTABLE_LOGIC__CUBES_HIERARCHY_MODULE, null, msgs);
			if (newCubesHierarchyModule != null)
				msgs = ((InternalEObject)newCubesHierarchyModule).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - TransformationPackage.DTM_EXECUTABLE_LOGIC__CUBES_HIERARCHY_MODULE, null, msgs);
			msgs = basicSetCubesHierarchyModule(newCubesHierarchyModule, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TransformationPackage.DTM_EXECUTABLE_LOGIC__CUBES_HIERARCHY_MODULE, newCubesHierarchyModule, newCubesHierarchyModule));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<VersionedCubeSchemaModule> getCubeSchemaModules()
	{
		if (cubeSchemaModules == null) {
			cubeSchemaModules = new EObjectContainmentEList<VersionedCubeSchemaModule>(VersionedCubeSchemaModule.class, this, TransformationPackage.DTM_EXECUTABLE_LOGIC__CUBE_SCHEMA_MODULES);
		}
		return cubeSchemaModules;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<VersionedSQLViewsModule> getDatasetTransformationModules()
	{
		if (datasetTransformationModules == null) {
			datasetTransformationModules = new EObjectContainmentEList<VersionedSQLViewsModule>(VersionedSQLViewsModule.class, this, TransformationPackage.DTM_EXECUTABLE_LOGIC__DATASET_TRANSFORMATION_MODULES);
		}
		return datasetTransformationModules;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<ReportCellViewModule> getReportCellViewModules()
	{
		if (reportCellViewModules == null) {
			reportCellViewModules = new EObjectContainmentEList<ReportCellViewModule>(ReportCellViewModule.class, this, TransformationPackage.DTM_EXECUTABLE_LOGIC__REPORT_CELL_VIEW_MODULES);
		}
		return reportCellViewModules;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs)
	{
		switch (featureID) {
			case TransformationPackage.DTM_EXECUTABLE_LOGIC__CUBES_MODULE:
				return basicSetCubesModule(null, msgs);
			case TransformationPackage.DTM_EXECUTABLE_LOGIC__FUNCTION_SPEC_MODULE:
				return basicSetFunctionSpecModule(null, msgs);
			case TransformationPackage.DTM_EXECUTABLE_LOGIC__CUBES_HIERARCHY_MODULE:
				return basicSetCubesHierarchyModule(null, msgs);
			case TransformationPackage.DTM_EXECUTABLE_LOGIC__CUBE_SCHEMA_MODULES:
				return ((InternalEList<?>)getCubeSchemaModules()).basicRemove(otherEnd, msgs);
			case TransformationPackage.DTM_EXECUTABLE_LOGIC__DATASET_TRANSFORMATION_MODULES:
				return ((InternalEList<?>)getDatasetTransformationModules()).basicRemove(otherEnd, msgs);
			case TransformationPackage.DTM_EXECUTABLE_LOGIC__REPORT_CELL_VIEW_MODULES:
				return ((InternalEList<?>)getReportCellViewModules()).basicRemove(otherEnd, msgs);
		}
		return super.eInverseRemove(otherEnd, featureID, msgs);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType)
	{
		switch (featureID) {
			case TransformationPackage.DTM_EXECUTABLE_LOGIC__CUBES_MODULE:
				return getCubesModule();
			case TransformationPackage.DTM_EXECUTABLE_LOGIC__FUNCTION_SPEC_MODULE:
				return getFunctionSpecModule();
			case TransformationPackage.DTM_EXECUTABLE_LOGIC__NAME:
				return getName();
			case TransformationPackage.DTM_EXECUTABLE_LOGIC__CUBES_HIERARCHY_MODULE:
				return getCubesHierarchyModule();
			case TransformationPackage.DTM_EXECUTABLE_LOGIC__CUBE_SCHEMA_MODULES:
				return getCubeSchemaModules();
			case TransformationPackage.DTM_EXECUTABLE_LOGIC__DATASET_TRANSFORMATION_MODULES:
				return getDatasetTransformationModules();
			case TransformationPackage.DTM_EXECUTABLE_LOGIC__REPORT_CELL_VIEW_MODULES:
				return getReportCellViewModules();
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
	public void eSet(int featureID, Object newValue)
	{
		switch (featureID) {
			case TransformationPackage.DTM_EXECUTABLE_LOGIC__CUBES_MODULE:
				setCubesModule((FreeBirdToolsCubeModule)newValue);
				return;
			case TransformationPackage.DTM_EXECUTABLE_LOGIC__FUNCTION_SPEC_MODULE:
				setFunctionSpecModule((FunctionSpecModule)newValue);
				return;
			case TransformationPackage.DTM_EXECUTABLE_LOGIC__NAME:
				setName((String)newValue);
				return;
			case TransformationPackage.DTM_EXECUTABLE_LOGIC__CUBES_HIERARCHY_MODULE:
				setCubesHierarchyModule((FreeBIRDToolsCubeHierarchyRelationshipModule)newValue);
				return;
			case TransformationPackage.DTM_EXECUTABLE_LOGIC__CUBE_SCHEMA_MODULES:
				getCubeSchemaModules().clear();
				getCubeSchemaModules().addAll((Collection<? extends VersionedCubeSchemaModule>)newValue);
				return;
			case TransformationPackage.DTM_EXECUTABLE_LOGIC__DATASET_TRANSFORMATION_MODULES:
				getDatasetTransformationModules().clear();
				getDatasetTransformationModules().addAll((Collection<? extends VersionedSQLViewsModule>)newValue);
				return;
			case TransformationPackage.DTM_EXECUTABLE_LOGIC__REPORT_CELL_VIEW_MODULES:
				getReportCellViewModules().clear();
				getReportCellViewModules().addAll((Collection<? extends ReportCellViewModule>)newValue);
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
	public void eUnset(int featureID)
	{
		switch (featureID) {
			case TransformationPackage.DTM_EXECUTABLE_LOGIC__CUBES_MODULE:
				setCubesModule((FreeBirdToolsCubeModule)null);
				return;
			case TransformationPackage.DTM_EXECUTABLE_LOGIC__FUNCTION_SPEC_MODULE:
				setFunctionSpecModule((FunctionSpecModule)null);
				return;
			case TransformationPackage.DTM_EXECUTABLE_LOGIC__NAME:
				setName(NAME_EDEFAULT);
				return;
			case TransformationPackage.DTM_EXECUTABLE_LOGIC__CUBES_HIERARCHY_MODULE:
				setCubesHierarchyModule((FreeBIRDToolsCubeHierarchyRelationshipModule)null);
				return;
			case TransformationPackage.DTM_EXECUTABLE_LOGIC__CUBE_SCHEMA_MODULES:
				getCubeSchemaModules().clear();
				return;
			case TransformationPackage.DTM_EXECUTABLE_LOGIC__DATASET_TRANSFORMATION_MODULES:
				getDatasetTransformationModules().clear();
				return;
			case TransformationPackage.DTM_EXECUTABLE_LOGIC__REPORT_CELL_VIEW_MODULES:
				getReportCellViewModules().clear();
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
	public boolean eIsSet(int featureID)
	{
		switch (featureID) {
			case TransformationPackage.DTM_EXECUTABLE_LOGIC__CUBES_MODULE:
				return cubesModule != null;
			case TransformationPackage.DTM_EXECUTABLE_LOGIC__FUNCTION_SPEC_MODULE:
				return functionSpecModule != null;
			case TransformationPackage.DTM_EXECUTABLE_LOGIC__NAME:
				return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
			case TransformationPackage.DTM_EXECUTABLE_LOGIC__CUBES_HIERARCHY_MODULE:
				return cubesHierarchyModule != null;
			case TransformationPackage.DTM_EXECUTABLE_LOGIC__CUBE_SCHEMA_MODULES:
				return cubeSchemaModules != null && !cubeSchemaModules.isEmpty();
			case TransformationPackage.DTM_EXECUTABLE_LOGIC__DATASET_TRANSFORMATION_MODULES:
				return datasetTransformationModules != null && !datasetTransformationModules.isEmpty();
			case TransformationPackage.DTM_EXECUTABLE_LOGIC__REPORT_CELL_VIEW_MODULES:
				return reportCellViewModules != null && !reportCellViewModules.isEmpty();
		}
		return super.eIsSet(featureID);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String toString()
	{
		if (eIsProxy()) return super.toString();

		StringBuilder result = new StringBuilder(super.toString());
		result.append(" (name: ");
		result.append(name);
		result.append(')');
		return result.toString();
	}

} //dtmExecutableLogicImpl
