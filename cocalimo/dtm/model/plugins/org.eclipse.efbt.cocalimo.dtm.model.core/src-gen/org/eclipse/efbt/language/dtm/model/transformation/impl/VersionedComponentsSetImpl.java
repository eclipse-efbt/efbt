/**
 */
package org.eclipse.efbt.language.dtm.model.transformation.impl;

import java.util.Collection;

import org.eclipse.efbt.language.dtm.model.dtm_report_cell_views.ReportCellViewModule;

import org.eclipse.efbt.language.dtm.model.dtm_sql_views.VersionedSQLViewsModule;

import org.eclipse.efbt.language.dtm.model.transformation.TransformationPackage;
import org.eclipse.efbt.language.dtm.model.transformation.VersionedComponentsSet;
import org.eclipse.efbt.language.dtm.model.transformation.VersionedCubeSchemaModule;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

import org.eclipse.emf.ecore.util.EObjectResolvingEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Versioned Components Set</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.transformation.impl.VersionedComponentsSetImpl#getCubeSchemaModules <em>Cube Schema Modules</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.transformation.impl.VersionedComponentsSetImpl#getDatasetTransformationModules <em>Dataset Transformation Modules</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.transformation.impl.VersionedComponentsSetImpl#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.transformation.impl.VersionedComponentsSetImpl#getReportCellViewModules <em>Report Cell View Modules</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.transformation.impl.VersionedComponentsSetImpl#getLicense <em>License</em>}</li>
 * </ul>
 *
 * @generated
 */
public class VersionedComponentsSetImpl extends MinimalEObjectImpl.Container implements VersionedComponentsSet
{
	/**
	 * The cached value of the '{@link #getCubeSchemaModules() <em>Cube Schema Modules</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCubeSchemaModules()
	 * @generated
	 * @ordered
	 */
	protected EList<VersionedCubeSchemaModule> cubeSchemaModules;

	/**
	 * The cached value of the '{@link #getDatasetTransformationModules() <em>Dataset Transformation Modules</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getDatasetTransformationModules()
	 * @generated
	 * @ordered
	 */
	protected EList<VersionedSQLViewsModule> datasetTransformationModules;

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
	 * The cached value of the '{@link #getReportCellViewModules() <em>Report Cell View Modules</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getReportCellViewModules()
	 * @generated
	 * @ordered
	 */
	protected EList<ReportCellViewModule> reportCellViewModules;

	/**
	 * The default value of the '{@link #getLicense() <em>License</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getLicense()
	 * @generated
	 * @ordered
	 */
	protected static final String LICENSE_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getLicense() <em>License</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getLicense()
	 * @generated
	 * @ordered
	 */
	protected String license = LICENSE_EDEFAULT;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected VersionedComponentsSetImpl()
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
		return TransformationPackage.Literals.VERSIONED_COMPONENTS_SET;
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
			cubeSchemaModules = new EObjectResolvingEList<VersionedCubeSchemaModule>(VersionedCubeSchemaModule.class, this, TransformationPackage.VERSIONED_COMPONENTS_SET__CUBE_SCHEMA_MODULES);
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
			datasetTransformationModules = new EObjectResolvingEList<VersionedSQLViewsModule>(VersionedSQLViewsModule.class, this, TransformationPackage.VERSIONED_COMPONENTS_SET__DATASET_TRANSFORMATION_MODULES);
		}
		return datasetTransformationModules;
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
			eNotify(new ENotificationImpl(this, Notification.SET, TransformationPackage.VERSIONED_COMPONENTS_SET__NAME, oldName, name));
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
			reportCellViewModules = new EObjectResolvingEList<ReportCellViewModule>(ReportCellViewModule.class, this, TransformationPackage.VERSIONED_COMPONENTS_SET__REPORT_CELL_VIEW_MODULES);
		}
		return reportCellViewModules;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getLicense()
	{
		return license;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setLicense(String newLicense)
	{
		String oldLicense = license;
		license = newLicense;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TransformationPackage.VERSIONED_COMPONENTS_SET__LICENSE, oldLicense, license));
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
			case TransformationPackage.VERSIONED_COMPONENTS_SET__CUBE_SCHEMA_MODULES:
				return getCubeSchemaModules();
			case TransformationPackage.VERSIONED_COMPONENTS_SET__DATASET_TRANSFORMATION_MODULES:
				return getDatasetTransformationModules();
			case TransformationPackage.VERSIONED_COMPONENTS_SET__NAME:
				return getName();
			case TransformationPackage.VERSIONED_COMPONENTS_SET__REPORT_CELL_VIEW_MODULES:
				return getReportCellViewModules();
			case TransformationPackage.VERSIONED_COMPONENTS_SET__LICENSE:
				return getLicense();
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
			case TransformationPackage.VERSIONED_COMPONENTS_SET__CUBE_SCHEMA_MODULES:
				getCubeSchemaModules().clear();
				getCubeSchemaModules().addAll((Collection<? extends VersionedCubeSchemaModule>)newValue);
				return;
			case TransformationPackage.VERSIONED_COMPONENTS_SET__DATASET_TRANSFORMATION_MODULES:
				getDatasetTransformationModules().clear();
				getDatasetTransformationModules().addAll((Collection<? extends VersionedSQLViewsModule>)newValue);
				return;
			case TransformationPackage.VERSIONED_COMPONENTS_SET__NAME:
				setName((String)newValue);
				return;
			case TransformationPackage.VERSIONED_COMPONENTS_SET__REPORT_CELL_VIEW_MODULES:
				getReportCellViewModules().clear();
				getReportCellViewModules().addAll((Collection<? extends ReportCellViewModule>)newValue);
				return;
			case TransformationPackage.VERSIONED_COMPONENTS_SET__LICENSE:
				setLicense((String)newValue);
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
			case TransformationPackage.VERSIONED_COMPONENTS_SET__CUBE_SCHEMA_MODULES:
				getCubeSchemaModules().clear();
				return;
			case TransformationPackage.VERSIONED_COMPONENTS_SET__DATASET_TRANSFORMATION_MODULES:
				getDatasetTransformationModules().clear();
				return;
			case TransformationPackage.VERSIONED_COMPONENTS_SET__NAME:
				setName(NAME_EDEFAULT);
				return;
			case TransformationPackage.VERSIONED_COMPONENTS_SET__REPORT_CELL_VIEW_MODULES:
				getReportCellViewModules().clear();
				return;
			case TransformationPackage.VERSIONED_COMPONENTS_SET__LICENSE:
				setLicense(LICENSE_EDEFAULT);
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
			case TransformationPackage.VERSIONED_COMPONENTS_SET__CUBE_SCHEMA_MODULES:
				return cubeSchemaModules != null && !cubeSchemaModules.isEmpty();
			case TransformationPackage.VERSIONED_COMPONENTS_SET__DATASET_TRANSFORMATION_MODULES:
				return datasetTransformationModules != null && !datasetTransformationModules.isEmpty();
			case TransformationPackage.VERSIONED_COMPONENTS_SET__NAME:
				return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
			case TransformationPackage.VERSIONED_COMPONENTS_SET__REPORT_CELL_VIEW_MODULES:
				return reportCellViewModules != null && !reportCellViewModules.isEmpty();
			case TransformationPackage.VERSIONED_COMPONENTS_SET__LICENSE:
				return LICENSE_EDEFAULT == null ? license != null : !LICENSE_EDEFAULT.equals(license);
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
		result.append(", license: ");
		result.append(license);
		result.append(')');
		return result.toString();
	}

} //VersionedComponentsSetImpl
