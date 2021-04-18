/**
 */
package org.eclipse.efbt.language.dtm.model.transformation;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBIRDToolsCubeHierarchyRelationshipModule;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBirdToolsCubeModule;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.FunctionSpecModule;

import org.eclipse.efbt.language.dtm.model.dtm_report_cell_views.ReportCellViewModule;

import org.eclipse.efbt.language.dtm.model.dtm_sql_views.VersionedSQLViewsModule;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>dtm Executable Logic</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.transformation.dtmExecutableLogic#getCubesModule <em>Cubes Module</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.transformation.dtmExecutableLogic#getFunctionSpecModule <em>Function Spec Module</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.transformation.dtmExecutableLogic#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.transformation.dtmExecutableLogic#getCubesHierarchyModule <em>Cubes Hierarchy Module</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.transformation.dtmExecutableLogic#getCubeSchemaModules <em>Cube Schema Modules</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.transformation.dtmExecutableLogic#getDatasetTransformationModules <em>Dataset Transformation Modules</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.transformation.dtmExecutableLogic#getReportCellViewModules <em>Report Cell View Modules</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.language.dtm.model.transformation.TransformationPackage#getdtmExecutableLogic()
 * @model
 * @generated
 */
public interface dtmExecutableLogic extends EObject
{
	/**
	 * Returns the value of the '<em><b>Cubes Module</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Cubes Module</em>' containment reference.
	 * @see #setCubesModule(FreeBirdToolsCubeModule)
	 * @see org.eclipse.efbt.language.dtm.model.transformation.TransformationPackage#getdtmExecutableLogic_CubesModule()
	 * @model containment="true"
	 * @generated
	 */
	FreeBirdToolsCubeModule getCubesModule();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.dtm.model.transformation.dtmExecutableLogic#getCubesModule <em>Cubes Module</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Cubes Module</em>' containment reference.
	 * @see #getCubesModule()
	 * @generated
	 */
	void setCubesModule(FreeBirdToolsCubeModule value);

	/**
	 * Returns the value of the '<em><b>Function Spec Module</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Function Spec Module</em>' containment reference.
	 * @see #setFunctionSpecModule(FunctionSpecModule)
	 * @see org.eclipse.efbt.language.dtm.model.transformation.TransformationPackage#getdtmExecutableLogic_FunctionSpecModule()
	 * @model containment="true"
	 * @generated
	 */
	FunctionSpecModule getFunctionSpecModule();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.dtm.model.transformation.dtmExecutableLogic#getFunctionSpecModule <em>Function Spec Module</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Function Spec Module</em>' containment reference.
	 * @see #getFunctionSpecModule()
	 * @generated
	 */
	void setFunctionSpecModule(FunctionSpecModule value);

	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see org.eclipse.efbt.language.dtm.model.transformation.TransformationPackage#getdtmExecutableLogic_Name()
	 * @model id="true"
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.dtm.model.transformation.dtmExecutableLogic#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

	/**
	 * Returns the value of the '<em><b>Cubes Hierarchy Module</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Cubes Hierarchy Module</em>' containment reference.
	 * @see #setCubesHierarchyModule(FreeBIRDToolsCubeHierarchyRelationshipModule)
	 * @see org.eclipse.efbt.language.dtm.model.transformation.TransformationPackage#getdtmExecutableLogic_CubesHierarchyModule()
	 * @model containment="true"
	 * @generated
	 */
	FreeBIRDToolsCubeHierarchyRelationshipModule getCubesHierarchyModule();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.dtm.model.transformation.dtmExecutableLogic#getCubesHierarchyModule <em>Cubes Hierarchy Module</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Cubes Hierarchy Module</em>' containment reference.
	 * @see #getCubesHierarchyModule()
	 * @generated
	 */
	void setCubesHierarchyModule(FreeBIRDToolsCubeHierarchyRelationshipModule value);

	/**
	 * Returns the value of the '<em><b>Cube Schema Modules</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.language.dtm.model.transformation.VersionedCubeSchemaModule}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The set of VersionedCubeSchemaModules. Note that VersionedCubeSchemaModules state which ReadDataFunctionalModule they are associated with in the functionalModule reference.
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Cube Schema Modules</em>' containment reference list.
	 * @see org.eclipse.efbt.language.dtm.model.transformation.TransformationPackage#getdtmExecutableLogic_CubeSchemaModules()
	 * @model containment="true"
	 * @generated
	 */
	EList<VersionedCubeSchemaModule> getCubeSchemaModules();

	/**
	 * Returns the value of the '<em><b>Dataset Transformation Modules</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.VersionedSQLViewsModule}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The set of DataSetTransformationModules, note that each DataSetTransformationModule states which  DataSetTransformationFunctionalModule they are associated with in the functionalModule reference.
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Dataset Transformation Modules</em>' containment reference list.
	 * @see org.eclipse.efbt.language.dtm.model.transformation.TransformationPackage#getdtmExecutableLogic_DatasetTransformationModules()
	 * @model containment="true"
	 * @generated
	 */
	EList<VersionedSQLViewsModule> getDatasetTransformationModules();

	/**
	 * Returns the value of the '<em><b>Report Cell View Modules</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.language.dtm.model.dtm_report_cell_views.ReportCellViewModule}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The set of ReportCellViewModules, note that each ReportCellViewModule states which  ReportCellCreationFunctionalModule they are associated with in the functionalModule reference.
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Report Cell View Modules</em>' containment reference list.
	 * @see org.eclipse.efbt.language.dtm.model.transformation.TransformationPackage#getdtmExecutableLogic_ReportCellViewModules()
	 * @model containment="true"
	 * @generated
	 */
	EList<ReportCellViewModule> getReportCellViewModules();

} // dtmExecutableLogic
