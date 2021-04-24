/**
 */
package org.eclipse.efbt.language.dtm.model.transformation;

import org.eclipse.efbt.language.dtm.model.dtm_report_cell_views.ReportCellViewModule;

import org.eclipse.efbt.language.dtm.model.dtm_views.VersionedDTMViewsModule;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Versioned Components Set</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * VersionedComponentsSet contains all the information required to translate a FunctionalModule and its dependent FunctionalModules into an AttributeLineageModel
 * It is this component set that contains the cub scehma structures ,and the actual DataSetTransformations.
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.transformation.VersionedComponentsSet#getCubeSchemaModules <em>Cube Schema Modules</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.transformation.VersionedComponentsSet#getDatasetTransformationModules <em>Dataset Transformation Modules</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.transformation.VersionedComponentsSet#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.transformation.VersionedComponentsSet#getReportCellViewModules <em>Report Cell View Modules</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.transformation.VersionedComponentsSet#getLicense <em>License</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.language.dtm.model.transformation.TransformationPackage#getVersionedComponentsSet()
 * @model extendedMetaData="name='VersionedComponentsSet' kind='empty'"
 * @generated
 */
public interface VersionedComponentsSet extends EObject {
	/**
	 * Returns the value of the '<em><b>Cube Schema Modules</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.efbt.language.dtm.model.transformation.VersionedCubeSchemaModule}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The set of VersionedCubeSchemaModules. Note that VersionedCubeSchemaModules state which ReadDataFunctionalModule they are associated with in the functionalModule reference.
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Cube Schema Modules</em>' reference list.
	 * @see org.eclipse.efbt.language.dtm.model.transformation.TransformationPackage#getVersionedComponentsSet_CubeSchemaModules()
	 * @model
	 * @generated
	 */
	EList<VersionedCubeSchemaModule> getCubeSchemaModules();

	/**
	 * Returns the value of the '<em><b>Dataset Transformation Modules</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.efbt.language.dtm.model.dtm_views.VersionedDTMViewsModule}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The set of DataSetTransformationModules, note that each DataSetTransformationModule states which  DataSetTransformationFunctionalModule they are associated with in the functionalModule reference.
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Dataset Transformation Modules</em>' reference list.
	 * @see org.eclipse.efbt.language.dtm.model.transformation.TransformationPackage#getVersionedComponentsSet_DatasetTransformationModules()
	 * @model
	 * @generated
	 */
	EList<VersionedDTMViewsModule> getDatasetTransformationModules();

	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The name of the VersionedComponentSet
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see org.eclipse.efbt.language.dtm.model.transformation.TransformationPackage#getVersionedComponentsSet_Name()
	 * @model id="true"
	 *        extendedMetaData="kind='attribute' name='name'"
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.dtm.model.transformation.VersionedComponentsSet#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

	/**
	 * Returns the value of the '<em><b>Report Cell View Modules</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.efbt.language.dtm.model.dtm_report_cell_views.ReportCellViewModule}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The set of ReportCellViewModules, note that each ReportCellViewModule states which  ReportCellCreationFunctionalModule they are associated with in the functionalModule reference.
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Report Cell View Modules</em>' reference list.
	 * @see org.eclipse.efbt.language.dtm.model.transformation.TransformationPackage#getVersionedComponentsSet_ReportCellViewModules()
	 * @model
	 * @generated
	 */
	EList<ReportCellViewModule> getReportCellViewModules();

	/**
	 * Returns the value of the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The license under which this VersionedComponentSet is distributed
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>License</em>' attribute.
	 * @see #setLicense(String)
	 * @see org.eclipse.efbt.language.dtm.model.transformation.TransformationPackage#getVersionedComponentsSet_License()
	 * @model
	 * @generated
	 */
	String getLicense();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.dtm.model.transformation.VersionedComponentsSet#getLicense <em>License</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>License</em>' attribute.
	 * @see #getLicense()
	 * @generated
	 */
	void setLicense(String value);

} // VersionedComponentsSet
