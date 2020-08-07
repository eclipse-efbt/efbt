/**
 */
package org.eclipse.efbt.controller.model.platform_call;

import org.eclipse.efbt.almengine.model.attribute_lineage.attribute_lineage.AttributeLineageModel;

import org.eclipse.efbt.bird.reports.ReportCell;

import org.eclipse.efbt.trl.model.trl.transformation.VersionedComponentsSet;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Get Attribute Lineage For One Report Cell</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.controller.model.platform_call.GetAttributeLineageForOneReportCell#getTransformationContext <em>Transformation Context</em>}</li>
 *   <li>{@link org.eclipse.efbt.controller.model.platform_call.GetAttributeLineageForOneReportCell#getReportCell <em>Report Cell</em>}</li>
 *   <li>{@link org.eclipse.efbt.controller.model.platform_call.GetAttributeLineageForOneReportCell#getResultingModel <em>Resulting Model</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.controller.model.platform_call.Platform_callPackage#getGetAttributeLineageForOneReportCell()
 * @model
 * @generated
 */
public interface GetAttributeLineageForOneReportCell extends PlatformCall
{
	/**
	 * Returns the value of the '<em><b>Transformation Context</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Transformation Context</em>' reference.
	 * @see #setTransformationContext(VersionedComponentsSet)
	 * @see org.eclipse.efbt.controller.model.platform_call.Platform_callPackage#getGetAttributeLineageForOneReportCell_TransformationContext()
	 * @model
	 * @generated
	 */
	VersionedComponentsSet getTransformationContext();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.controller.model.platform_call.GetAttributeLineageForOneReportCell#getTransformationContext <em>Transformation Context</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Transformation Context</em>' reference.
	 * @see #getTransformationContext()
	 * @generated
	 */
	void setTransformationContext(VersionedComponentsSet value);

	/**
	 * Returns the value of the '<em><b>Report Cell</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Report Cell</em>' reference.
	 * @see #setReportCell(ReportCell)
	 * @see org.eclipse.efbt.controller.model.platform_call.Platform_callPackage#getGetAttributeLineageForOneReportCell_ReportCell()
	 * @model
	 * @generated
	 */
	ReportCell getReportCell();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.controller.model.platform_call.GetAttributeLineageForOneReportCell#getReportCell <em>Report Cell</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Report Cell</em>' reference.
	 * @see #getReportCell()
	 * @generated
	 */
	void setReportCell(ReportCell value);

	/**
	 * Returns the value of the '<em><b>Resulting Model</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Resulting Model</em>' containment reference.
	 * @see #setResultingModel(AttributeLineageModel)
	 * @see org.eclipse.efbt.controller.model.platform_call.Platform_callPackage#getGetAttributeLineageForOneReportCell_ResultingModel()
	 * @model containment="true"
	 * @generated
	 */
	AttributeLineageModel getResultingModel();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.controller.model.platform_call.GetAttributeLineageForOneReportCell#getResultingModel <em>Resulting Model</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Resulting Model</em>' containment reference.
	 * @see #getResultingModel()
	 * @generated
	 */
	void setResultingModel(AttributeLineageModel value);

} // GetAttributeLineageForOneReportCell
