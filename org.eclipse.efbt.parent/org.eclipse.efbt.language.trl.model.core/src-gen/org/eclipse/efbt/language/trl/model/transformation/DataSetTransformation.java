/**
 */
package org.eclipse.efbt.language.trl.model.transformation;

import org.eclipse.efbt.lineage.common.model.cubes.DerivedCube;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Data Set Transformation</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A DataSetTransformations is used to derive DerivedCubes. 
 * The DataSetTransformation links the DerivedCube to the logic which is used to derive the cubes structure from the structure of its source cubes, and to derive its data from populated source cubes.
 * Note that the DataSetTransformations has many subsclasses, currently they represent structures from the TraceableReportingLanguage (TRL), and soon these will be replaced by subclasses which represent structures from VTL.
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.trl.model.transformation.DataSetTransformation#getCube <em>Cube</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.trl.model.transformation.DataSetTransformation#getName <em>Name</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.language.trl.model.transformation.TransformationPackage#getDataSetTransformation()
 * @model extendedMetaData="name='DataSetTransformation' kind='empty'"
 * @generated
 */
public interface DataSetTransformation extends EObject
{
	/**
	 * Returns the value of the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The DerivedCube associated with this DataSetTransformation
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Cube</em>' reference.
	 * @see #setCube(DerivedCube)
	 * @see org.eclipse.efbt.language.trl.model.transformation.TransformationPackage#getDataSetTransformation_Cube()
	 * @model extendedMetaData="kind='attribute' name='cube'"
	 * @generated
	 */
	DerivedCube getCube();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.trl.model.transformation.DataSetTransformation#getCube <em>Cube</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Cube</em>' reference.
	 * @see #getCube()
	 * @generated
	 */
	void setCube(DerivedCube value);

	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The name of this DataSetTransformation
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see org.eclipse.efbt.language.trl.model.transformation.TransformationPackage#getDataSetTransformation_Name()
	 * @model id="true"
	 *        extendedMetaData="kind='attribute' name='name'"
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.trl.model.transformation.DataSetTransformation#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

} // DataSetTransformation
