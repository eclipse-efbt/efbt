/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic;

import org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.ColumnStructuredData;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.DerivedCube;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Derived Cube Data</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 *  data for derived cubes 
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.DerivedCubeData#getRows <em>Rows</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.DerivedCubeData#getCube <em>Cube</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.DerivedCubeData#getSourceCubeData <em>Source Cube Data</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.DerivedCubeData#getCubeCreationDescription <em>Cube Creation Description</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.Cell_transformation_logicPackage#getDerivedCubeData()
 * @model extendedMetaData="name='DerivedCubeData' kind='elementOnly'"
 * @generated
 */
public interface DerivedCubeData extends ColumnStructuredData
{
	/**
	 * Returns the value of the '<em><b>Rows</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.DerivedRowData}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The  Rows of data
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Rows</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.Cell_transformation_logicPackage#getDerivedCubeData_Rows()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='rows'"
	 * @generated
	 */
	EList<DerivedRowData> getRows();

	/**
	 * Returns the value of the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The associated cube
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Cube</em>' reference.
	 * @see #setCube(DerivedCube)
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.Cell_transformation_logicPackage#getDerivedCubeData_Cube()
	 * @model extendedMetaData="kind='attribute' name='cube'"
	 * @generated
	 */
	DerivedCube getCube();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.DerivedCubeData#getCube <em>Cube</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Cube</em>' reference.
	 * @see #getCube()
	 * @generated
	 */
	void setCube(DerivedCube value);

	/**
	 * Returns the value of the '<em><b>Source Cube Data</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.column_structured_data.ColumnStructuredData}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The source cube data
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Source Cube Data</em>' reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.Cell_transformation_logicPackage#getDerivedCubeData_SourceCubeData()
	 * @model extendedMetaData="kind='attribute' name='sourceCubeData'"
	 * @generated
	 */
	EList<ColumnStructuredData> getSourceCubeData();

	/**
	 * Returns the value of the '<em><b>Cube Creation Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Cube Creation Description</em>' attribute.
	 * @see #setCubeCreationDescription(String)
	 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.Cell_transformation_logicPackage#getDerivedCubeData_CubeCreationDescription()
	 * @model
	 * @generated
	 */
	String getCubeCreationDescription();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.DerivedCubeData#getCubeCreationDescription <em>Cube Creation Description</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Cube Creation Description</em>' attribute.
	 * @see #getCubeCreationDescription()
	 * @generated
	 */
	void setCubeCreationDescription(String value);

} // DerivedCubeData
