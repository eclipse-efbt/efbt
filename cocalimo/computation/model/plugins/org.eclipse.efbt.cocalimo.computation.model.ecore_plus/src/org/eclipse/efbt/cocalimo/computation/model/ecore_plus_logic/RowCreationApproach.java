/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EOperation;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Row Creation Approach</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * The RowCreationApproach which describes how Rows are generated, and linked to rows from the source cubes.
 *             	 The row Function by default does not set any column values in the rows, however to allow more flexibility in a language, we also allow optionally for the row function to set fields in the created row. 
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.RowCreationApproach#getCreatedCubeColumns <em>Created Cube Columns</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getRowCreationApproach()
 * @model extendedMetaData="name='RowCreationApproach' kind='elementOnly'"
 * @generated
 */
public interface RowCreationApproach extends EOperation {
	/**
	 * Returns the value of the '<em><b>Created Cube Columns</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.emf.ecore.EAttribute}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * Any cubeColumns which are set explicitly by the row function. 
	 * An example here would be uniquely setting a CubeColumn called index if the row function is responsable for creating multiple rows from one source row.
	 * Another example would be setting a field called index if the row is responsable for ordering a set of rows and setting index to indicate ordering position.
	 * In most common cases this field is empty.
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Created Cube Columns</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getRowCreationApproach_CreatedCubeColumns()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='createdCubeColumns'"
	 * @generated
	 */
	EList<EAttribute> getCreatedCubeColumns();

} // RowCreationApproach
