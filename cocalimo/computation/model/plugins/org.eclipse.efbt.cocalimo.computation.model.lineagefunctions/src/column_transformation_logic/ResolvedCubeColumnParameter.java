/**
 */
package column_transformation_logic;

import functions.Parameter;

import org.eclipse.emf.ecore.EStructuralFeature;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Resolved Cube Column Parameter</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Cube Column parameter, this is a reference to a real existing CubeColumn.
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link column_transformation_logic.ResolvedCubeColumnParameter#getCubeColumn <em>Cube Column</em>}</li>
 * </ul>
 *
 * @see column_transformation_logic.Column_transformation_logicPackage#getResolvedCubeColumnParameter()
 * @model extendedMetaData="name='ResolvedCubeColumnParameter' kind='empty'"
 * @generated
 */
public interface ResolvedCubeColumnParameter extends Parameter {
	/**
	 * Returns the value of the '<em><b>Cube Column</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The CubeColumn this is a reference to a real existing cubeColumn.
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Cube Column</em>' reference.
	 * @see #setCubeColumn(EStructuralFeature)
	 * @see column_transformation_logic.Column_transformation_logicPackage#getResolvedCubeColumnParameter_CubeColumn()
	 * @model extendedMetaData="kind='attribute' name='cubeColumn'"
	 * @generated
	 */
	EStructuralFeature getCubeColumn();

	/**
	 * Sets the value of the '{@link column_transformation_logic.ResolvedCubeColumnParameter#getCubeColumn <em>Cube Column</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Cube Column</em>' reference.
	 * @see #getCubeColumn()
	 * @generated
	 */
	void setCubeColumn(EStructuralFeature value);

} // ResolvedCubeColumnParameter
