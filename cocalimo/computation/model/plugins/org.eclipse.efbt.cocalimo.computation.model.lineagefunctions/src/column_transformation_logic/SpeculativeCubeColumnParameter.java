/**
 */
package column_transformation_logic;

import functions.Parameter;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Speculative Cube Column Parameter</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Parameter where we are taking a CubeColumn from a cube.
 *  Speculative here relates to the fact that this cubeColumn may or may not actually exist. 
 *  Note that during development of a program that set of actually existing CubeColumns changes.
 *  Allowing a specultiveCubeColumnParameter    means that we can develop more quickly, and run checks occasionally to check that all SpeculativeCubeColumnParameters can be resolved  to real CubeColumns.
 *                
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link column_transformation_logic.SpeculativeCubeColumnParameter#getCube <em>Cube</em>}</li>
 *   <li>{@link column_transformation_logic.SpeculativeCubeColumnParameter#getAttribute <em>Attribute</em>}</li>
 * </ul>
 *
 * @see column_transformation_logic.Column_transformation_logicPackage#getSpeculativeCubeColumnParameter()
 * @model extendedMetaData="name='SpeculativeCubeColumnParameter' kind='empty'"
 * @generated
 */
public interface SpeculativeCubeColumnParameter extends Parameter {
	/**
	 * Returns the value of the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The cube of the desired cube Column
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Cube</em>' reference.
	 * @see #setCube(EClass)
	 * @see column_transformation_logic.Column_transformation_logicPackage#getSpeculativeCubeColumnParameter_Cube()
	 * @model extendedMetaData="kind='attribute' name='cube'"
	 * @generated
	 */
	EClass getCube();

	/**
	 * Sets the value of the '{@link column_transformation_logic.SpeculativeCubeColumnParameter#getCube <em>Cube</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Cube</em>' reference.
	 * @see #getCube()
	 * @generated
	 */
	void setCube(EClass value);

	/**
	 * Returns the value of the '<em><b>Attribute</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Attribute</em>' reference.
	 * @see #setAttribute(EAttribute)
	 * @see column_transformation_logic.Column_transformation_logicPackage#getSpeculativeCubeColumnParameter_Attribute()
	 * @model
	 * @generated
	 */
	EAttribute getAttribute();

	/**
	 * Sets the value of the '{@link column_transformation_logic.SpeculativeCubeColumnParameter#getAttribute <em>Attribute</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Attribute</em>' reference.
	 * @see #getAttribute()
	 * @generated
	 */
	void setAttribute(EAttribute value);

} // SpeculativeCubeColumnParameter
