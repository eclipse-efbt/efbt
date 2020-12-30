/**
 */
package org.eclipse.efbt.lineage.common.model.column_transformation_logic;

import org.eclipse.efbt.cocamo.smcubes.model.core.VARIABLE;

import org.eclipse.efbt.lineage.common.model.lineagecubes.cubes.FreeBirdToolsCube;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Cube Column</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Variable on a cube. For visualization it helps to use an analagoy of this being a column on  table.
 * Note that this class specifies the column (i.e. Variable), AND  the cube.
 * For example their might be 2 different CubeColumns with the same column. E.g there might be a CubeColumn with cube=Trades and column=Currency, and another CubeColumn with table=Position and column Currency.
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.lineage.common.model.column_transformation_logic.CubeColumn#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.lineage.common.model.column_transformation_logic.CubeColumn#getCube <em>Cube</em>}</li>
 *   <li>{@link org.eclipse.efbt.lineage.common.model.column_transformation_logic.CubeColumn#isUsedInSubsets <em>Used In Subsets</em>}</li>
 *   <li>{@link org.eclipse.efbt.lineage.common.model.column_transformation_logic.CubeColumn#getVariable <em>Variable</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.lineage.common.model.column_transformation_logic.Column_transformation_logicPackage#getCubeColumn()
 * @model extendedMetaData="name='CubeColumn' kind='empty'"
 * @generated
 */
public interface CubeColumn extends EObject
{
	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The CubeColumn ID.
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see org.eclipse.efbt.lineage.common.model.column_transformation_logic.Column_transformation_logicPackage#getCubeColumn_Name()
	 * @model id="true"
	 *        extendedMetaData="kind='attribute' name='name'"
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.lineage.common.model.column_transformation_logic.CubeColumn#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

	/**
	 * Returns the value of the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The associated Cube.
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Cube</em>' reference.
	 * @see #setCube(FreeBirdToolsCube)
	 * @see org.eclipse.efbt.lineage.common.model.column_transformation_logic.Column_transformation_logicPackage#getCubeColumn_Cube()
	 * @model extendedMetaData="kind='attribute' name='cube'"
	 * @generated
	 */
	FreeBirdToolsCube getCube();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.lineage.common.model.column_transformation_logic.CubeColumn#getCube <em>Cube</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Cube</em>' reference.
	 * @see #getCube()
	 * @generated
	 */
	void setCube(FreeBirdToolsCube value);

	/**
	 * Returns the value of the '<em><b>Used In Subsets</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * This can be a useful marker when used with attribute lineage models
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Used In Subsets</em>' attribute.
	 * @see #setUsedInSubsets(boolean)
	 * @see org.eclipse.efbt.lineage.common.model.column_transformation_logic.Column_transformation_logicPackage#getCubeColumn_UsedInSubsets()
	 * @model extendedMetaData="kind='attribute' name='usedInSubsets'"
	 * @generated
	 */
	boolean isUsedInSubsets();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.lineage.common.model.column_transformation_logic.CubeColumn#isUsedInSubsets <em>Used In Subsets</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Used In Subsets</em>' attribute.
	 * @see #isUsedInSubsets()
	 * @generated
	 */
	void setUsedInSubsets(boolean value);

	/**
	 * Returns the value of the '<em><b>Variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The associated Variable.
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Variable</em>' reference.
	 * @see #setVariable(VARIABLE)
	 * @see org.eclipse.efbt.lineage.common.model.column_transformation_logic.Column_transformation_logicPackage#getCubeColumn_Variable()
	 * @model extendedMetaData="kind='attribute' name='variable'"
	 * @generated
	 */
	VARIABLE getVariable();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.lineage.common.model.column_transformation_logic.CubeColumn#getVariable <em>Variable</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Variable</em>' reference.
	 * @see #getVariable()
	 * @generated
	 */
	void setVariable(VARIABLE value);

} // CubeColumn
