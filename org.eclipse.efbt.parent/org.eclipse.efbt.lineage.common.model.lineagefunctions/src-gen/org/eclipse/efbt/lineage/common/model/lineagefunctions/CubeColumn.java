/**
 */
package org.eclipse.efbt.lineage.common.model.lineagefunctions;

import org.eclipse.efbt.data_structures.smcubes.model.core.VARIABLE;

import org.eclipse.efbt.lineage.common.model.cubes.FreeBirdToolsCube;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Cube Column</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A column on a cube.
 * Note that this class specifies the column , AND  the cube.
 * For example their might be 2 different CubeColumns with the same column. E.g there might be a CubeColumn with cube=Trades and column=Currency, and another CubeColumn with table=Position and column Currency.
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.lineage.common.model.lineagefunctions.CubeColumn#getColumnID <em>Column ID</em>}</li>
 *   <li>{@link org.eclipse.efbt.lineage.common.model.lineagefunctions.CubeColumn#getCube <em>Cube</em>}</li>
 *   <li>{@link org.eclipse.efbt.lineage.common.model.lineagefunctions.CubeColumn#isUsedInSubsets <em>Used In Subsets</em>}</li>
 *   <li>{@link org.eclipse.efbt.lineage.common.model.lineagefunctions.CubeColumn#getVariable <em>Variable</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.lineage.common.model.lineagefunctions.LineagefunctionsPackage#getCubeColumn()
 * @model extendedMetaData="name='CubeColumn' kind='empty'"
 * @generated
 */
public interface CubeColumn extends EObject
{
	/**
	 * Returns the value of the '<em><b>Column ID</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The CubeColumn ID.
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Column ID</em>' attribute.
	 * @see #setColumnID(String)
	 * @see org.eclipse.efbt.lineage.common.model.lineagefunctions.LineagefunctionsPackage#getCubeColumn_ColumnID()
	 * @model id="true"
	 *        extendedMetaData="kind='attribute' name='columnID'"
	 * @generated
	 */
	String getColumnID();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.lineage.common.model.lineagefunctions.CubeColumn#getColumnID <em>Column ID</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Column ID</em>' attribute.
	 * @see #getColumnID()
	 * @generated
	 */
	void setColumnID(String value);

	/**
	 * Returns the value of the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The associated Cube.
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Cube</em>' reference.
	 * @see #setCube(FreeBirdToolsCube)
	 * @see org.eclipse.efbt.lineage.common.model.lineagefunctions.LineagefunctionsPackage#getCubeColumn_Cube()
	 * @model extendedMetaData="kind='attribute' name='cube'"
	 * @generated
	 */
	FreeBirdToolsCube getCube();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.lineage.common.model.lineagefunctions.CubeColumn#getCube <em>Cube</em>}' reference.
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
	 * The CompareAttributeLineageModels PlatformCall in the platform_call package creates an AttributeLineageModel where items (such as CubeColumns) which are in both models are marked with usedInSubset=true
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Used In Subsets</em>' attribute.
	 * @see #setUsedInSubsets(boolean)
	 * @see org.eclipse.efbt.lineage.common.model.lineagefunctions.LineagefunctionsPackage#getCubeColumn_UsedInSubsets()
	 * @model extendedMetaData="kind='attribute' name='usedInSubsets'"
	 * @generated
	 */
	boolean isUsedInSubsets();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.lineage.common.model.lineagefunctions.CubeColumn#isUsedInSubsets <em>Used In Subsets</em>}' attribute.
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
	 * @see org.eclipse.efbt.lineage.common.model.lineagefunctions.LineagefunctionsPackage#getCubeColumn_Variable()
	 * @model extendedMetaData="kind='attribute' name='variable'"
	 * @generated
	 */
	VARIABLE getVariable();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.lineage.common.model.lineagefunctions.CubeColumn#getVariable <em>Variable</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Variable</em>' reference.
	 * @see #getVariable()
	 * @generated
	 */
	void setVariable(VARIABLE value);

} // CubeColumn
