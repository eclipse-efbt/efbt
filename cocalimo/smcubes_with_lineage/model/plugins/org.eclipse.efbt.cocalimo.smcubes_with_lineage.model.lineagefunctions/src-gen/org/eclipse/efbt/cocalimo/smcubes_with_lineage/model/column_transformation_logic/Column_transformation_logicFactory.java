/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.Column_transformation_logicPackage
 * @generated
 */
public interface Column_transformation_logicFactory extends EFactory
{
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Column_transformation_logicFactory eINSTANCE = org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.impl.Column_transformation_logicFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>Aggregate Column Function</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Aggregate Column Function</em>'.
	 * @generated
	 */
	AggregateColumnFunction createAggregateColumnFunction();

	/**
	 * Returns a new object of class '<em>Basic Column Function</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Basic Column Function</em>'.
	 * @generated
	 */
	BasicColumnFunction createBasicColumnFunction();

	/**
	 * Returns a new object of class '<em>Column Function</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Column Function</em>'.
	 * @generated
	 */
	ColumnFunction createColumnFunction();

	/**
	 * Returns a new object of class '<em>Column Function Group</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Column Function Group</em>'.
	 * @generated
	 */
	ColumnFunctionGroup createColumnFunctionGroup();

	/**
	 * Returns a new object of class '<em>Resolved Cube Column Parameter</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Resolved Cube Column Parameter</em>'.
	 * @generated
	 */
	ResolvedCubeColumnParameter createResolvedCubeColumnParameter();

	/**
	 * Returns a new object of class '<em>Cube Column</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Cube Column</em>'.
	 * @generated
	 */
	CubeColumn createCubeColumn();

	/**
	 * Returns a new object of class '<em>Speculative Cube Column Parameter</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Speculative Cube Column Parameter</em>'.
	 * @generated
	 */
	SpeculativeCubeColumnParameter createSpeculativeCubeColumnParameter();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	Column_transformation_logicPackage getColumn_transformation_logicPackage();

} //Column_transformation_logicFactory
