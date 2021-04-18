/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.Cell_transformation_logicPackage
 * @generated
 */
public interface Cell_transformation_logicFactory extends EFactory
{
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Cell_transformation_logicFactory eINSTANCE = org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cell_transformation_logic.impl.Cell_transformation_logicFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>Formula Cell</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Formula Cell</em>'.
	 * @generated
	 */
	FormulaCell createFormulaCell();

	/**
	 * Returns a new object of class '<em>Derived Cube Data</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Derived Cube Data</em>'.
	 * @generated
	 */
	DerivedCubeData createDerivedCubeData();

	/**
	 * Returns a new object of class '<em>Derived Row Data</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Derived Row Data</em>'.
	 * @generated
	 */
	DerivedRowData createDerivedRowData();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	Cell_transformation_logicPackage getCell_transformation_logicPackage();

} //Cell_transformation_logicFactory
