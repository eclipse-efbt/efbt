/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage.Data_lineagePackage
 * @generated
 */
public interface Data_lineageFactory extends EFactory
{
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Data_lineageFactory eINSTANCE = org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.data_lineage.impl.Data_lineageFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>Data Lineage Model</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Data Lineage Model</em>'.
	 * @generated
	 */
	DataLineageModel createDataLineageModel();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	Data_lineagePackage getData_lineagePackage();

} //Data_lineageFactory
