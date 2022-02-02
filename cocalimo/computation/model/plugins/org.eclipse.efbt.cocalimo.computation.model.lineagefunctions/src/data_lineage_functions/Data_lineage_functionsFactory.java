/**
 */
package data_lineage_functions;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see data_lineage_functions.Data_lineage_functionsPackage
 * @generated
 */
public interface Data_lineage_functionsFactory extends EFactory {
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Data_lineage_functionsFactory eINSTANCE = data_lineage_functions.impl.Data_lineage_functionsFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>Cells Parameter</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Cells Parameter</em>'.
	 * @generated
	 */
	CellsParameter createCellsParameter();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	Data_lineage_functionsPackage getData_lineage_functionsPackage();

} //Data_lineage_functionsFactory
