/**
 */
package org.eclipse.efbt.cocalimo.core.model.functionality_module;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.Functionality_modulePackage
 * @generated
 */
public interface Functionality_moduleFactory extends EFactory {
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Functionality_moduleFactory eINSTANCE = org.eclipse.efbt.cocalimo.core.model.functionality_module.impl.Functionality_moduleFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>Data Processing Functionality Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Data Processing Functionality Module</em>'.
	 * @generated
	 */
	DataProcessingFunctionalityModule createDataProcessingFunctionalityModule();

	/**
	 * Returns a new object of class '<em>Leaf Functionality Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Leaf Functionality Module</em>'.
	 * @generated
	 */
	LeafFunctionalityModule createLeafFunctionalityModule();

	/**
	 * Returns a new object of class '<em>Parallel Functionality Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Parallel Functionality Module</em>'.
	 * @generated
	 */
	ParallelFunctionalityModule createParallelFunctionalityModule();

	/**
	 * Returns a new object of class '<em>Scenario Set Functionality Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Scenario Set Functionality Module</em>'.
	 * @generated
	 */
	ScenarioSetFunctionalityModule createScenarioSetFunctionalityModule();

	/**
	 * Returns a new object of class '<em>Serial Functionality Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Serial Functionality Module</em>'.
	 * @generated
	 */
	SerialFunctionalityModule createSerialFunctionalityModule();

	/**
	 * Returns a new object of class '<em>Functionality Module Set</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Functionality Module Set</em>'.
	 * @generated
	 */
	FunctionalityModuleSet createFunctionalityModuleSet();

	/**
	 * Returns a new object of class '<em>Read Data Source Functionality Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Read Data Source Functionality Module</em>'.
	 * @generated
	 */
	ReadDataSourceFunctionalityModule createReadDataSourceFunctionalityModule();

	/**
	 * Returns a new object of class '<em>Report Cell Creation Functionality Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Report Cell Creation Functionality Module</em>'.
	 * @generated
	 */
	ReportCellCreationFunctionalityModule createReportCellCreationFunctionalityModule();

	/**
	 * Returns a new object of class '<em>Functionality Module Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Functionality Module Module</em>'.
	 * @generated
	 */
	FunctionalityModuleModule createFunctionalityModuleModule();

	/**
	 * Returns a new object of class '<em>Functionality Module Tag</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Functionality Module Tag</em>'.
	 * @generated
	 */
	FunctionalityModuleTag createFunctionalityModuleTag();

	/**
	 * Returns a new object of class '<em>Validation Functionality Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Validation Functionality Module</em>'.
	 * @generated
	 */
	ValidationFunctionalityModule createValidationFunctionalityModule();

	/**
	 * Returns a new object of class '<em>Parallel Choice Functionality Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Parallel Choice Functionality Module</em>'.
	 * @generated
	 */
	ParallelChoiceFunctionalityModule createParallelChoiceFunctionalityModule();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	Functionality_modulePackage getFunctionality_modulePackage();

} //Functionality_moduleFactory
