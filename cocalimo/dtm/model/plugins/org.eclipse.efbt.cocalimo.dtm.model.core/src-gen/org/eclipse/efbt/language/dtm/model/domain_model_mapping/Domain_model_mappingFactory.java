/**
 */
package org.eclipse.efbt.language.dtm.model.domain_model_mapping;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.language.dtm.model.domain_model_mapping.Domain_model_mappingPackage
 * @generated
 */
public interface Domain_model_mappingFactory extends EFactory
{
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Domain_model_mappingFactory eINSTANCE = org.eclipse.efbt.language.dtm.model.domain_model_mapping.impl.Domain_model_mappingFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>Leaf Module To Versioned SQL Views Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Leaf Module To Versioned SQL Views Module</em>'.
	 * @generated
	 */
	LeafModuleToVersionedSQLViewsModule createLeafModuleToVersionedSQLViewsModule();

	/**
	 * Returns a new object of class '<em>Leaf Module To Versioned Cube Schema Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Leaf Module To Versioned Cube Schema Module</em>'.
	 * @generated
	 */
	LeafModuleToVersionedCubeSchemaModule createLeafModuleToVersionedCubeSchemaModule();

	/**
	 * Returns a new object of class '<em>Leaf Module To Report Cell View Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Leaf Module To Report Cell View Module</em>'.
	 * @generated
	 */
	LeafModuleToReportCellViewModule createLeafModuleToReportCellViewModule();

	/**
	 * Returns a new object of class '<em>Scenario To Code Conditions</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Scenario To Code Conditions</em>'.
	 * @generated
	 */
	ScenarioToCodeConditions createScenarioToCodeConditions();

	/**
	 * Returns a new object of class '<em>Code Condition</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Code Condition</em>'.
	 * @generated
	 */
	CodeCondition createCodeCondition();

	/**
	 * Returns a new object of class '<em>Column Setting Condition</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Column Setting Condition</em>'.
	 * @generated
	 */
	ColumnSettingCondition createColumnSettingCondition();

	/**
	 * Returns a new object of class '<em>Data Set Filter Condition</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Data Set Filter Condition</em>'.
	 * @generated
	 */
	DataSetFilterCondition createDataSetFilterCondition();

	/**
	 * Returns a new object of class '<em>Union Choice Condition</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Union Choice Condition</em>'.
	 * @generated
	 */
	UnionChoiceCondition createUnionChoiceCondition();

	/**
	 * Returns a new object of class '<em>Domain Mapping Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Domain Mapping Module</em>'.
	 * @generated
	 */
	DomainMappingModule createDomainMappingModule();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	Domain_model_mappingPackage getDomain_model_mappingPackage();

} //Domain_model_mappingFactory
