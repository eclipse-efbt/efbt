/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.Ecore_plus_structurePackage
 * @generated
 */
public interface Ecore_plus_structureFactory extends EFactory {
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Ecore_plus_structureFactory eINSTANCE = org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.impl.Ecore_plus_structureFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>Base Entity</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Base Entity</em>'.
	 * @generated
	 */
	BaseEntity createBaseEntity();

	/**
	 * Returns a new object of class '<em>Cell</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Cell</em>'.
	 * @generated
	 */
	Cell createCell();

	/**
	 * Returns a new object of class '<em>Heirarchical Enum</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Heirarchical Enum</em>'.
	 * @generated
	 */
	HeirarchicalEnum createHeirarchicalEnum();

	/**
	 * Returns a new object of class '<em>Heirarchical Enum Literal</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Heirarchical Enum Literal</em>'.
	 * @generated
	 */
	HeirarchicalEnumLiteral createHeirarchicalEnumLiteral();

	/**
	 * Returns a new object of class '<em>Enumerated Domain</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Enumerated Domain</em>'.
	 * @generated
	 */
	EnumeratedDomain createEnumeratedDomain();

	/**
	 * Returns a new object of class '<em>Sub Domain Enum</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Sub Domain Enum</em>'.
	 * @generated
	 */
	SubDomainEnum createSubDomainEnum();

	/**
	 * Returns a new object of class '<em>Cells Parameter</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Cells Parameter</em>'.
	 * @generated
	 */
	CellsParameter createCellsParameter();

	/**
	 * Returns a new object of class '<em>Entity</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Entity</em>'.
	 * @generated
	 */
	Entity createEntity();

	/**
	 * Returns a new object of class '<em>Base Entity Feature</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Base Entity Feature</em>'.
	 * @generated
	 */
	BaseEntityFeature createBaseEntityFeature();

	/**
	 * Returns a new object of class '<em>Base Entity Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Base Entity Module</em>'.
	 * @generated
	 */
	BaseEntityModule createBaseEntityModule();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	Ecore_plus_structurePackage getEcore_plus_structurePackage();

} //Ecore_plus_structureFactory
