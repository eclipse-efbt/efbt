/**
 */
package attribute_lineage;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see attribute_lineage.Attribute_lineagePackage
 * @generated
 */
public interface Attribute_lineageFactory extends EFactory {
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Attribute_lineageFactory eINSTANCE = attribute_lineage.impl.Attribute_lineageFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>Attribute Lineage Model</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Attribute Lineage Model</em>'.
	 * @generated
	 */
	AttributeLineageModel createAttributeLineageModel();

	/**
	 * Returns a new object of class '<em>Attribute Lineage Model Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Attribute Lineage Model Module</em>'.
	 * @generated
	 */
	AttributeLineageModelModule createAttributeLineageModelModule();

	/**
	 * Returns a new object of class '<em>Is Not Subset</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Is Not Subset</em>'.
	 * @generated
	 */
	IsNotSubset createIsNotSubset();

	/**
	 * Returns a new object of class '<em>Is Subset</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Is Subset</em>'.
	 * @generated
	 */
	IsSubset createIsSubset();

	/**
	 * Returns a new object of class '<em>Subset Boolean</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Subset Boolean</em>'.
	 * @generated
	 */
	SubsetBoolean createSubsetBoolean();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	Attribute_lineagePackage getAttribute_lineagePackage();

} //Attribute_lineageFactory
