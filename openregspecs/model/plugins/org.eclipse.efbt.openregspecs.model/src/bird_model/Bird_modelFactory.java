/**
 */
package bird_model;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see bird_model.Bird_modelPackage
 * @generated
 */
public interface Bird_modelFactory extends EFactory {
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Bird_modelFactory eINSTANCE = bird_model.impl.Bird_modelFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>BIRD Model</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>BIRD Model</em>'.
	 * @generated
	 */
	BIRDModel createBIRDModel();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	Bird_modelPackage getBird_modelPackage();

} //Bird_modelFactory
