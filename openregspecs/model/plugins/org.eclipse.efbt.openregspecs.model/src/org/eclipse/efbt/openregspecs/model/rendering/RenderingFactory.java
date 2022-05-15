/**
 */
package org.eclipse.efbt.openregspecs.model.rendering;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.openregspecs.model.rendering.RenderingPackage
 * @generated
 */
public interface RenderingFactory extends EFactory {
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	RenderingFactory eINSTANCE = org.eclipse.efbt.openregspecs.model.rendering.impl.RenderingFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>AXIS</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>AXIS</em>'.
	 * @generated
	 */
	AXIS createAXIS();

	/**
	 * Returns a new object of class '<em>AXIS ORDINATE</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>AXIS ORDINATE</em>'.
	 * @generated
	 */
	AXIS_ORDINATE createAXIS_ORDINATE();

	/**
	 * Returns a new object of class '<em>CELL POSITION</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>CELL POSITION</em>'.
	 * @generated
	 */
	CELL_POSITION createCELL_POSITION();

	/**
	 * Returns a new object of class '<em>ORDINATE ITEM</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>ORDINATE ITEM</em>'.
	 * @generated
	 */
	ORDINATE_ITEM createORDINATE_ITEM();

	/**
	 * Returns a new object of class '<em>TABLE</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>TABLE</em>'.
	 * @generated
	 */
	TABLE createTABLE();

	/**
	 * Returns a new object of class '<em>TABLE CELL</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>TABLE CELL</em>'.
	 * @generated
	 */
	TABLE_CELL createTABLE_CELL();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	RenderingPackage getRenderingPackage();

} //RenderingFactory
