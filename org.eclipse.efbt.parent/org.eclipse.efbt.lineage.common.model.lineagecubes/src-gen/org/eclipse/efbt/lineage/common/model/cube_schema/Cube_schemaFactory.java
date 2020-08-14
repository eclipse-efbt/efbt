/**
 */
package org.eclipse.efbt.lineage.common.model.cube_schema;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.lineage.common.model.cube_schema.Cube_schemaPackage
 * @generated
 */
public interface Cube_schemaFactory extends EFactory
{
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Cube_schemaFactory eINSTANCE = org.eclipse.efbt.lineage.common.model.cube_schema.impl.Cube_schemaFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>Cube Schema</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Cube Schema</em>'.
	 * @generated
	 */
	CubeSchema createCubeSchema();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	Cube_schemaPackage getCube_schemaPackage();

} //Cube_schemaFactory
