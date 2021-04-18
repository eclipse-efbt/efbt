/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.Incremental_cubesPackage
 * @generated
 */
public interface Incremental_cubesFactory extends EFactory
{
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Incremental_cubesFactory eINSTANCE = org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.incremental_cubes.impl.Incremental_cubesFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>Base Delta Cube</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Base Delta Cube</em>'.
	 * @generated
	 */
	BaseDeltaCube createBaseDeltaCube();

	/**
	 * Returns a new object of class '<em>Base View Cube</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Base View Cube</em>'.
	 * @generated
	 */
	BaseViewCube createBaseViewCube();

	/**
	 * Returns a new object of class '<em>Delta Accumulation Cube</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Delta Accumulation Cube</em>'.
	 * @generated
	 */
	DeltaAccumulationCube createDeltaAccumulationCube();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	Incremental_cubesPackage getIncremental_cubesPackage();

} //Incremental_cubesFactory
