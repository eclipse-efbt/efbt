/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.cocamo;

import org.eclipse.efbt.cocalimo.core.model.test.TestModule;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>SM Cubes Reg Functionality Test Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocamo.SMCubesRegFunctionalityTestModule#getRegFunctionalityTests <em>Reg Functionality Tests</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocamo.CocamoPackage#getSMCubesRegFunctionalityTestModule()
 * @model
 * @generated
 */
public interface SMCubesRegFunctionalityTestModule extends TestModule
{
	/**
	 * Returns the value of the '<em><b>Reg Functionality Tests</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.cocamo.SMCubesRegFunctionalityTest}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Reg Functionality Tests</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.cocamo.CocamoPackage#getSMCubesRegFunctionalityTestModule_RegFunctionalityTests()
	 * @model containment="true"
	 * @generated
	 */
	EList<SMCubesRegFunctionalityTest> getRegFunctionalityTests();

} // SMCubesRegFunctionalityTestModule
