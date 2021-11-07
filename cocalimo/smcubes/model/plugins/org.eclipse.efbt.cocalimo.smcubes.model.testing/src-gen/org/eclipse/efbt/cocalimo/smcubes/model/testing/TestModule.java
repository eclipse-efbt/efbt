/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.testing;

import org.eclipse.efbt.cocalimo.core.model.logical_transformations.LogicalTransformationModule;
import org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.SmcubesModel;
import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Test Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.TestModule#getSMCubesModel <em>SM Cubes Model</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.TestModule#getTests <em>Tests</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.TestModule#getLogicalTransformations <em>Logical Transformations</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.TestingPackage#getTestModule()
 * @model
 * @generated
 */
public interface TestModule extends org.eclipse.efbt.cocalimo.core.model.module_management.Module {
	/**
	 * Returns the value of the '<em><b>SM Cubes Model</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.cocalimo_smcubes_extension.SmcubesModel}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>SM Cubes Model</em>' reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.TestingPackage#getTestModule_SMCubesModel()
	 * @model
	 * @generated
	 */
	EList<SmcubesModel> getSMCubesModel();

	/**
	 * Returns the value of the '<em><b>Tests</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.smcubes.model.testing.Test}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Tests</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.TestingPackage#getTestModule_Tests()
	 * @model containment="true"
	 * @generated
	 */
	EList<Test> getTests();

	/**
	 * Returns the value of the '<em><b>Logical Transformations</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Logical Transformations</em>' reference.
	 * @see #setLogicalTransformations(LogicalTransformationModule)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.testing.TestingPackage#getTestModule_LogicalTransformations()
	 * @model
	 * @generated
	 */
	LogicalTransformationModule getLogicalTransformations();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.testing.TestModule#getLogicalTransformations <em>Logical Transformations</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Logical Transformations</em>' reference.
	 * @see #getLogicalTransformations()
	 * @generated
	 */
	void setLogicalTransformations(LogicalTransformationModule value);

} // TestModule
