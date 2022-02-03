/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.tests;

import junit.textui.TestRunner;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntity;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicFactory;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.tests.EntityTest;

/**
 * <!-- begin-user-doc -->
 * A test case for the model object '<em><b>Derived Entity</b></em>'.
 * <!-- end-user-doc -->
 * @generated
 */
public class DerivedEntityTest extends EntityTest {

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static void main(String[] args) {
		TestRunner.run(DerivedEntityTest.class);
	}

	/**
	 * Constructs a new Derived Entity test case with the given name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public DerivedEntityTest(String name) {
		super(name);
	}

	/**
	 * Returns the fixture for this Derived Entity test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected DerivedEntity getFixture() {
		return (DerivedEntity)fixture;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see junit.framework.TestCase#setUp()
	 * @generated
	 */
	@Override
	protected void setUp() throws Exception {
		setFixture(Ecore_plus_logicFactory.eINSTANCE.createDerivedEntity());
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see junit.framework.TestCase#tearDown()
	 * @generated
	 */
	@Override
	protected void tearDown() throws Exception {
		setFixture(null);
	}

} //DerivedEntityTest
