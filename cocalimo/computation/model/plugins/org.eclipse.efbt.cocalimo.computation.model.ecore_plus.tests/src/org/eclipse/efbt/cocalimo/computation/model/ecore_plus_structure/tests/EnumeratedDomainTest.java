/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.tests;

import junit.textui.TestRunner;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.Ecore_plus_structureFactory;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.EnumeratedDomain;

/**
 * <!-- begin-user-doc -->
 * A test case for the model object '<em><b>Enumerated Domain</b></em>'.
 * <!-- end-user-doc -->
 * @generated
 */
public class EnumeratedDomainTest extends HeirarchicalEnumTest {

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static void main(String[] args) {
		TestRunner.run(EnumeratedDomainTest.class);
	}

	/**
	 * Constructs a new Enumerated Domain test case with the given name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EnumeratedDomainTest(String name) {
		super(name);
	}

	/**
	 * Returns the fixture for this Enumerated Domain test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EnumeratedDomain getFixture() {
		return (EnumeratedDomain)fixture;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see junit.framework.TestCase#setUp()
	 * @generated
	 */
	@Override
	protected void setUp() throws Exception {
		setFixture(Ecore_plus_structureFactory.eINSTANCE.createEnumeratedDomain());
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

} //EnumeratedDomainTest
