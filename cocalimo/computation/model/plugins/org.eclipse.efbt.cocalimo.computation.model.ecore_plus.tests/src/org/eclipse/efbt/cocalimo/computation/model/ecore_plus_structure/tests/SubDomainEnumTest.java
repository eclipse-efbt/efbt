/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.tests;

import junit.textui.TestRunner;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.Ecore_plus_structureFactory;
import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.SubDomainEnum;

/**
 * <!-- begin-user-doc -->
 * A test case for the model object '<em><b>Sub Domain Enum</b></em>'.
 * <!-- end-user-doc -->
 * @generated
 */
public class SubDomainEnumTest extends HeirarchicalEnumTest {

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static void main(String[] args) {
		TestRunner.run(SubDomainEnumTest.class);
	}

	/**
	 * Constructs a new Sub Domain Enum test case with the given name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SubDomainEnumTest(String name) {
		super(name);
	}

	/**
	 * Returns the fixture for this Sub Domain Enum test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected SubDomainEnum getFixture() {
		return (SubDomainEnum)fixture;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see junit.framework.TestCase#setUp()
	 * @generated
	 */
	@Override
	protected void setUp() throws Exception {
		setFixture(Ecore_plus_structureFactory.eINSTANCE.createSubDomainEnum());
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

} //SubDomainEnumTest
