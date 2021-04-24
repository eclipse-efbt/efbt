/**
 */
package org.eclipse.efbt.language.dtm.model.transformation.tests;

import junit.textui.TestRunner;

import org.eclipse.efbt.language.dtm.model.transformation.Release;
import org.eclipse.efbt.language.dtm.model.transformation.TransformationFactory;

/**
 * <!-- begin-user-doc -->
 * A test case for the model object '<em><b>Release</b></em>'.
 * <!-- end-user-doc -->
 * @generated
 */
public class ReleaseTest extends VersionedComponentsSetTest {

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static void main(String[] args) {
		TestRunner.run(ReleaseTest.class);
	}

	/**
	 * Constructs a new Release test case with the given name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ReleaseTest(String name) {
		super(name);
	}

	/**
	 * Returns the fixture for this Release test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected Release getFixture() {
		return (Release)fixture;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see junit.framework.TestCase#setUp()
	 * @generated
	 */
	@Override
	protected void setUp() throws Exception {
		setFixture(TransformationFactory.eINSTANCE.createRelease());
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

} //ReleaseTest
