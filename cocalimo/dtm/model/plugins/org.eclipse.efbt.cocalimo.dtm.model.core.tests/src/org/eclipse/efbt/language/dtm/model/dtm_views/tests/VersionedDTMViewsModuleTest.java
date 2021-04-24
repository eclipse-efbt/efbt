/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_views.tests;

import junit.textui.TestRunner;

import org.eclipse.efbt.language.dtm.model.dtm_views.VersionedDTMViewsModule;
import org.eclipse.efbt.language.dtm.model.dtm_views.dtm_viewsFactory;

import org.eclipse.efbt.language.dtm.model.transformation.tests.VersionedFunctionalModuleLogicTest;

/**
 * <!-- begin-user-doc -->
 * A test case for the model object '<em><b>Versioned DTM Views Module</b></em>'.
 * <!-- end-user-doc -->
 * @generated
 */
public class VersionedDTMViewsModuleTest extends VersionedFunctionalModuleLogicTest {

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static void main(String[] args) {
		TestRunner.run(VersionedDTMViewsModuleTest.class);
	}

	/**
	 * Constructs a new Versioned DTM Views Module test case with the given name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VersionedDTMViewsModuleTest(String name) {
		super(name);
	}

	/**
	 * Returns the fixture for this Versioned DTM Views Module test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected VersionedDTMViewsModule getFixture() {
		return (VersionedDTMViewsModule)fixture;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see junit.framework.TestCase#setUp()
	 * @generated
	 */
	@Override
	protected void setUp() throws Exception {
		setFixture(dtm_viewsFactory.eINSTANCE.createVersionedDTMViewsModule());
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

} //VersionedDTMViewsModuleTest
