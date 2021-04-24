/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_views.tests;

import junit.textui.TestRunner;

import org.eclipse.efbt.language.dtm.model.dtm_views.CastColumnView;
import org.eclipse.efbt.language.dtm.model.dtm_views.dtm_viewsFactory;

/**
 * <!-- begin-user-doc -->
 * A test case for the model object '<em><b>Cast Column View</b></em>'.
 * <!-- end-user-doc -->
 * @generated
 */
public class CastColumnViewTest extends DTMViewTest {

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static void main(String[] args) {
		TestRunner.run(CastColumnViewTest.class);
	}

	/**
	 * Constructs a new Cast Column View test case with the given name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CastColumnViewTest(String name) {
		super(name);
	}

	/**
	 * Returns the fixture for this Cast Column View test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected CastColumnView getFixture() {
		return (CastColumnView)fixture;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see junit.framework.TestCase#setUp()
	 * @generated
	 */
	@Override
	protected void setUp() throws Exception {
		setFixture(dtm_viewsFactory.eINSTANCE.createCastColumnView());
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

} //CastColumnViewTest
