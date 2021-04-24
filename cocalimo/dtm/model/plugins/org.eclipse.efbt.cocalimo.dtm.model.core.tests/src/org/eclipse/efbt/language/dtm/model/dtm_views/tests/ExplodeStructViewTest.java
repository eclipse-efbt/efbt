/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_views.tests;

import junit.textui.TestRunner;

import org.eclipse.efbt.language.dtm.model.dtm_views.ExplodeStructView;
import org.eclipse.efbt.language.dtm.model.dtm_views.dtm_viewsFactory;

/**
 * <!-- begin-user-doc -->
 * A test case for the model object '<em><b>Explode Struct View</b></em>'.
 * <!-- end-user-doc -->
 * @generated
 */
public class ExplodeStructViewTest extends DTMViewTest {

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static void main(String[] args) {
		TestRunner.run(ExplodeStructViewTest.class);
	}

	/**
	 * Constructs a new Explode Struct View test case with the given name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ExplodeStructViewTest(String name) {
		super(name);
	}

	/**
	 * Returns the fixture for this Explode Struct View test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected ExplodeStructView getFixture() {
		return (ExplodeStructView)fixture;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see junit.framework.TestCase#setUp()
	 * @generated
	 */
	@Override
	protected void setUp() throws Exception {
		setFixture(dtm_viewsFactory.eINSTANCE.createExplodeStructView());
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

} //ExplodeStructViewTest
