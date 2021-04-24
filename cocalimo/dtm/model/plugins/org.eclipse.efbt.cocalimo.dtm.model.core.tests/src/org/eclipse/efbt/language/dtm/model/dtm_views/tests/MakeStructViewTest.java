/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_views.tests;

import junit.textui.TestRunner;

import org.eclipse.efbt.language.dtm.model.dtm_views.MakeStructView;
import org.eclipse.efbt.language.dtm.model.dtm_views.dtm_viewsFactory;

/**
 * <!-- begin-user-doc -->
 * A test case for the model object '<em><b>Make Struct View</b></em>'.
 * <!-- end-user-doc -->
 * @generated
 */
public class MakeStructViewTest extends DTMViewTest {

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static void main(String[] args) {
		TestRunner.run(MakeStructViewTest.class);
	}

	/**
	 * Constructs a new Make Struct View test case with the given name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public MakeStructViewTest(String name) {
		super(name);
	}

	/**
	 * Returns the fixture for this Make Struct View test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected MakeStructView getFixture() {
		return (MakeStructView)fixture;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see junit.framework.TestCase#setUp()
	 * @generated
	 */
	@Override
	protected void setUp() throws Exception {
		setFixture(dtm_viewsFactory.eINSTANCE.createMakeStructView());
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

} //MakeStructViewTest
