/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_views.tests;

import junit.textui.TestRunner;

import org.eclipse.efbt.language.dtm.model.dtm_views.DTMView;
import org.eclipse.efbt.language.dtm.model.dtm_views.dtm_viewsFactory;

import org.eclipse.efbt.language.dtm.model.transformation.tests.DataSetTransformationTest;

/**
 * <!-- begin-user-doc -->
 * A test case for the model object '<em><b>DTM View</b></em>'.
 * <!-- end-user-doc -->
 * @generated
 */
public class DTMViewTest extends DataSetTransformationTest {

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static void main(String[] args) {
		TestRunner.run(DTMViewTest.class);
	}

	/**
	 * Constructs a new DTM View test case with the given name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public DTMViewTest(String name) {
		super(name);
	}

	/**
	 * Returns the fixture for this DTM View test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected DTMView getFixture() {
		return (DTMView)fixture;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see junit.framework.TestCase#setUp()
	 * @generated
	 */
	@Override
	protected void setUp() throws Exception {
		setFixture(dtm_viewsFactory.eINSTANCE.createDTMView());
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

} //DTMViewTest
