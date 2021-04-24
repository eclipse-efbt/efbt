/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_views.tests;

import junit.textui.TestRunner;

import org.eclipse.efbt.language.dtm.model.dtm_views.CopyView;
import org.eclipse.efbt.language.dtm.model.dtm_views.dtm_viewsFactory;

/**
 * <!-- begin-user-doc -->
 * A test case for the model object '<em><b>Copy View</b></em>'.
 * <!-- end-user-doc -->
 * @generated
 */
public class CopyViewTest extends DTMViewTest {

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static void main(String[] args) {
		TestRunner.run(CopyViewTest.class);
	}

	/**
	 * Constructs a new Copy View test case with the given name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CopyViewTest(String name) {
		super(name);
	}

	/**
	 * Returns the fixture for this Copy View test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected CopyView getFixture() {
		return (CopyView)fixture;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see junit.framework.TestCase#setUp()
	 * @generated
	 */
	@Override
	protected void setUp() throws Exception {
		setFixture(dtm_viewsFactory.eINSTANCE.createCopyView());
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

} //CopyViewTest
