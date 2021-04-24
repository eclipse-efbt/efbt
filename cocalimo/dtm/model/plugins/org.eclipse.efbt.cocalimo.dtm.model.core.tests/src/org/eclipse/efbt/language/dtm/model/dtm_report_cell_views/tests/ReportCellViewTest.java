/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_report_cell_views.tests;

import junit.textui.TestRunner;

import org.eclipse.efbt.language.dtm.model.dtm_report_cell_views.ReportCellView;
import org.eclipse.efbt.language.dtm.model.dtm_report_cell_views.dtm_report_cell_viewsFactory;

import org.eclipse.efbt.language.dtm.model.dtm_views.tests.DTMViewTest;

/**
 * <!-- begin-user-doc -->
 * A test case for the model object '<em><b>Report Cell View</b></em>'.
 * <!-- end-user-doc -->
 * @generated
 */
public class ReportCellViewTest extends DTMViewTest {

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static void main(String[] args) {
		TestRunner.run(ReportCellViewTest.class);
	}

	/**
	 * Constructs a new Report Cell View test case with the given name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ReportCellViewTest(String name) {
		super(name);
	}

	/**
	 * Returns the fixture for this Report Cell View test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected ReportCellView getFixture() {
		return (ReportCellView)fixture;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see junit.framework.TestCase#setUp()
	 * @generated
	 */
	@Override
	protected void setUp() throws Exception {
		setFixture(dtm_report_cell_viewsFactory.eINSTANCE.createReportCellView());
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

} //ReportCellViewTest
