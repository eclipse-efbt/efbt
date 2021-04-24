/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_views.tests;

import junit.textui.TestRunner;

import org.eclipse.efbt.language.dtm.model.dtm_views.AggregateEnrichmentViewAndOrderBy;
import org.eclipse.efbt.language.dtm.model.dtm_views.dtm_viewsFactory;

/**
 * <!-- begin-user-doc -->
 * A test case for the model object '<em><b>Aggregate Enrichment View And Order By</b></em>'.
 * <!-- end-user-doc -->
 * @generated
 */
public class AggregateEnrichmentViewAndOrderByTest extends DTMViewTest {

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static void main(String[] args) {
		TestRunner.run(AggregateEnrichmentViewAndOrderByTest.class);
	}

	/**
	 * Constructs a new Aggregate Enrichment View And Order By test case with the given name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public AggregateEnrichmentViewAndOrderByTest(String name) {
		super(name);
	}

	/**
	 * Returns the fixture for this Aggregate Enrichment View And Order By test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected AggregateEnrichmentViewAndOrderBy getFixture() {
		return (AggregateEnrichmentViewAndOrderBy)fixture;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see junit.framework.TestCase#setUp()
	 * @generated
	 */
	@Override
	protected void setUp() throws Exception {
		setFixture(dtm_viewsFactory.eINSTANCE.createAggregateEnrichmentViewAndOrderBy());
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

} //AggregateEnrichmentViewAndOrderByTest
