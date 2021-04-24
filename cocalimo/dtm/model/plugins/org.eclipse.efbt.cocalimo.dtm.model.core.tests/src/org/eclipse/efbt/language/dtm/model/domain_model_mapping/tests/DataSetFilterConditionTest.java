/**
 */
package org.eclipse.efbt.language.dtm.model.domain_model_mapping.tests;

import junit.textui.TestRunner;

import org.eclipse.efbt.language.dtm.model.domain_model_mapping.DataSetFilterCondition;
import org.eclipse.efbt.language.dtm.model.domain_model_mapping.Domain_model_mappingFactory;

/**
 * <!-- begin-user-doc -->
 * A test case for the model object '<em><b>Data Set Filter Condition</b></em>'.
 * <!-- end-user-doc -->
 * @generated
 */
public class DataSetFilterConditionTest extends CodeConditionTest {

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static void main(String[] args) {
		TestRunner.run(DataSetFilterConditionTest.class);
	}

	/**
	 * Constructs a new Data Set Filter Condition test case with the given name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public DataSetFilterConditionTest(String name) {
		super(name);
	}

	/**
	 * Returns the fixture for this Data Set Filter Condition test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected DataSetFilterCondition getFixture() {
		return (DataSetFilterCondition)fixture;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see junit.framework.TestCase#setUp()
	 * @generated
	 */
	@Override
	protected void setUp() throws Exception {
		setFixture(Domain_model_mappingFactory.eINSTANCE.createDataSetFilterCondition());
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

} //DataSetFilterConditionTest
