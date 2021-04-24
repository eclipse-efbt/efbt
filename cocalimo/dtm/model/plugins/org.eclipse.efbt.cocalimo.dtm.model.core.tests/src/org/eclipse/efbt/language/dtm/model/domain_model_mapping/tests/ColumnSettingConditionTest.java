/**
 */
package org.eclipse.efbt.language.dtm.model.domain_model_mapping.tests;

import junit.textui.TestRunner;

import org.eclipse.efbt.language.dtm.model.domain_model_mapping.ColumnSettingCondition;
import org.eclipse.efbt.language.dtm.model.domain_model_mapping.Domain_model_mappingFactory;

/**
 * <!-- begin-user-doc -->
 * A test case for the model object '<em><b>Column Setting Condition</b></em>'.
 * <!-- end-user-doc -->
 * @generated
 */
public class ColumnSettingConditionTest extends CodeConditionTest {

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static void main(String[] args) {
		TestRunner.run(ColumnSettingConditionTest.class);
	}

	/**
	 * Constructs a new Column Setting Condition test case with the given name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ColumnSettingConditionTest(String name) {
		super(name);
	}

	/**
	 * Returns the fixture for this Column Setting Condition test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected ColumnSettingCondition getFixture() {
		return (ColumnSettingCondition)fixture;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see junit.framework.TestCase#setUp()
	 * @generated
	 */
	@Override
	protected void setUp() throws Exception {
		setFixture(Domain_model_mappingFactory.eINSTANCE.createColumnSettingCondition());
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

} //ColumnSettingConditionTest
