/**
 */
package org.eclipse.efbt.language.dtm.model.domain_model_mapping.tests;

import junit.textui.TestRunner;

import org.eclipse.efbt.language.dtm.model.domain_model_mapping.Domain_model_mappingFactory;
import org.eclipse.efbt.language.dtm.model.domain_model_mapping.UnionChoiceCondition;

/**
 * <!-- begin-user-doc -->
 * A test case for the model object '<em><b>Union Choice Condition</b></em>'.
 * <!-- end-user-doc -->
 * @generated
 */
public class UnionChoiceConditionTest extends CodeConditionTest {

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static void main(String[] args) {
		TestRunner.run(UnionChoiceConditionTest.class);
	}

	/**
	 * Constructs a new Union Choice Condition test case with the given name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public UnionChoiceConditionTest(String name) {
		super(name);
	}

	/**
	 * Returns the fixture for this Union Choice Condition test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected UnionChoiceCondition getFixture() {
		return (UnionChoiceCondition)fixture;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see junit.framework.TestCase#setUp()
	 * @generated
	 */
	@Override
	protected void setUp() throws Exception {
		setFixture(Domain_model_mappingFactory.eINSTANCE.createUnionChoiceCondition());
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

} //UnionChoiceConditionTest
