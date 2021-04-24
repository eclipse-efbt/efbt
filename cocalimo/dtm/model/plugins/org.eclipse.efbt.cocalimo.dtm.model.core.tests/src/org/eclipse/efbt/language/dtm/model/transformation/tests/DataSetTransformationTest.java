/**
 */
package org.eclipse.efbt.language.dtm.model.transformation.tests;

import junit.framework.TestCase;

import junit.textui.TestRunner;

import org.eclipse.efbt.language.dtm.model.transformation.DataSetTransformation;
import org.eclipse.efbt.language.dtm.model.transformation.TransformationFactory;

/**
 * <!-- begin-user-doc -->
 * A test case for the model object '<em><b>Data Set Transformation</b></em>'.
 * <!-- end-user-doc -->
 * @generated
 */
public class DataSetTransformationTest extends TestCase {

	/**
	 * The fixture for this Data Set Transformation test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected DataSetTransformation fixture = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static void main(String[] args) {
		TestRunner.run(DataSetTransformationTest.class);
	}

	/**
	 * Constructs a new Data Set Transformation test case with the given name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public DataSetTransformationTest(String name) {
		super(name);
	}

	/**
	 * Sets the fixture for this Data Set Transformation test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void setFixture(DataSetTransformation fixture) {
		this.fixture = fixture;
	}

	/**
	 * Returns the fixture for this Data Set Transformation test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected DataSetTransformation getFixture() {
		return fixture;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see junit.framework.TestCase#setUp()
	 * @generated
	 */
	@Override
	protected void setUp() throws Exception {
		setFixture(TransformationFactory.eINSTANCE.createDataSetTransformation());
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

} //DataSetTransformationTest
