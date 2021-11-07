/**
 */
package org.eclipse.efbt.cocalimo.core.model.logical_transformations.tests;

import junit.framework.TestCase;

import junit.textui.TestRunner;

import org.eclipse.efbt.cocalimo.core.model.logical_transformations.Logical_transformationsFactory;
import org.eclipse.efbt.cocalimo.core.model.logical_transformations.TaskTag;

/**
 * <!-- begin-user-doc -->
 * A test case for the model object '<em><b>Task Tag</b></em>'.
 * <!-- end-user-doc -->
 * @generated
 */
public class TaskTagTest extends TestCase {

	/**
	 * The fixture for this Task Tag test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected TaskTag fixture = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static void main(String[] args) {
		TestRunner.run(TaskTagTest.class);
	}

	/**
	 * Constructs a new Task Tag test case with the given name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TaskTagTest(String name) {
		super(name);
	}

	/**
	 * Sets the fixture for this Task Tag test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void setFixture(TaskTag fixture) {
		this.fixture = fixture;
	}

	/**
	 * Returns the fixture for this Task Tag test case.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected TaskTag getFixture() {
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
		setFixture(Logical_transformationsFactory.eINSTANCE.createTaskTag());
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

} //TaskTagTest
