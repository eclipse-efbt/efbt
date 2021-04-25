/**
 */
package org.eclipse.efbt.cocalimo.core.model.test_definition;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Coverage Grid</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * For a TestConstraint, we can imagine the complete set of combinations of  possible parameters that meet the constraint, make up a coverage grid.
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_definition.CoverageGrid#getCoverageTestSet <em>Coverage Test Set</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_definition.CoverageGrid#getCoverageGridPoints <em>Coverage Grid Points</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_definition.CoverageGrid#getName <em>Name</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionPackage#getCoverageGrid()
 * @model
 * @generated
 */
public interface CoverageGrid extends EObject
{
	/**
	 * Returns the value of the '<em><b>Coverage Test Set</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The Test Constraint
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Coverage Test Set</em>' reference.
	 * @see #setCoverageTestSet(BDDTestContraints)
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionPackage#getCoverageGrid_CoverageTestSet()
	 * @model
	 * @generated
	 */
	BDDTestContraints getCoverageTestSet();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.CoverageGrid#getCoverageTestSet <em>Coverage Test Set</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Coverage Test Set</em>' reference.
	 * @see #getCoverageTestSet()
	 * @generated
	 */
	void setCoverageTestSet(BDDTestContraints value);

	/**
	 * Returns the value of the '<em><b>Coverage Grid Points</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * One Point of the Coverage grid
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Coverage Grid Points</em>' containment reference.
	 * @see #setCoverageGridPoints(CoverageGridPoint)
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionPackage#getCoverageGrid_CoverageGridPoints()
	 * @model containment="true"
	 * @generated
	 */
	CoverageGridPoint getCoverageGridPoints();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.CoverageGrid#getCoverageGridPoints <em>Coverage Grid Points</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Coverage Grid Points</em>' containment reference.
	 * @see #getCoverageGridPoints()
	 * @generated
	 */
	void setCoverageGridPoints(CoverageGridPoint value);

	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The name of the coverage grid
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionPackage#getCoverageGrid_Name()
	 * @model id="true"
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.core.model.test_definition.CoverageGrid#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

} // CoverageGrid
