/**
 */
package org.eclipse.efbt.cocalimo.core.model.scenarios;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Scenario</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Scenario
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.scenarios.Scenario#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.scenarios.Scenario#isUsedInSubset <em>Used In Subset</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.core.model.scenarios.ScenariosPackage#getScenario()
 * @model extendedMetaData="name='Scenario' kind='empty'"
 * @generated
 */
public interface Scenario extends EObject {
	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The name of the Scenario
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see org.eclipse.efbt.cocalimo.core.model.scenarios.ScenariosPackage#getScenario_Name()
	 * @model id="true"
	 *        extendedMetaData="kind='attribute' name='name'"
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.core.model.scenarios.Scenario#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

	/**
	 * Returns the value of the '<em><b>Used In Subset</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Used In Subset</em>' attribute.
	 * @see #setUsedInSubset(boolean)
	 * @see org.eclipse.efbt.cocalimo.core.model.scenarios.ScenariosPackage#getScenario_UsedInSubset()
	 * @model
	 * @generated
	 */
	boolean isUsedInSubset();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.core.model.scenarios.Scenario#isUsedInSubset <em>Used In Subset</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Used In Subset</em>' attribute.
	 * @see #isUsedInSubset()
	 * @generated
	 */
	void setUsedInSubset(boolean value);

} // Scenario
