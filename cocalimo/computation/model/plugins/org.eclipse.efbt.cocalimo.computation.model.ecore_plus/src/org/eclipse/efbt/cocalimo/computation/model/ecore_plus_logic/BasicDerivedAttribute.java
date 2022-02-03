/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Basic Derived Attribute</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A ColumnFunction represnted by a BasicFunction (i.e. not an aggregating function).
 * The parameters of this function will be other CubeColumns, which might be a CubeColumn of a Base Cube, or another ColumnFunction from a DerivedCube.
 * 
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicDerivedAttribute#getBasicScenarioLogic <em>Basic Scenario Logic</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getBasicDerivedAttribute()
 * @model extendedMetaData="name='BasicColumnFunction' kind='empty'"
 * @generated
 */
public interface BasicDerivedAttribute extends DerivedAttribute {
	/**
	 * Returns the value of the '<em><b>Basic Scenario Logic</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicScenarioLogic}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Basic Scenario Logic</em>' containment reference list.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicPackage#getBasicDerivedAttribute_BasicScenarioLogic()
	 * @model containment="true"
	 * @generated
	 */
	EList<BasicScenarioLogic> getBasicScenarioLogic();

} // BasicDerivedAttribute
