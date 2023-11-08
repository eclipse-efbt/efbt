/**
 */
package org.eclipse.efbt.regdna.model.regdna;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Generation Rules Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.GenerationRulesModule#getRulesForReport <em>Rules For Report</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getGenerationRulesModule()
 * @model
 * @generated
 */
public interface GenerationRulesModule extends org.eclipse.efbt.regdna.model.regdna.Module {
	/**
	 * Returns the value of the '<em><b>Rules For Report</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.regdna.model.regdna.RulesForReport}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Rules For Report</em>' containment reference list.
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getGenerationRulesModule_RulesForReport()
	 * @model containment="true"
	 * @generated
	 */
	EList<RulesForReport> getRulesForReport();

} // GenerationRulesModule
