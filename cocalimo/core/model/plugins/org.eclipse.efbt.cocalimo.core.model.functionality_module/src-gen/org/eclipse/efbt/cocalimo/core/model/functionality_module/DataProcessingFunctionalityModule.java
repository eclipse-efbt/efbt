/**
 */
package org.eclipse.efbt.cocalimo.core.model.functionality_module;

import org.eclipse.emf.ecore.EAttribute;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Data Processing Functionality Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A DataSetTransformationFunctionalityModule is a Functionality Module related to processing of data.
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.DataProcessingFunctionalityModule#getEnrichedAttribute <em>Enriched Attribute</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.Functionality_modulePackage#getDataProcessingFunctionalityModule()
 * @model
 * @generated
 */
public interface DataProcessingFunctionalityModule extends LeafFunctionalityModule {

	/**
	 * Returns the value of the '<em><b>Enriched Attribute</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Enriched Attribute</em>' reference.
	 * @see #setEnrichedAttribute(EAttribute)
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.Functionality_modulePackage#getDataProcessingFunctionalityModule_EnrichedAttribute()
	 * @model
	 * @generated
	 */
	EAttribute getEnrichedAttribute();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.DataProcessingFunctionalityModule#getEnrichedAttribute <em>Enriched Attribute</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Enriched Attribute</em>' reference.
	 * @see #getEnrichedAttribute()
	 * @generated
	 */
	void setEnrichedAttribute(EAttribute value);
} // DataProcessingFunctionalityModule
