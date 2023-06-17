/**
 */
package org.eclipse.efbt.ecore4reg.model.ecore4reg;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>VTL Generated Intermediate Layer</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedIntermediateLayer#getDependant_enriched_cubes <em>Dependant enriched cubes</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedIntermediateLayer#getTransformations <em>Transformations</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedIntermediateLayer#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedIntermediateLayer#isIsEnrichment <em>Is Enrichment</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getVTLGeneratedIntermediateLayer()
 * @model
 * @generated
 */
public interface VTLGeneratedIntermediateLayer extends EObject {
	/**
	 * Returns the value of the '<em><b>Dependant enriched cubes</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Dependant enriched cubes</em>' reference.
	 * @see #setDependant_enriched_cubes(VTLGeneratedIntermediateLayer)
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getVTLGeneratedIntermediateLayer_Dependant_enriched_cubes()
	 * @model
	 * @generated
	 */
	VTLGeneratedIntermediateLayer getDependant_enriched_cubes();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedIntermediateLayer#getDependant_enriched_cubes <em>Dependant enriched cubes</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Dependant enriched cubes</em>' reference.
	 * @see #getDependant_enriched_cubes()
	 * @generated
	 */
	void setDependant_enriched_cubes(VTLGeneratedIntermediateLayer value);

	/**
	 * Returns the value of the '<em><b>Transformations</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLTransformation}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Transformations</em>' reference list.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getVTLGeneratedIntermediateLayer_Transformations()
	 * @model
	 * @generated
	 */
	EList<VTLTransformation> getTransformations();

	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getVTLGeneratedIntermediateLayer_Name()
	 * @model id="true"
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedIntermediateLayer#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

	/**
	 * Returns the value of the '<em><b>Is Enrichment</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Is Enrichment</em>' attribute.
	 * @see #setIsEnrichment(boolean)
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getVTLGeneratedIntermediateLayer_IsEnrichment()
	 * @model
	 * @generated
	 */
	boolean isIsEnrichment();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedIntermediateLayer#isIsEnrichment <em>Is Enrichment</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Is Enrichment</em>' attribute.
	 * @see #isIsEnrichment()
	 * @generated
	 */
	void setIsEnrichment(boolean value);

} // VTLGeneratedIntermediateLayer
