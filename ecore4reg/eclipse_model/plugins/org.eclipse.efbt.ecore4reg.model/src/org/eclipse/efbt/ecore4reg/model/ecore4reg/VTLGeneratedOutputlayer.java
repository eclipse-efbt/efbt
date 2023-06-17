/**
 */
package org.eclipse.efbt.ecore4reg.model.ecore4reg;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>VTL Generated Outputlayer</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedOutputlayer#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedOutputlayer#getDependant_intermediate_layers <em>Dependant intermediate layers</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedOutputlayer#getVTLForOutputLayerAndIntemedateLayerCombinations <em>VTL For Output Layer And Intemedate Layer Combinations</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedOutputlayer#getOutputLayer <em>Output Layer</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getVTLGeneratedOutputlayer()
 * @model
 * @generated
 */
public interface VTLGeneratedOutputlayer extends EObject {
	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getVTLGeneratedOutputlayer_Name()
	 * @model id="true"
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedOutputlayer#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

	/**
	 * Returns the value of the '<em><b>Dependant intermediate layers</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedIntermediateLayer}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Dependant intermediate layers</em>' reference list.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getVTLGeneratedOutputlayer_Dependant_intermediate_layers()
	 * @model
	 * @generated
	 */
	EList<VTLGeneratedIntermediateLayer> getDependant_intermediate_layers();

	/**
	 * Returns the value of the '<em><b>VTL For Output Layer And Intemedate Layer Combinations</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForOutputLayerAndIntermediateLayerCombination}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>VTL For Output Layer And Intemedate Layer Combinations</em>' containment reference list.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getVTLGeneratedOutputlayer_VTLForOutputLayerAndIntemedateLayerCombinations()
	 * @model containment="true"
	 * @generated
	 */
	EList<VTLForOutputLayerAndIntermediateLayerCombination> getVTLForOutputLayerAndIntemedateLayerCombinations();

	/**
	 * Returns the value of the '<em><b>Output Layer</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Output Layer</em>' reference.
	 * @see #setOutputLayer(ELClass)
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getVTLGeneratedOutputlayer_OutputLayer()
	 * @model
	 * @generated
	 */
	ELClass getOutputLayer();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedOutputlayer#getOutputLayer <em>Output Layer</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Output Layer</em>' reference.
	 * @see #getOutputLayer()
	 * @generated
	 */
	void setOutputLayer(ELClass value);

} // VTLGeneratedOutputlayer
