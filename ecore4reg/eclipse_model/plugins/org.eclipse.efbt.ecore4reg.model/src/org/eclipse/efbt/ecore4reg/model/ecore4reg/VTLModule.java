/**
 */
package org.eclipse.efbt.ecore4reg.model.ecore4reg;


/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>VTL Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLModule#getVTLSchemes <em>VTL Schemes</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLModule#getVTLGeneratedOutputLayers <em>VTL Generated Output Layers</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLModule#getVTLGeneratedIntermediateLayers <em>VTL Generated Intermediate Layers</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLModule#getVTLEnrichedLayers <em>VTL Enriched Layers</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLModule#getVTLForSelectionLayers <em>VTL For Selection Layers</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLModule#getEntityToVTLIntermediateLayerLinks <em>Entity To VTL Intermediate Layer Links</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLModule#getVTLForViews <em>VTL For Views</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getVTLModule()
 * @model
 * @generated
 */
public interface VTLModule extends org.eclipse.efbt.ecore4reg.model.ecore4reg.Module {
	/**
	 * Returns the value of the '<em><b>VTL Schemes</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>VTL Schemes</em>' containment reference.
	 * @see #setVTLSchemes(VTLSchemesModule)
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getVTLModule_VTLSchemes()
	 * @model containment="true"
	 * @generated
	 */
	VTLSchemesModule getVTLSchemes();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLModule#getVTLSchemes <em>VTL Schemes</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>VTL Schemes</em>' containment reference.
	 * @see #getVTLSchemes()
	 * @generated
	 */
	void setVTLSchemes(VTLSchemesModule value);

	/**
	 * Returns the value of the '<em><b>VTL Generated Output Layers</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>VTL Generated Output Layers</em>' containment reference.
	 * @see #setVTLGeneratedOutputLayers(VTLGeneratedOutputlayerModule)
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getVTLModule_VTLGeneratedOutputLayers()
	 * @model containment="true"
	 * @generated
	 */
	VTLGeneratedOutputlayerModule getVTLGeneratedOutputLayers();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLModule#getVTLGeneratedOutputLayers <em>VTL Generated Output Layers</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>VTL Generated Output Layers</em>' containment reference.
	 * @see #getVTLGeneratedOutputLayers()
	 * @generated
	 */
	void setVTLGeneratedOutputLayers(VTLGeneratedOutputlayerModule value);

	/**
	 * Returns the value of the '<em><b>VTL Generated Intermediate Layers</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>VTL Generated Intermediate Layers</em>' containment reference.
	 * @see #setVTLGeneratedIntermediateLayers(VTLGeneratedIntermediateLayerModule)
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getVTLModule_VTLGeneratedIntermediateLayers()
	 * @model containment="true"
	 * @generated
	 */
	VTLGeneratedIntermediateLayerModule getVTLGeneratedIntermediateLayers();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLModule#getVTLGeneratedIntermediateLayers <em>VTL Generated Intermediate Layers</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>VTL Generated Intermediate Layers</em>' containment reference.
	 * @see #getVTLGeneratedIntermediateLayers()
	 * @generated
	 */
	void setVTLGeneratedIntermediateLayers(VTLGeneratedIntermediateLayerModule value);

	/**
	 * Returns the value of the '<em><b>VTL Enriched Layers</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>VTL Enriched Layers</em>' containment reference.
	 * @see #setVTLEnrichedLayers(VTLGeneratedIntermediateLayerModule)
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getVTLModule_VTLEnrichedLayers()
	 * @model containment="true"
	 * @generated
	 */
	VTLGeneratedIntermediateLayerModule getVTLEnrichedLayers();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLModule#getVTLEnrichedLayers <em>VTL Enriched Layers</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>VTL Enriched Layers</em>' containment reference.
	 * @see #getVTLEnrichedLayers()
	 * @generated
	 */
	void setVTLEnrichedLayers(VTLGeneratedIntermediateLayerModule value);

	/**
	 * Returns the value of the '<em><b>VTL For Selection Layers</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>VTL For Selection Layers</em>' containment reference.
	 * @see #setVTLForSelectionLayers(VTLForSelectionLayerModule)
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getVTLModule_VTLForSelectionLayers()
	 * @model containment="true"
	 * @generated
	 */
	VTLForSelectionLayerModule getVTLForSelectionLayers();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLModule#getVTLForSelectionLayers <em>VTL For Selection Layers</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>VTL For Selection Layers</em>' containment reference.
	 * @see #getVTLForSelectionLayers()
	 * @generated
	 */
	void setVTLForSelectionLayers(VTLForSelectionLayerModule value);

	/**
	 * Returns the value of the '<em><b>Entity To VTL Intermediate Layer Links</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Entity To VTL Intermediate Layer Links</em>' containment reference.
	 * @see #setEntityToVTLIntermediateLayerLinks(EntityToVTLIntermediateLayerLinkModule)
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getVTLModule_EntityToVTLIntermediateLayerLinks()
	 * @model containment="true"
	 * @generated
	 */
	EntityToVTLIntermediateLayerLinkModule getEntityToVTLIntermediateLayerLinks();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLModule#getEntityToVTLIntermediateLayerLinks <em>Entity To VTL Intermediate Layer Links</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Entity To VTL Intermediate Layer Links</em>' containment reference.
	 * @see #getEntityToVTLIntermediateLayerLinks()
	 * @generated
	 */
	void setEntityToVTLIntermediateLayerLinks(EntityToVTLIntermediateLayerLinkModule value);

	/**
	 * Returns the value of the '<em><b>VTL For Views</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>VTL For Views</em>' containment reference.
	 * @see #setVTLForViews(VTLForViewModule)
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getVTLModule_VTLForViews()
	 * @model containment="true"
	 * @generated
	 */
	VTLForViewModule getVTLForViews();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLModule#getVTLForViews <em>VTL For Views</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>VTL For Views</em>' containment reference.
	 * @see #getVTLForViews()
	 * @generated
	 */
	void setVTLForViews(VTLForViewModule value);

} // VTLModule
