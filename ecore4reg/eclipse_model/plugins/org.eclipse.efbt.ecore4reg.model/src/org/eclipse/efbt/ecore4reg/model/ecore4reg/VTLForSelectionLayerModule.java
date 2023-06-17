/**
 */
package org.eclipse.efbt.ecore4reg.model.ecore4reg;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>VTL For Selection Layer Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForSelectionLayerModule#getVTLForSelectionLayers <em>VTL For Selection Layers</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getVTLForSelectionLayerModule()
 * @model
 * @generated
 */
public interface VTLForSelectionLayerModule extends org.eclipse.efbt.ecore4reg.model.ecore4reg.Module {
	/**
	 * Returns the value of the '<em><b>VTL For Selection Layers</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForSelectionLayer}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>VTL For Selection Layers</em>' containment reference list.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getVTLForSelectionLayerModule_VTLForSelectionLayers()
	 * @model containment="true"
	 * @generated
	 */
	EList<VTLForSelectionLayer> getVTLForSelectionLayers();

} // VTLForSelectionLayerModule
