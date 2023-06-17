/**
 */
package org.eclipse.efbt.ecore4reg.model.ecore4reg;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>VTL Generated Outputlayer Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedOutputlayerModule#getVTLGeneratedOutputlayerModules <em>VTL Generated Outputlayer Modules</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getVTLGeneratedOutputlayerModule()
 * @model
 * @generated
 */
public interface VTLGeneratedOutputlayerModule extends org.eclipse.efbt.ecore4reg.model.ecore4reg.Module {
	/**
	 * Returns the value of the '<em><b>VTL Generated Outputlayer Modules</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedOutputlayer}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>VTL Generated Outputlayer Modules</em>' containment reference list.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getVTLGeneratedOutputlayerModule_VTLGeneratedOutputlayerModules()
	 * @model containment="true"
	 * @generated
	 */
	EList<VTLGeneratedOutputlayer> getVTLGeneratedOutputlayerModules();

} // VTLGeneratedOutputlayerModule
