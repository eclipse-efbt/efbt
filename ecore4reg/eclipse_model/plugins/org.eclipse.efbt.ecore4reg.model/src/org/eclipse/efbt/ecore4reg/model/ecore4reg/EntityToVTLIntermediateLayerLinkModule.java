/**
 */
package org.eclipse.efbt.ecore4reg.model.ecore4reg;

import org.eclipse.emf.common.util.EList;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Entity To VTL Intermediate Layer Link Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.EntityToVTLIntermediateLayerLinkModule#getEntityToVTLIntermediateLayerLinks <em>Entity To VTL Intermediate Layer Links</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getEntityToVTLIntermediateLayerLinkModule()
 * @model
 * @generated
 */
public interface EntityToVTLIntermediateLayerLinkModule extends org.eclipse.efbt.ecore4reg.model.ecore4reg.Module {
	/**
	 * Returns the value of the '<em><b>Entity To VTL Intermediate Layer Links</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.ecore4reg.model.ecore4reg.EntityToVTLIntermediateLayerLink}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Entity To VTL Intermediate Layer Links</em>' containment reference list.
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getEntityToVTLIntermediateLayerLinkModule_EntityToVTLIntermediateLayerLinks()
	 * @model containment="true"
	 * @generated
	 */
	EList<EntityToVTLIntermediateLayerLink> getEntityToVTLIntermediateLayerLinks();

} // EntityToVTLIntermediateLayerLinkModule
