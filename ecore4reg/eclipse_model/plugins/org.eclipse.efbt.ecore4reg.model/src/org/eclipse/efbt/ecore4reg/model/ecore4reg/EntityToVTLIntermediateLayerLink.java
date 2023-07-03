/**
 */
package org.eclipse.efbt.ecore4reg.model.ecore4reg;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Entity To VTL Intermediate Layer Link</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.EntityToVTLIntermediateLayerLink#getVTLIntermediateLayer <em>VTL Intermediate Layer</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.EntityToVTLIntermediateLayerLink#getEntity <em>Entity</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.EntityToVTLIntermediateLayerLink#getTheFilter <em>The Filter</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getEntityToVTLIntermediateLayerLink()
 * @model
 * @generated
 */
public interface EntityToVTLIntermediateLayerLink extends EObject {
	/**
	 * Returns the value of the '<em><b>VTL Intermediate Layer</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>VTL Intermediate Layer</em>' reference.
	 * @see #setVTLIntermediateLayer(VTLGeneratedIntermediateLayer)
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getEntityToVTLIntermediateLayerLink_VTLIntermediateLayer()
	 * @model
	 * @generated
	 */
	VTLGeneratedIntermediateLayer getVTLIntermediateLayer();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.EntityToVTLIntermediateLayerLink#getVTLIntermediateLayer <em>VTL Intermediate Layer</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>VTL Intermediate Layer</em>' reference.
	 * @see #getVTLIntermediateLayer()
	 * @generated
	 */
	void setVTLIntermediateLayer(VTLGeneratedIntermediateLayer value);

	/**
	 * Returns the value of the '<em><b>Entity</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Entity</em>' reference.
	 * @see #setEntity(ELClass)
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getEntityToVTLIntermediateLayerLink_Entity()
	 * @model
	 * @generated
	 */
	ELClass getEntity();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.EntityToVTLIntermediateLayerLink#getEntity <em>Entity</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Entity</em>' reference.
	 * @see #getEntity()
	 * @generated
	 */
	void setEntity(ELClass value);

	/**
	 * Returns the value of the '<em><b>The Filter</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>The Filter</em>' attribute.
	 * @see #setTheFilter(String)
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#getEntityToVTLIntermediateLayerLink_TheFilter()
	 * @model
	 * @generated
	 */
	String getTheFilter();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.EntityToVTLIntermediateLayerLink#getTheFilter <em>The Filter</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>The Filter</em>' attribute.
	 * @see #getTheFilter()
	 * @generated
	 */
	void setTheFilter(String value);

} // EntityToVTLIntermediateLayerLink
