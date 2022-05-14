/**
 */
package bird_model;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Concept</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link bird_model.Concept#getConceptName <em>Concept Name</em>}</li>
 * </ul>
 *
 * @see bird_model.Bird_modelPackage#getConcept()
 * @model
 * @generated
 */
public interface Concept extends EObject {
	/**
	 * Returns the value of the '<em><b>Concept Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Concept Name</em>' attribute.
	 * @see #setConceptName(String)
	 * @see bird_model.Bird_modelPackage#getConcept_ConceptName()
	 * @model
	 * @generated
	 */
	String getConceptName();

	/**
	 * Sets the value of the '{@link bird_model.Concept#getConceptName <em>Concept Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Concept Name</em>' attribute.
	 * @see #getConceptName()
	 * @generated
	 */
	void setConceptName(String value);

} // Concept