/**
 */
package entities;

import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EStructuralFeature;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Cell</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link entities.Cell#getRow <em>Row</em>}</li>
 *   <li>{@link entities.Cell#getColumn <em>Column</em>}</li>
 * </ul>
 *
 * @see entities.EntitiesPackage#getCell()
 * @model
 * @generated
 */
public interface Cell extends EObject {
	/**
	 * Returns the value of the '<em><b>Row</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Row</em>' reference.
	 * @see #setRow(EObject)
	 * @see entities.EntitiesPackage#getCell_Row()
	 * @model
	 * @generated
	 */
	EObject getRow();

	/**
	 * Sets the value of the '{@link entities.Cell#getRow <em>Row</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Row</em>' reference.
	 * @see #getRow()
	 * @generated
	 */
	void setRow(EObject value);

	/**
	 * Returns the value of the '<em><b>Column</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Column</em>' reference.
	 * @see #setColumn(EStructuralFeature)
	 * @see entities.EntitiesPackage#getCell_Column()
	 * @model
	 * @generated
	 */
	EStructuralFeature getColumn();

	/**
	 * Sets the value of the '{@link entities.Cell#getColumn <em>Column</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Column</em>' reference.
	 * @see #getColumn()
	 * @generated
	 */
	void setColumn(EStructuralFeature value);

} // Cell
