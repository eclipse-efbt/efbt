/**
 */
package entities;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Derived Entity</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link entities.DerivedEntity#getSourceEntities <em>Source Entities</em>}</li>
 * </ul>
 *
 * @see entities.EntitiesPackage#getDerivedEntity()
 * @model
 * @generated
 */
public interface DerivedEntity extends EClass {
	/**
	 * Returns the value of the '<em><b>Source Entities</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.emf.ecore.EClass}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Source Entities</em>' reference list.
	 * @see entities.EntitiesPackage#getDerivedEntity_SourceEntities()
	 * @model
	 * @generated
	 */
	EList<EClass> getSourceEntities();

} // DerivedEntity
