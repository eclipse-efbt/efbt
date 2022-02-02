/**
 */
package row_transformation_logic;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Row Creation Approach For Entity</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A description of logic and dependencies for describing how the set rows in a derived cube is created, and how the source rows of each of those rows is set.
 *       For example a CubeTransformationeLogic describing a Group By situation on 1 million trades by currency, would result in the derived cube
 *       having one row per currency, and each of those rows would have a link to all the source rows with that currency.
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link row_transformation_logic.RowCreationApproachForEntity#getRowCreationApproach <em>Row Creation Approach</em>}</li>
 *   <li>{@link row_transformation_logic.RowCreationApproachForEntity#getEntity <em>Entity</em>}</li>
 *   <li>{@link row_transformation_logic.RowCreationApproachForEntity#getName <em>Name</em>}</li>
 * </ul>
 *
 * @see row_transformation_logic.Row_transformation_logicPackage#getRowCreationApproachForEntity()
 * @model extendedMetaData="name='RowCreationApproachForCube' kind='elementOnly'"
 * @generated
 */
public interface RowCreationApproachForEntity extends EObject {
	/**
	 * Returns the value of the '<em><b>Row Creation Approach</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The RowCreationApproach which describes how Rows are generated, this will be a sub class of RowCreationApproach such as JoinRowCreationApproach
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Row Creation Approach</em>' containment reference.
	 * @see #setRowCreationApproach(RowCreationApproach)
	 * @see row_transformation_logic.Row_transformation_logicPackage#getRowCreationApproachForEntity_RowCreationApproach()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='rowCreationApproach'"
	 * @generated
	 */
	RowCreationApproach getRowCreationApproach();

	/**
	 * Sets the value of the '{@link row_transformation_logic.RowCreationApproachForEntity#getRowCreationApproach <em>Row Creation Approach</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Row Creation Approach</em>' containment reference.
	 * @see #getRowCreationApproach()
	 * @generated
	 */
	void setRowCreationApproach(RowCreationApproach value);

	/**
	 * Returns the value of the '<em><b>Entity</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The derivedCube
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Entity</em>' reference.
	 * @see #setEntity(EClass)
	 * @see row_transformation_logic.Row_transformation_logicPackage#getRowCreationApproachForEntity_Entity()
	 * @model extendedMetaData="kind='attribute' name='cube'"
	 * @generated
	 */
	EClass getEntity();

	/**
	 * Sets the value of the '{@link row_transformation_logic.RowCreationApproachForEntity#getEntity <em>Entity</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Entity</em>' reference.
	 * @see #getEntity()
	 * @generated
	 */
	void setEntity(EClass value);

	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The name of the RowCreationApproachForCube
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see row_transformation_logic.Row_transformation_logicPackage#getRowCreationApproachForEntity_Name()
	 * @model id="true"
	 *        extendedMetaData="kind='attribute' name='name'"
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link row_transformation_logic.RowCreationApproachForEntity#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

} // RowCreationApproachForEntity
