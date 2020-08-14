/**
 */
package org.eclipse.efbt.lineage.common.model.column_transformation_logic;

import org.eclipse.efbt.common.model.functions.BasicFunction;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Standard Basic Column Function</b></em>'.
 * <!-- end-user-doc -->
 *
 * <!-- begin-model-doc -->
 * A Basic function which does not have multiple scenarios 
 * <!-- end-model-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.lineage.common.model.column_transformation_logic.StandardBasicColumnFunction#getBasicFunction <em>Basic Function</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.lineage.common.model.column_transformation_logic.Column_transformation_logicPackage#getStandardBasicColumnFunction()
 * @model extendedMetaData="name='StandardBasicColumnFunction' kind='elementOnly'"
 * @generated
 */
public interface StandardBasicColumnFunction extends BasicColumnFunction
{
	/**
	 * Returns the value of the '<em><b>Basic Function</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * <!-- begin-model-doc -->
	 * The associated function
	 * <!-- end-model-doc -->
	 * @return the value of the '<em>Basic Function</em>' containment reference.
	 * @see #setBasicFunction(BasicFunction)
	 * @see org.eclipse.efbt.lineage.common.model.column_transformation_logic.Column_transformation_logicPackage#getStandardBasicColumnFunction_BasicFunction()
	 * @model containment="true"
	 *        extendedMetaData="kind='element' name='basicFunction'"
	 * @generated
	 */
	BasicFunction getBasicFunction();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.lineage.common.model.column_transformation_logic.StandardBasicColumnFunction#getBasicFunction <em>Basic Function</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Basic Function</em>' containment reference.
	 * @see #getBasicFunction()
	 * @generated
	 */
	void setBasicFunction(BasicFunction value);

} // StandardBasicColumnFunction
