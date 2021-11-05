/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.rendering;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>CELL POSITION</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.CELL_POSITION#getAxis_ordinate_id <em>Axis ordinate id</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.CELL_POSITION#getCell_id <em>Cell id</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.RenderingPackage#getCELL_POSITION()
 * @model extendedMetaData="name='CELL_POSITION' kind='empty'"
 * @generated
 */
public interface CELL_POSITION extends EObject {
	/**
	 * Returns the value of the '<em><b>Axis ordinate id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Axis ordinate id</em>' reference.
	 * @see #setAxis_ordinate_id(AXIS_ORDINATE)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.RenderingPackage#getCELL_POSITION_Axis_ordinate_id()
	 * @model extendedMetaData="kind='attribute' name='axis_ordinate_id'"
	 * @generated
	 */
	AXIS_ORDINATE getAxis_ordinate_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.CELL_POSITION#getAxis_ordinate_id <em>Axis ordinate id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Axis ordinate id</em>' reference.
	 * @see #getAxis_ordinate_id()
	 * @generated
	 */
	void setAxis_ordinate_id(AXIS_ORDINATE value);

	/**
	 * Returns the value of the '<em><b>Cell id</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Cell id</em>' reference.
	 * @see #setCell_id(TABLE_CELL)
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.rendering.RenderingPackage#getCELL_POSITION_Cell_id()
	 * @model extendedMetaData="kind='attribute' name='cell_id'"
	 * @generated
	 */
	TABLE_CELL getCell_id();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.smcubes.model.rendering.CELL_POSITION#getCell_id <em>Cell id</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Cell id</em>' reference.
	 * @see #getCell_id()
	 * @generated
	 */
	void setCell_id(TABLE_CELL value);

} // CELL_POSITION
