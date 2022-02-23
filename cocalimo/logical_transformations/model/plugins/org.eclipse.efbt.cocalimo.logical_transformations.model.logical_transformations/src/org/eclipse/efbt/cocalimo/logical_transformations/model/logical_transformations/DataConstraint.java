/**
 */
package org.eclipse.efbt.cocalimo.logical_transformations.model.logical_transformations;

import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Attribute;
import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Data Constraint</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.logical_transformations.model.logical_transformations.DataConstraint#getAttr1 <em>Attr1</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.logical_transformations.model.logical_transformations.DataConstraint#getAttr2 <em>Attr2</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.logical_transformations.model.logical_transformations.DataConstraint#getComparison <em>Comparison</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.cocalimo.logical_transformations.model.logical_transformations.Logical_transformationsPackage#getDataConstraint()
 * @model
 * @generated
 */
public interface DataConstraint extends EObject {
	/**
	 * Returns the value of the '<em><b>Attr1</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Attr1</em>' reference.
	 * @see #setAttr1(Attribute)
	 * @see org.eclipse.efbt.cocalimo.logical_transformations.model.logical_transformations.Logical_transformationsPackage#getDataConstraint_Attr1()
	 * @model
	 * @generated
	 */
	Attribute getAttr1();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.logical_transformations.model.logical_transformations.DataConstraint#getAttr1 <em>Attr1</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Attr1</em>' reference.
	 * @see #getAttr1()
	 * @generated
	 */
	void setAttr1(Attribute value);

	/**
	 * Returns the value of the '<em><b>Attr2</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Attr2</em>' reference.
	 * @see #setAttr2(Attribute)
	 * @see org.eclipse.efbt.cocalimo.logical_transformations.model.logical_transformations.Logical_transformationsPackage#getDataConstraint_Attr2()
	 * @model
	 * @generated
	 */
	Attribute getAttr2();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.logical_transformations.model.logical_transformations.DataConstraint#getAttr2 <em>Attr2</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Attr2</em>' reference.
	 * @see #getAttr2()
	 * @generated
	 */
	void setAttr2(Attribute value);

	/**
	 * Returns the value of the '<em><b>Comparison</b></em>' attribute.
	 * The literals are from the enumeration {@link org.eclipse.efbt.cocalimo.logical_transformations.model.logical_transformations.AttrComparison}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Comparison</em>' attribute.
	 * @see org.eclipse.efbt.cocalimo.logical_transformations.model.logical_transformations.AttrComparison
	 * @see #setComparison(AttrComparison)
	 * @see org.eclipse.efbt.cocalimo.logical_transformations.model.logical_transformations.Logical_transformationsPackage#getDataConstraint_Comparison()
	 * @model
	 * @generated
	 */
	AttrComparison getComparison();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.cocalimo.logical_transformations.model.logical_transformations.DataConstraint#getComparison <em>Comparison</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Comparison</em>' attribute.
	 * @see org.eclipse.efbt.cocalimo.logical_transformations.model.logical_transformations.AttrComparison
	 * @see #getComparison()
	 * @generated
	 */
	void setComparison(AttrComparison value);

} // DataConstraint
