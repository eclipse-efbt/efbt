/**
 */
package testmodel;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Attribute Sample</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link testmodel.AttributeSample#getSingleIntOptional <em>Single Int Optional</em>}</li>
 *   <li>{@link testmodel.AttributeSample#getListIntRequired <em>List Int Required</em>}</li>
 *   <li>{@link testmodel.AttributeSample#getBigDecimal <em>Big Decimal</em>}</li>
 *   <li>{@link testmodel.AttributeSample#getBigInteger <em>Big Integer</em>}</li>
 *   <li>{@link testmodel.AttributeSample#getDouble <em>Double</em>}</li>
 * </ul>
 *
 * @see testmodel.TestmodelPackage#getAttributeSample()
 * @model
 * @generated
 */
public interface AttributeSample extends EObject {
	/**
	 * Returns the value of the '<em><b>Single Int Optional</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Single Int Optional</em>' attribute isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Single Int Optional</em>' attribute.
	 * @see #setSingleIntOptional(int)
	 * @see testmodel.TestmodelPackage#getAttributeSample_SingleIntOptional()
	 * @model
	 * @generated
	 */
	int getSingleIntOptional();

	/**
	 * Sets the value of the '{@link testmodel.AttributeSample#getSingleIntOptional <em>Single Int Optional</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Single Int Optional</em>' attribute.
	 * @see #getSingleIntOptional()
	 * @generated
	 */
	void setSingleIntOptional(int value);

	/**
	 * Returns the value of the '<em><b>List Int Required</b></em>' attribute list.
	 * The list contents are of type {@link java.lang.Integer}.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>List Int Required</em>' attribute list isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>List Int Required</em>' attribute list.
	 * @see testmodel.TestmodelPackage#getAttributeSample_ListIntRequired()
	 * @model required="true"
	 * @generated
	 */
	EList<Integer> getListIntRequired();

	/**
	 * Returns the value of the '<em><b>Big Decimal</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Big Decimal</em>' attribute isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Big Decimal</em>' attribute.
	 * @see #setBigDecimal(BigDecimal)
	 * @see testmodel.TestmodelPackage#getAttributeSample_BigDecimal()
	 * @model
	 * @generated
	 */
	BigDecimal getBigDecimal();

	/**
	 * Sets the value of the '{@link testmodel.AttributeSample#getBigDecimal <em>Big Decimal</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Big Decimal</em>' attribute.
	 * @see #getBigDecimal()
	 * @generated
	 */
	void setBigDecimal(BigDecimal value);

	/**
	 * Returns the value of the '<em><b>Big Integer</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Big Integer</em>' attribute isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Big Integer</em>' attribute.
	 * @see #setBigInteger(BigInteger)
	 * @see testmodel.TestmodelPackage#getAttributeSample_BigInteger()
	 * @model
	 * @generated
	 */
	BigInteger getBigInteger();

	/**
	 * Sets the value of the '{@link testmodel.AttributeSample#getBigInteger <em>Big Integer</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Big Integer</em>' attribute.
	 * @see #getBigInteger()
	 * @generated
	 */
	void setBigInteger(BigInteger value);

	/**
	 * Returns the value of the '<em><b>Double</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Double</em>' attribute isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Double</em>' attribute.
	 * @see #setDouble(double)
	 * @see testmodel.TestmodelPackage#getAttributeSample_Double()
	 * @model
	 * @generated
	 */
	double getDouble();

	/**
	 * Sets the value of the '{@link testmodel.AttributeSample#getDouble <em>Double</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Double</em>' attribute.
	 * @see #getDouble()
	 * @generated
	 */
	void setDouble(double value);

} // AttributeSample
