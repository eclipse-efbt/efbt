/**
 */
package testmodel;

import java.math.BigDecimal;
import java.math.BigInteger;

import java.util.Date;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Attribute Test Container</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link testmodel.AttributeTestContainer#getName <em>Name</em>}</li>
 *   <li>{@link testmodel.AttributeTestContainer#getOneInt <em>One Int</em>}</li>
 *   <li>{@link testmodel.AttributeTestContainer#getOneInteger <em>One Integer</em>}</li>
 *   <li>{@link testmodel.AttributeTestContainer#getManyInt <em>Many Int</em>}</li>
 *   <li>{@link testmodel.AttributeTestContainer#isOneBool <em>One Bool</em>}</li>
 *   <li>{@link testmodel.AttributeTestContainer#getManyBool <em>Many Bool</em>}</li>
 *   <li>{@link testmodel.AttributeTestContainer#getOneDouble <em>One Double</em>}</li>
 *   <li>{@link testmodel.AttributeTestContainer#getManyDouble <em>Many Double</em>}</li>
 *   <li>{@link testmodel.AttributeTestContainer#getOneBigDecimal <em>One Big Decimal</em>}</li>
 *   <li>{@link testmodel.AttributeTestContainer#getManyBigDecimal <em>Many Big Decimal</em>}</li>
 *   <li>{@link testmodel.AttributeTestContainer#getOneBigInteger <em>One Big Integer</em>}</li>
 *   <li>{@link testmodel.AttributeTestContainer#getManyBigInteger <em>Many Big Integer</em>}</li>
 *   <li>{@link testmodel.AttributeTestContainer#getOneEnum <em>One Enum</em>}</li>
 *   <li>{@link testmodel.AttributeTestContainer#getManyEnums <em>Many Enums</em>}</li>
 *   <li>{@link testmodel.AttributeTestContainer#getOneShort <em>One Short</em>}</li>
 *   <li>{@link testmodel.AttributeTestContainer#getManyShorts <em>Many Shorts</em>}</li>
 *   <li>{@link testmodel.AttributeTestContainer#getOneDate <em>One Date</em>}</li>
 *   <li>{@link testmodel.AttributeTestContainer#getManyDates <em>Many Dates</em>}</li>
 *   <li>{@link testmodel.AttributeTestContainer#getOneString <em>One String</em>}</li>
 *   <li>{@link testmodel.AttributeTestContainer#getManyString <em>Many String</em>}</li>
 * </ul>
 *
 * @see testmodel.TestmodelPackage#getAttributeTestContainer()
 * @model
 * @generated
 */
public interface AttributeTestContainer extends EObject {
	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Name</em>' attribute isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see testmodel.TestmodelPackage#getAttributeTestContainer_Name()
	 * @model
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link testmodel.AttributeTestContainer#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

	/**
	 * Returns the value of the '<em><b>One Int</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>One Int</em>' attribute isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>One Int</em>' attribute.
	 * @see #setOneInt(int)
	 * @see testmodel.TestmodelPackage#getAttributeTestContainer_OneInt()
	 * @model
	 * @generated
	 */
	int getOneInt();

	/**
	 * Sets the value of the '{@link testmodel.AttributeTestContainer#getOneInt <em>One Int</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>One Int</em>' attribute.
	 * @see #getOneInt()
	 * @generated
	 */
	void setOneInt(int value);

	/**
	 * Returns the value of the '<em><b>One Integer</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>One Integer</em>' attribute isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>One Integer</em>' attribute.
	 * @see #setOneInteger(Integer)
	 * @see testmodel.TestmodelPackage#getAttributeTestContainer_OneInteger()
	 * @model
	 * @generated
	 */
	Integer getOneInteger();

	/**
	 * Sets the value of the '{@link testmodel.AttributeTestContainer#getOneInteger <em>One Integer</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>One Integer</em>' attribute.
	 * @see #getOneInteger()
	 * @generated
	 */
	void setOneInteger(Integer value);

	/**
	 * Returns the value of the '<em><b>Many Int</b></em>' attribute list.
	 * The list contents are of type {@link java.lang.Integer}.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Many Int</em>' attribute list isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Many Int</em>' attribute list.
	 * @see testmodel.TestmodelPackage#getAttributeTestContainer_ManyInt()
	 * @model
	 * @generated
	 */
	EList<Integer> getManyInt();

	/**
	 * Returns the value of the '<em><b>One Bool</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>One Bool</em>' attribute isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>One Bool</em>' attribute.
	 * @see #setOneBool(boolean)
	 * @see testmodel.TestmodelPackage#getAttributeTestContainer_OneBool()
	 * @model
	 * @generated
	 */
	boolean isOneBool();

	/**
	 * Sets the value of the '{@link testmodel.AttributeTestContainer#isOneBool <em>One Bool</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>One Bool</em>' attribute.
	 * @see #isOneBool()
	 * @generated
	 */
	void setOneBool(boolean value);

	/**
	 * Returns the value of the '<em><b>Many Bool</b></em>' attribute list.
	 * The list contents are of type {@link java.lang.Boolean}.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Many Bool</em>' attribute list isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Many Bool</em>' attribute list.
	 * @see testmodel.TestmodelPackage#getAttributeTestContainer_ManyBool()
	 * @model unique="false"
	 * @generated
	 */
	EList<Boolean> getManyBool();

	/**
	 * Returns the value of the '<em><b>One Double</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>One Double</em>' attribute isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>One Double</em>' attribute.
	 * @see #setOneDouble(double)
	 * @see testmodel.TestmodelPackage#getAttributeTestContainer_OneDouble()
	 * @model
	 * @generated
	 */
	double getOneDouble();

	/**
	 * Sets the value of the '{@link testmodel.AttributeTestContainer#getOneDouble <em>One Double</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>One Double</em>' attribute.
	 * @see #getOneDouble()
	 * @generated
	 */
	void setOneDouble(double value);

	/**
	 * Returns the value of the '<em><b>Many Double</b></em>' attribute list.
	 * The list contents are of type {@link java.lang.Double}.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Many Double</em>' attribute list isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Many Double</em>' attribute list.
	 * @see testmodel.TestmodelPackage#getAttributeTestContainer_ManyDouble()
	 * @model
	 * @generated
	 */
	EList<Double> getManyDouble();

	/**
	 * Returns the value of the '<em><b>One Big Decimal</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>One Big Decimal</em>' attribute isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>One Big Decimal</em>' attribute.
	 * @see #setOneBigDecimal(BigDecimal)
	 * @see testmodel.TestmodelPackage#getAttributeTestContainer_OneBigDecimal()
	 * @model
	 * @generated
	 */
	BigDecimal getOneBigDecimal();

	/**
	 * Sets the value of the '{@link testmodel.AttributeTestContainer#getOneBigDecimal <em>One Big Decimal</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>One Big Decimal</em>' attribute.
	 * @see #getOneBigDecimal()
	 * @generated
	 */
	void setOneBigDecimal(BigDecimal value);

	/**
	 * Returns the value of the '<em><b>Many Big Decimal</b></em>' attribute list.
	 * The list contents are of type {@link java.math.BigDecimal}.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Many Big Decimal</em>' attribute list isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Many Big Decimal</em>' attribute list.
	 * @see testmodel.TestmodelPackage#getAttributeTestContainer_ManyBigDecimal()
	 * @model
	 * @generated
	 */
	EList<BigDecimal> getManyBigDecimal();

	/**
	 * Returns the value of the '<em><b>One Big Integer</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>One Big Integer</em>' attribute isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>One Big Integer</em>' attribute.
	 * @see #setOneBigInteger(BigInteger)
	 * @see testmodel.TestmodelPackage#getAttributeTestContainer_OneBigInteger()
	 * @model
	 * @generated
	 */
	BigInteger getOneBigInteger();

	/**
	 * Sets the value of the '{@link testmodel.AttributeTestContainer#getOneBigInteger <em>One Big Integer</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>One Big Integer</em>' attribute.
	 * @see #getOneBigInteger()
	 * @generated
	 */
	void setOneBigInteger(BigInteger value);

	/**
	 * Returns the value of the '<em><b>Many Big Integer</b></em>' attribute list.
	 * The list contents are of type {@link java.math.BigInteger}.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Many Big Integer</em>' attribute list isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Many Big Integer</em>' attribute list.
	 * @see testmodel.TestmodelPackage#getAttributeTestContainer_ManyBigInteger()
	 * @model
	 * @generated
	 */
	EList<BigInteger> getManyBigInteger();

	/**
	 * Returns the value of the '<em><b>One Enum</b></em>' attribute.
	 * The literals are from the enumeration {@link testmodel.SampleEnum}.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>One Enum</em>' attribute isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>One Enum</em>' attribute.
	 * @see testmodel.SampleEnum
	 * @see #setOneEnum(SampleEnum)
	 * @see testmodel.TestmodelPackage#getAttributeTestContainer_OneEnum()
	 * @model
	 * @generated
	 */
	SampleEnum getOneEnum();

	/**
	 * Sets the value of the '{@link testmodel.AttributeTestContainer#getOneEnum <em>One Enum</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>One Enum</em>' attribute.
	 * @see testmodel.SampleEnum
	 * @see #getOneEnum()
	 * @generated
	 */
	void setOneEnum(SampleEnum value);

	/**
	 * Returns the value of the '<em><b>Many Enums</b></em>' attribute list.
	 * The list contents are of type {@link testmodel.SampleEnum}.
	 * The literals are from the enumeration {@link testmodel.SampleEnum}.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Many Enums</em>' attribute list isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Many Enums</em>' attribute list.
	 * @see testmodel.SampleEnum
	 * @see testmodel.TestmodelPackage#getAttributeTestContainer_ManyEnums()
	 * @model
	 * @generated
	 */
	EList<SampleEnum> getManyEnums();

	/**
	 * Returns the value of the '<em><b>One Short</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>One Short</em>' attribute isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>One Short</em>' attribute.
	 * @see #setOneShort(short)
	 * @see testmodel.TestmodelPackage#getAttributeTestContainer_OneShort()
	 * @model
	 * @generated
	 */
	short getOneShort();

	/**
	 * Sets the value of the '{@link testmodel.AttributeTestContainer#getOneShort <em>One Short</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>One Short</em>' attribute.
	 * @see #getOneShort()
	 * @generated
	 */
	void setOneShort(short value);

	/**
	 * Returns the value of the '<em><b>Many Shorts</b></em>' attribute list.
	 * The list contents are of type {@link java.lang.Short}.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Many Shorts</em>' attribute list isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Many Shorts</em>' attribute list.
	 * @see testmodel.TestmodelPackage#getAttributeTestContainer_ManyShorts()
	 * @model
	 * @generated
	 */
	EList<Short> getManyShorts();

	/**
	 * Returns the value of the '<em><b>One Date</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>One Date</em>' attribute isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>One Date</em>' attribute.
	 * @see #setOneDate(Date)
	 * @see testmodel.TestmodelPackage#getAttributeTestContainer_OneDate()
	 * @model
	 * @generated
	 */
	Date getOneDate();

	/**
	 * Sets the value of the '{@link testmodel.AttributeTestContainer#getOneDate <em>One Date</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>One Date</em>' attribute.
	 * @see #getOneDate()
	 * @generated
	 */
	void setOneDate(Date value);

	/**
	 * Returns the value of the '<em><b>Many Dates</b></em>' attribute list.
	 * The list contents are of type {@link java.util.Date}.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Many Dates</em>' attribute list isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Many Dates</em>' attribute list.
	 * @see testmodel.TestmodelPackage#getAttributeTestContainer_ManyDates()
	 * @model
	 * @generated
	 */
	EList<Date> getManyDates();

	/**
	 * Returns the value of the '<em><b>One String</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>One String</em>' attribute isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>One String</em>' attribute.
	 * @see #setOneString(String)
	 * @see testmodel.TestmodelPackage#getAttributeTestContainer_OneString()
	 * @model
	 * @generated
	 */
	String getOneString();

	/**
	 * Sets the value of the '{@link testmodel.AttributeTestContainer#getOneString <em>One String</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>One String</em>' attribute.
	 * @see #getOneString()
	 * @generated
	 */
	void setOneString(String value);

	/**
	 * Returns the value of the '<em><b>Many String</b></em>' attribute list.
	 * The list contents are of type {@link java.lang.String}.
	 * <!-- begin-user-doc -->
	 * <p>
	 * If the meaning of the '<em>Many String</em>' attribute list isn't clear,
	 * there really should be more of a description here...
	 * </p>
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Many String</em>' attribute list.
	 * @see testmodel.TestmodelPackage#getAttributeTestContainer_ManyString()
	 * @model
	 * @generated
	 */
	EList<String> getManyString();

} // AttributeTestContainer
