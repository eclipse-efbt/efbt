/**
 */
package testmodel.impl;

import java.math.BigDecimal;
import java.math.BigInteger;

import java.util.Collection;
import java.util.Date;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.EObjectImpl;

import org.eclipse.emf.ecore.util.EDataTypeEList;
import org.eclipse.emf.ecore.util.EDataTypeUniqueEList;

import testmodel.AttributeTestContainer;
import testmodel.SampleEnum;
import testmodel.TestmodelPackage;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Attribute Test Container</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link testmodel.impl.AttributeTestContainerImpl#getName <em>Name</em>}</li>
 *   <li>{@link testmodel.impl.AttributeTestContainerImpl#getOneInt <em>One Int</em>}</li>
 *   <li>{@link testmodel.impl.AttributeTestContainerImpl#getOneInteger <em>One Integer</em>}</li>
 *   <li>{@link testmodel.impl.AttributeTestContainerImpl#getManyInt <em>Many Int</em>}</li>
 *   <li>{@link testmodel.impl.AttributeTestContainerImpl#isOneBool <em>One Bool</em>}</li>
 *   <li>{@link testmodel.impl.AttributeTestContainerImpl#getManyBool <em>Many Bool</em>}</li>
 *   <li>{@link testmodel.impl.AttributeTestContainerImpl#getOneDouble <em>One Double</em>}</li>
 *   <li>{@link testmodel.impl.AttributeTestContainerImpl#getManyDouble <em>Many Double</em>}</li>
 *   <li>{@link testmodel.impl.AttributeTestContainerImpl#getOneBigDecimal <em>One Big Decimal</em>}</li>
 *   <li>{@link testmodel.impl.AttributeTestContainerImpl#getManyBigDecimal <em>Many Big Decimal</em>}</li>
 *   <li>{@link testmodel.impl.AttributeTestContainerImpl#getOneBigInteger <em>One Big Integer</em>}</li>
 *   <li>{@link testmodel.impl.AttributeTestContainerImpl#getManyBigInteger <em>Many Big Integer</em>}</li>
 *   <li>{@link testmodel.impl.AttributeTestContainerImpl#getOneEnum <em>One Enum</em>}</li>
 *   <li>{@link testmodel.impl.AttributeTestContainerImpl#getManyEnums <em>Many Enums</em>}</li>
 *   <li>{@link testmodel.impl.AttributeTestContainerImpl#getOneShort <em>One Short</em>}</li>
 *   <li>{@link testmodel.impl.AttributeTestContainerImpl#getManyShorts <em>Many Shorts</em>}</li>
 *   <li>{@link testmodel.impl.AttributeTestContainerImpl#getOneDate <em>One Date</em>}</li>
 *   <li>{@link testmodel.impl.AttributeTestContainerImpl#getManyDates <em>Many Dates</em>}</li>
 *   <li>{@link testmodel.impl.AttributeTestContainerImpl#getOneString <em>One String</em>}</li>
 *   <li>{@link testmodel.impl.AttributeTestContainerImpl#getManyString <em>Many String</em>}</li>
 * </ul>
 *
 * @generated
 */
public class AttributeTestContainerImpl extends EObjectImpl implements AttributeTestContainer {
	/**
	 * The default value of the '{@link #getName() <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getName()
	 * @generated
	 * @ordered
	 */
	protected static final String NAME_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getName() <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getName()
	 * @generated
	 * @ordered
	 */
	protected String name = NAME_EDEFAULT;

	/**
	 * The default value of the '{@link #getOneInt() <em>One Int</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getOneInt()
	 * @generated
	 * @ordered
	 */
	protected static final int ONE_INT_EDEFAULT = 0;

	/**
	 * The cached value of the '{@link #getOneInt() <em>One Int</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getOneInt()
	 * @generated
	 * @ordered
	 */
	protected int oneInt = ONE_INT_EDEFAULT;

	/**
	 * The default value of the '{@link #getOneInteger() <em>One Integer</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getOneInteger()
	 * @generated
	 * @ordered
	 */
	protected static final Integer ONE_INTEGER_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getOneInteger() <em>One Integer</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getOneInteger()
	 * @generated
	 * @ordered
	 */
	protected Integer oneInteger = ONE_INTEGER_EDEFAULT;

	/**
	 * The cached value of the '{@link #getManyInt() <em>Many Int</em>}' attribute list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getManyInt()
	 * @generated
	 * @ordered
	 */
	protected EList<Integer> manyInt;

	/**
	 * The default value of the '{@link #isOneBool() <em>One Bool</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isOneBool()
	 * @generated
	 * @ordered
	 */
	protected static final boolean ONE_BOOL_EDEFAULT = false;

	/**
	 * The cached value of the '{@link #isOneBool() <em>One Bool</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #isOneBool()
	 * @generated
	 * @ordered
	 */
	protected boolean oneBool = ONE_BOOL_EDEFAULT;

	/**
	 * The cached value of the '{@link #getManyBool() <em>Many Bool</em>}' attribute list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getManyBool()
	 * @generated
	 * @ordered
	 */
	protected EList<Boolean> manyBool;

	/**
	 * The default value of the '{@link #getOneDouble() <em>One Double</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getOneDouble()
	 * @generated
	 * @ordered
	 */
	protected static final double ONE_DOUBLE_EDEFAULT = 0.0;

	/**
	 * The cached value of the '{@link #getOneDouble() <em>One Double</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getOneDouble()
	 * @generated
	 * @ordered
	 */
	protected double oneDouble = ONE_DOUBLE_EDEFAULT;

	/**
	 * The cached value of the '{@link #getManyDouble() <em>Many Double</em>}' attribute list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getManyDouble()
	 * @generated
	 * @ordered
	 */
	protected EList<Double> manyDouble;

	/**
	 * The default value of the '{@link #getOneBigDecimal() <em>One Big Decimal</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getOneBigDecimal()
	 * @generated
	 * @ordered
	 */
	protected static final BigDecimal ONE_BIG_DECIMAL_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getOneBigDecimal() <em>One Big Decimal</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getOneBigDecimal()
	 * @generated
	 * @ordered
	 */
	protected BigDecimal oneBigDecimal = ONE_BIG_DECIMAL_EDEFAULT;

	/**
	 * The cached value of the '{@link #getManyBigDecimal() <em>Many Big Decimal</em>}' attribute list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getManyBigDecimal()
	 * @generated
	 * @ordered
	 */
	protected EList<BigDecimal> manyBigDecimal;

	/**
	 * The default value of the '{@link #getOneBigInteger() <em>One Big Integer</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getOneBigInteger()
	 * @generated
	 * @ordered
	 */
	protected static final BigInteger ONE_BIG_INTEGER_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getOneBigInteger() <em>One Big Integer</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getOneBigInteger()
	 * @generated
	 * @ordered
	 */
	protected BigInteger oneBigInteger = ONE_BIG_INTEGER_EDEFAULT;

	/**
	 * The cached value of the '{@link #getManyBigInteger() <em>Many Big Integer</em>}' attribute list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getManyBigInteger()
	 * @generated
	 * @ordered
	 */
	protected EList<BigInteger> manyBigInteger;

	/**
	 * The default value of the '{@link #getOneEnum() <em>One Enum</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getOneEnum()
	 * @generated
	 * @ordered
	 */
	protected static final SampleEnum ONE_ENUM_EDEFAULT = SampleEnum.SAMPLE;

	/**
	 * The cached value of the '{@link #getOneEnum() <em>One Enum</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getOneEnum()
	 * @generated
	 * @ordered
	 */
	protected SampleEnum oneEnum = ONE_ENUM_EDEFAULT;

	/**
	 * The cached value of the '{@link #getManyEnums() <em>Many Enums</em>}' attribute list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getManyEnums()
	 * @generated
	 * @ordered
	 */
	protected EList<SampleEnum> manyEnums;

	/**
	 * The default value of the '{@link #getOneShort() <em>One Short</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getOneShort()
	 * @generated
	 * @ordered
	 */
	protected static final short ONE_SHORT_EDEFAULT = 0;

	/**
	 * The cached value of the '{@link #getOneShort() <em>One Short</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getOneShort()
	 * @generated
	 * @ordered
	 */
	protected short oneShort = ONE_SHORT_EDEFAULT;

	/**
	 * The cached value of the '{@link #getManyShorts() <em>Many Shorts</em>}' attribute list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getManyShorts()
	 * @generated
	 * @ordered
	 */
	protected EList<Short> manyShorts;

	/**
	 * The default value of the '{@link #getOneDate() <em>One Date</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getOneDate()
	 * @generated
	 * @ordered
	 */
	protected static final Date ONE_DATE_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getOneDate() <em>One Date</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getOneDate()
	 * @generated
	 * @ordered
	 */
	protected Date oneDate = ONE_DATE_EDEFAULT;

	/**
	 * The cached value of the '{@link #getManyDates() <em>Many Dates</em>}' attribute list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getManyDates()
	 * @generated
	 * @ordered
	 */
	protected EList<Date> manyDates;

	/**
	 * The default value of the '{@link #getOneString() <em>One String</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getOneString()
	 * @generated
	 * @ordered
	 */
	protected static final String ONE_STRING_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getOneString() <em>One String</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getOneString()
	 * @generated
	 * @ordered
	 */
	protected String oneString = ONE_STRING_EDEFAULT;

	/**
	 * The cached value of the '{@link #getManyString() <em>Many String</em>}' attribute list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getManyString()
	 * @generated
	 * @ordered
	 */
	protected EList<String> manyString;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected AttributeTestContainerImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return TestmodelPackage.Literals.ATTRIBUTE_TEST_CONTAINER;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getName() {
		return name;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setName(String newName) {
		String oldName = name;
		name = newName;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__NAME, oldName, name));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public int getOneInt() {
		return oneInt;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setOneInt(int newOneInt) {
		int oldOneInt = oneInt;
		oneInt = newOneInt;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__ONE_INT, oldOneInt, oneInt));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Integer getOneInteger() {
		return oneInteger;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setOneInteger(Integer newOneInteger) {
		Integer oldOneInteger = oneInteger;
		oneInteger = newOneInteger;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__ONE_INTEGER, oldOneInteger, oneInteger));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<Integer> getManyInt() {
		if (manyInt == null) {
			manyInt = new EDataTypeUniqueEList<Integer>(Integer.class, this, TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__MANY_INT);
		}
		return manyInt;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public boolean isOneBool() {
		return oneBool;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setOneBool(boolean newOneBool) {
		boolean oldOneBool = oneBool;
		oneBool = newOneBool;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__ONE_BOOL, oldOneBool, oneBool));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<Boolean> getManyBool() {
		if (manyBool == null) {
			manyBool = new EDataTypeEList<Boolean>(Boolean.class, this, TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__MANY_BOOL);
		}
		return manyBool;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public double getOneDouble() {
		return oneDouble;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setOneDouble(double newOneDouble) {
		double oldOneDouble = oneDouble;
		oneDouble = newOneDouble;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__ONE_DOUBLE, oldOneDouble, oneDouble));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<Double> getManyDouble() {
		if (manyDouble == null) {
			manyDouble = new EDataTypeUniqueEList<Double>(Double.class, this, TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__MANY_DOUBLE);
		}
		return manyDouble;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public BigDecimal getOneBigDecimal() {
		return oneBigDecimal;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setOneBigDecimal(BigDecimal newOneBigDecimal) {
		BigDecimal oldOneBigDecimal = oneBigDecimal;
		oneBigDecimal = newOneBigDecimal;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__ONE_BIG_DECIMAL, oldOneBigDecimal, oneBigDecimal));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<BigDecimal> getManyBigDecimal() {
		if (manyBigDecimal == null) {
			manyBigDecimal = new EDataTypeUniqueEList<BigDecimal>(BigDecimal.class, this, TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__MANY_BIG_DECIMAL);
		}
		return manyBigDecimal;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public BigInteger getOneBigInteger() {
		return oneBigInteger;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setOneBigInteger(BigInteger newOneBigInteger) {
		BigInteger oldOneBigInteger = oneBigInteger;
		oneBigInteger = newOneBigInteger;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__ONE_BIG_INTEGER, oldOneBigInteger, oneBigInteger));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<BigInteger> getManyBigInteger() {
		if (manyBigInteger == null) {
			manyBigInteger = new EDataTypeUniqueEList<BigInteger>(BigInteger.class, this, TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__MANY_BIG_INTEGER);
		}
		return manyBigInteger;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SampleEnum getOneEnum() {
		return oneEnum;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setOneEnum(SampleEnum newOneEnum) {
		SampleEnum oldOneEnum = oneEnum;
		oneEnum = newOneEnum == null ? ONE_ENUM_EDEFAULT : newOneEnum;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__ONE_ENUM, oldOneEnum, oneEnum));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<SampleEnum> getManyEnums() {
		if (manyEnums == null) {
			manyEnums = new EDataTypeUniqueEList<SampleEnum>(SampleEnum.class, this, TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__MANY_ENUMS);
		}
		return manyEnums;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public short getOneShort() {
		return oneShort;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setOneShort(short newOneShort) {
		short oldOneShort = oneShort;
		oneShort = newOneShort;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__ONE_SHORT, oldOneShort, oneShort));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<Short> getManyShorts() {
		if (manyShorts == null) {
			manyShorts = new EDataTypeUniqueEList<Short>(Short.class, this, TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__MANY_SHORTS);
		}
		return manyShorts;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Date getOneDate() {
		return oneDate;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setOneDate(Date newOneDate) {
		Date oldOneDate = oneDate;
		oneDate = newOneDate;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__ONE_DATE, oldOneDate, oneDate));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<Date> getManyDates() {
		if (manyDates == null) {
			manyDates = new EDataTypeUniqueEList<Date>(Date.class, this, TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__MANY_DATES);
		}
		return manyDates;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getOneString() {
		return oneString;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setOneString(String newOneString) {
		String oldOneString = oneString;
		oneString = newOneString;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__ONE_STRING, oldOneString, oneString));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<String> getManyString() {
		if (manyString == null) {
			manyString = new EDataTypeUniqueEList<String>(String.class, this, TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__MANY_STRING);
		}
		return manyString;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__NAME:
				return getName();
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__ONE_INT:
				return getOneInt();
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__ONE_INTEGER:
				return getOneInteger();
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__MANY_INT:
				return getManyInt();
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__ONE_BOOL:
				return isOneBool();
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__MANY_BOOL:
				return getManyBool();
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__ONE_DOUBLE:
				return getOneDouble();
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__MANY_DOUBLE:
				return getManyDouble();
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__ONE_BIG_DECIMAL:
				return getOneBigDecimal();
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__MANY_BIG_DECIMAL:
				return getManyBigDecimal();
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__ONE_BIG_INTEGER:
				return getOneBigInteger();
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__MANY_BIG_INTEGER:
				return getManyBigInteger();
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__ONE_ENUM:
				return getOneEnum();
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__MANY_ENUMS:
				return getManyEnums();
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__ONE_SHORT:
				return getOneShort();
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__MANY_SHORTS:
				return getManyShorts();
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__ONE_DATE:
				return getOneDate();
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__MANY_DATES:
				return getManyDates();
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__ONE_STRING:
				return getOneString();
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__MANY_STRING:
				return getManyString();
		}
		return super.eGet(featureID, resolve, coreType);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void eSet(int featureID, Object newValue) {
		switch (featureID) {
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__NAME:
				setName((String)newValue);
				return;
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__ONE_INT:
				setOneInt((Integer)newValue);
				return;
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__ONE_INTEGER:
				setOneInteger((Integer)newValue);
				return;
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__MANY_INT:
				getManyInt().clear();
				getManyInt().addAll((Collection<? extends Integer>)newValue);
				return;
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__ONE_BOOL:
				setOneBool((Boolean)newValue);
				return;
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__MANY_BOOL:
				getManyBool().clear();
				getManyBool().addAll((Collection<? extends Boolean>)newValue);
				return;
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__ONE_DOUBLE:
				setOneDouble((Double)newValue);
				return;
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__MANY_DOUBLE:
				getManyDouble().clear();
				getManyDouble().addAll((Collection<? extends Double>)newValue);
				return;
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__ONE_BIG_DECIMAL:
				setOneBigDecimal((BigDecimal)newValue);
				return;
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__MANY_BIG_DECIMAL:
				getManyBigDecimal().clear();
				getManyBigDecimal().addAll((Collection<? extends BigDecimal>)newValue);
				return;
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__ONE_BIG_INTEGER:
				setOneBigInteger((BigInteger)newValue);
				return;
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__MANY_BIG_INTEGER:
				getManyBigInteger().clear();
				getManyBigInteger().addAll((Collection<? extends BigInteger>)newValue);
				return;
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__ONE_ENUM:
				setOneEnum((SampleEnum)newValue);
				return;
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__MANY_ENUMS:
				getManyEnums().clear();
				getManyEnums().addAll((Collection<? extends SampleEnum>)newValue);
				return;
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__ONE_SHORT:
				setOneShort((Short)newValue);
				return;
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__MANY_SHORTS:
				getManyShorts().clear();
				getManyShorts().addAll((Collection<? extends Short>)newValue);
				return;
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__ONE_DATE:
				setOneDate((Date)newValue);
				return;
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__MANY_DATES:
				getManyDates().clear();
				getManyDates().addAll((Collection<? extends Date>)newValue);
				return;
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__ONE_STRING:
				setOneString((String)newValue);
				return;
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__MANY_STRING:
				getManyString().clear();
				getManyString().addAll((Collection<? extends String>)newValue);
				return;
		}
		super.eSet(featureID, newValue);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void eUnset(int featureID) {
		switch (featureID) {
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__NAME:
				setName(NAME_EDEFAULT);
				return;
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__ONE_INT:
				setOneInt(ONE_INT_EDEFAULT);
				return;
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__ONE_INTEGER:
				setOneInteger(ONE_INTEGER_EDEFAULT);
				return;
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__MANY_INT:
				getManyInt().clear();
				return;
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__ONE_BOOL:
				setOneBool(ONE_BOOL_EDEFAULT);
				return;
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__MANY_BOOL:
				getManyBool().clear();
				return;
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__ONE_DOUBLE:
				setOneDouble(ONE_DOUBLE_EDEFAULT);
				return;
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__MANY_DOUBLE:
				getManyDouble().clear();
				return;
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__ONE_BIG_DECIMAL:
				setOneBigDecimal(ONE_BIG_DECIMAL_EDEFAULT);
				return;
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__MANY_BIG_DECIMAL:
				getManyBigDecimal().clear();
				return;
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__ONE_BIG_INTEGER:
				setOneBigInteger(ONE_BIG_INTEGER_EDEFAULT);
				return;
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__MANY_BIG_INTEGER:
				getManyBigInteger().clear();
				return;
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__ONE_ENUM:
				setOneEnum(ONE_ENUM_EDEFAULT);
				return;
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__MANY_ENUMS:
				getManyEnums().clear();
				return;
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__ONE_SHORT:
				setOneShort(ONE_SHORT_EDEFAULT);
				return;
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__MANY_SHORTS:
				getManyShorts().clear();
				return;
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__ONE_DATE:
				setOneDate(ONE_DATE_EDEFAULT);
				return;
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__MANY_DATES:
				getManyDates().clear();
				return;
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__ONE_STRING:
				setOneString(ONE_STRING_EDEFAULT);
				return;
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__MANY_STRING:
				getManyString().clear();
				return;
		}
		super.eUnset(featureID);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public boolean eIsSet(int featureID) {
		switch (featureID) {
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__NAME:
				return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__ONE_INT:
				return oneInt != ONE_INT_EDEFAULT;
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__ONE_INTEGER:
				return ONE_INTEGER_EDEFAULT == null ? oneInteger != null : !ONE_INTEGER_EDEFAULT.equals(oneInteger);
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__MANY_INT:
				return manyInt != null && !manyInt.isEmpty();
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__ONE_BOOL:
				return oneBool != ONE_BOOL_EDEFAULT;
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__MANY_BOOL:
				return manyBool != null && !manyBool.isEmpty();
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__ONE_DOUBLE:
				return oneDouble != ONE_DOUBLE_EDEFAULT;
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__MANY_DOUBLE:
				return manyDouble != null && !manyDouble.isEmpty();
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__ONE_BIG_DECIMAL:
				return ONE_BIG_DECIMAL_EDEFAULT == null ? oneBigDecimal != null : !ONE_BIG_DECIMAL_EDEFAULT.equals(oneBigDecimal);
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__MANY_BIG_DECIMAL:
				return manyBigDecimal != null && !manyBigDecimal.isEmpty();
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__ONE_BIG_INTEGER:
				return ONE_BIG_INTEGER_EDEFAULT == null ? oneBigInteger != null : !ONE_BIG_INTEGER_EDEFAULT.equals(oneBigInteger);
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__MANY_BIG_INTEGER:
				return manyBigInteger != null && !manyBigInteger.isEmpty();
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__ONE_ENUM:
				return oneEnum != ONE_ENUM_EDEFAULT;
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__MANY_ENUMS:
				return manyEnums != null && !manyEnums.isEmpty();
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__ONE_SHORT:
				return oneShort != ONE_SHORT_EDEFAULT;
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__MANY_SHORTS:
				return manyShorts != null && !manyShorts.isEmpty();
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__ONE_DATE:
				return ONE_DATE_EDEFAULT == null ? oneDate != null : !ONE_DATE_EDEFAULT.equals(oneDate);
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__MANY_DATES:
				return manyDates != null && !manyDates.isEmpty();
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__ONE_STRING:
				return ONE_STRING_EDEFAULT == null ? oneString != null : !ONE_STRING_EDEFAULT.equals(oneString);
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER__MANY_STRING:
				return manyString != null && !manyString.isEmpty();
		}
		return super.eIsSet(featureID);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String toString() {
		if (eIsProxy()) return super.toString();

		StringBuffer result = new StringBuffer(super.toString());
		result.append(" (name: ");
		result.append(name);
		result.append(", oneInt: ");
		result.append(oneInt);
		result.append(", oneInteger: ");
		result.append(oneInteger);
		result.append(", manyInt: ");
		result.append(manyInt);
		result.append(", oneBool: ");
		result.append(oneBool);
		result.append(", manyBool: ");
		result.append(manyBool);
		result.append(", oneDouble: ");
		result.append(oneDouble);
		result.append(", manyDouble: ");
		result.append(manyDouble);
		result.append(", oneBigDecimal: ");
		result.append(oneBigDecimal);
		result.append(", manyBigDecimal: ");
		result.append(manyBigDecimal);
		result.append(", oneBigInteger: ");
		result.append(oneBigInteger);
		result.append(", manyBigInteger: ");
		result.append(manyBigInteger);
		result.append(", oneEnum: ");
		result.append(oneEnum);
		result.append(", manyEnums: ");
		result.append(manyEnums);
		result.append(", oneShort: ");
		result.append(oneShort);
		result.append(", manyShorts: ");
		result.append(manyShorts);
		result.append(", oneDate: ");
		result.append(oneDate);
		result.append(", manyDates: ");
		result.append(manyDates);
		result.append(", oneString: ");
		result.append(oneString);
		result.append(", manyString: ");
		result.append(manyString);
		result.append(')');
		return result.toString();
	}

} //AttributeTestContainerImpl
