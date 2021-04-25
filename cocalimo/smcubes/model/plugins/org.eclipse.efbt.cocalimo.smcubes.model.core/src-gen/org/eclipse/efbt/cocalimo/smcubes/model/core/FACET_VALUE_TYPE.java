/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.core;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.eclipse.emf.common.util.Enumerator;

/**
 * <!-- begin-user-doc -->
 * A representation of the literals of the enumeration '<em><b>FACET VALUE TYPE</b></em>',
 * and utility methods for working with them.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes.model.core.CorePackage#getFACET_VALUE_TYPE()
 * @model extendedMetaData="name='FACET_VALUE_TYPE'"
 * @generated
 */
public enum FACET_VALUE_TYPE implements Enumerator
{
	/**
	 * The '<em><b>Big Integer</b></em>' literal object.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #BIG_INTEGER_VALUE
	 * @generated
	 * @ordered
	 */
	BIG_INTEGER(0, "BigInteger", "BigInteger"),

	/**
	 * The '<em><b>Boolean</b></em>' literal object.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #BOOLEAN_VALUE
	 * @generated
	 * @ordered
	 */
	BOOLEAN(1, "Boolean", "Boolean"),

	/**
	 * The '<em><b>Date Time</b></em>' literal object.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #DATE_TIME_VALUE
	 * @generated
	 * @ordered
	 */
	DATE_TIME(2, "DateTime", "DateTime"),

	/**
	 * The '<em><b>Day Month Day Month</b></em>' literal object.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #DAY_MONTH_DAY_MONTH_VALUE
	 * @generated
	 * @ordered
	 */
	DAY_MONTH_DAY_MONTH(3, "DayMonthDayMonth", "Day_MonthDay_Month"),

	/**
	 * The '<em><b>Decimal</b></em>' literal object.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #DECIMAL_VALUE
	 * @generated
	 * @ordered
	 */
	DECIMAL(4, "Decimal", "Decimal"),

	/**
	 * The '<em><b>Double</b></em>' literal object.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #DOUBLE_VALUE
	 * @generated
	 * @ordered
	 */
	DOUBLE(5, "Double", "Double"),

	/**
	 * The '<em><b>Duration</b></em>' literal object.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #DURATION_VALUE
	 * @generated
	 * @ordered
	 */
	DURATION(6, "Duration", "Duration"),

	/**
	 * The '<em><b>Float</b></em>' literal object.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #FLOAT_VALUE
	 * @generated
	 * @ordered
	 */
	FLOAT(7, "Float", "Float"),

	/**
	 * The '<em><b>Gregorian Day</b></em>' literal object.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #GREGORIAN_DAY_VALUE
	 * @generated
	 * @ordered
	 */
	GREGORIAN_DAY(8, "GregorianDay", "GregorianDay"),

	/**
	 * The '<em><b>Gregorian Month</b></em>' literal object.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #GREGORIAN_MONTH_VALUE
	 * @generated
	 * @ordered
	 */
	GREGORIAN_MONTH(9, "GregorianMonth", "GregorianMonth"),

	/**
	 * The '<em><b>Gregorian Year</b></em>' literal object.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #GREGORIAN_YEAR_VALUE
	 * @generated
	 * @ordered
	 */
	GREGORIAN_YEAR(11, "GregorianYear", "GregorianYear"),

	/**
	 * The '<em><b>Integer</b></em>' literal object.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #INTEGER_VALUE
	 * @generated
	 * @ordered
	 */
	INTEGER(12, "Integer", "Integer"),

	/**
	 * The '<em><b>Long</b></em>' literal object.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #LONG_VALUE
	 * @generated
	 * @ordered
	 */
	LONG(13, "Long", "Long"),

	/**
	 * The '<em><b>Short</b></em>' literal object.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #SHORT_VALUE
	 * @generated
	 * @ordered
	 */
	SHORT(14, "Short", "Short"),

	/**
	 * The '<em><b>String</b></em>' literal object.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #STRING_VALUE
	 * @generated
	 * @ordered
	 */
	STRING(15, "String", "String"),

	/**
	 * The '<em><b>Time</b></em>' literal object.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #TIME_VALUE
	 * @generated
	 * @ordered
	 */
	TIME(16, "Time", "Time"),

	/**
	 * The '<em><b>URI</b></em>' literal object.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #URI_VALUE
	 * @generated
	 * @ordered
	 */
	URI(17, "URI", "URI");

	/**
	 * The '<em><b>Big Integer</b></em>' literal value.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #BIG_INTEGER
	 * @model name="BigInteger"
	 * @generated
	 * @ordered
	 */
	public static final int BIG_INTEGER_VALUE = 0;

	/**
	 * The '<em><b>Boolean</b></em>' literal value.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #BOOLEAN
	 * @model name="Boolean"
	 * @generated
	 * @ordered
	 */
	public static final int BOOLEAN_VALUE = 1;

	/**
	 * The '<em><b>Date Time</b></em>' literal value.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #DATE_TIME
	 * @model name="DateTime"
	 * @generated
	 * @ordered
	 */
	public static final int DATE_TIME_VALUE = 2;

	/**
	 * The '<em><b>Day Month Day Month</b></em>' literal value.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #DAY_MONTH_DAY_MONTH
	 * @model name="DayMonthDayMonth" literal="Day_MonthDay_Month"
	 * @generated
	 * @ordered
	 */
	public static final int DAY_MONTH_DAY_MONTH_VALUE = 3;

	/**
	 * The '<em><b>Decimal</b></em>' literal value.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #DECIMAL
	 * @model name="Decimal"
	 * @generated
	 * @ordered
	 */
	public static final int DECIMAL_VALUE = 4;

	/**
	 * The '<em><b>Double</b></em>' literal value.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #DOUBLE
	 * @model name="Double"
	 * @generated
	 * @ordered
	 */
	public static final int DOUBLE_VALUE = 5;

	/**
	 * The '<em><b>Duration</b></em>' literal value.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #DURATION
	 * @model name="Duration"
	 * @generated
	 * @ordered
	 */
	public static final int DURATION_VALUE = 6;

	/**
	 * The '<em><b>Float</b></em>' literal value.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #FLOAT
	 * @model name="Float"
	 * @generated
	 * @ordered
	 */
	public static final int FLOAT_VALUE = 7;

	/**
	 * The '<em><b>Gregorian Day</b></em>' literal value.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #GREGORIAN_DAY
	 * @model name="GregorianDay"
	 * @generated
	 * @ordered
	 */
	public static final int GREGORIAN_DAY_VALUE = 8;

	/**
	 * The '<em><b>Gregorian Month</b></em>' literal value.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #GREGORIAN_MONTH
	 * @model name="GregorianMonth"
	 * @generated
	 * @ordered
	 */
	public static final int GREGORIAN_MONTH_VALUE = 9;

	/**
	 * The '<em><b>Gregorian Year</b></em>' literal value.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #GREGORIAN_YEAR
	 * @model name="GregorianYear"
	 * @generated
	 * @ordered
	 */
	public static final int GREGORIAN_YEAR_VALUE = 11;

	/**
	 * The '<em><b>Integer</b></em>' literal value.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #INTEGER
	 * @model name="Integer"
	 * @generated
	 * @ordered
	 */
	public static final int INTEGER_VALUE = 12;

	/**
	 * The '<em><b>Long</b></em>' literal value.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #LONG
	 * @model name="Long"
	 * @generated
	 * @ordered
	 */
	public static final int LONG_VALUE = 13;

	/**
	 * The '<em><b>Short</b></em>' literal value.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #SHORT
	 * @model name="Short"
	 * @generated
	 * @ordered
	 */
	public static final int SHORT_VALUE = 14;

	/**
	 * The '<em><b>String</b></em>' literal value.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #STRING
	 * @model name="String"
	 * @generated
	 * @ordered
	 */
	public static final int STRING_VALUE = 15;

	/**
	 * The '<em><b>Time</b></em>' literal value.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #TIME
	 * @model name="Time"
	 * @generated
	 * @ordered
	 */
	public static final int TIME_VALUE = 16;

	/**
	 * The '<em><b>URI</b></em>' literal value.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #URI
	 * @model
	 * @generated
	 * @ordered
	 */
	public static final int URI_VALUE = 17;

	/**
	 * An array of all the '<em><b>FACET VALUE TYPE</b></em>' enumerators.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private static final FACET_VALUE_TYPE[] VALUES_ARRAY =
		new FACET_VALUE_TYPE[] {
			BIG_INTEGER,
			BOOLEAN,
			DATE_TIME,
			DAY_MONTH_DAY_MONTH,
			DECIMAL,
			DOUBLE,
			DURATION,
			FLOAT,
			GREGORIAN_DAY,
			GREGORIAN_MONTH,
			GREGORIAN_YEAR,
			INTEGER,
			LONG,
			SHORT,
			STRING,
			TIME,
			URI,
		};

	/**
	 * A public read-only list of all the '<em><b>FACET VALUE TYPE</b></em>' enumerators.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static final List<FACET_VALUE_TYPE> VALUES = Collections.unmodifiableList(Arrays.asList(VALUES_ARRAY));

	/**
	 * Returns the '<em><b>FACET VALUE TYPE</b></em>' literal with the specified literal value.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param literal the literal.
	 * @return the matching enumerator or <code>null</code>.
	 * @generated
	 */
	public static FACET_VALUE_TYPE get(String literal)
	{
		for (int i = 0; i < VALUES_ARRAY.length; ++i) {
			FACET_VALUE_TYPE result = VALUES_ARRAY[i];
			if (result.toString().equals(literal)) {
				return result;
			}
		}
		return null;
	}

	/**
	 * Returns the '<em><b>FACET VALUE TYPE</b></em>' literal with the specified name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param name the name.
	 * @return the matching enumerator or <code>null</code>.
	 * @generated
	 */
	public static FACET_VALUE_TYPE getByName(String name)
	{
		for (int i = 0; i < VALUES_ARRAY.length; ++i) {
			FACET_VALUE_TYPE result = VALUES_ARRAY[i];
			if (result.getName().equals(name)) {
				return result;
			}
		}
		return null;
	}

	/**
	 * Returns the '<em><b>FACET VALUE TYPE</b></em>' literal with the specified integer value.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the integer value.
	 * @return the matching enumerator or <code>null</code>.
	 * @generated
	 */
	public static FACET_VALUE_TYPE get(int value)
	{
		switch (value) {
			case BIG_INTEGER_VALUE: return BIG_INTEGER;
			case BOOLEAN_VALUE: return BOOLEAN;
			case DATE_TIME_VALUE: return DATE_TIME;
			case DAY_MONTH_DAY_MONTH_VALUE: return DAY_MONTH_DAY_MONTH;
			case DECIMAL_VALUE: return DECIMAL;
			case DOUBLE_VALUE: return DOUBLE;
			case DURATION_VALUE: return DURATION;
			case FLOAT_VALUE: return FLOAT;
			case GREGORIAN_DAY_VALUE: return GREGORIAN_DAY;
			case GREGORIAN_MONTH_VALUE: return GREGORIAN_MONTH;
			case GREGORIAN_YEAR_VALUE: return GREGORIAN_YEAR;
			case INTEGER_VALUE: return INTEGER;
			case LONG_VALUE: return LONG;
			case SHORT_VALUE: return SHORT;
			case STRING_VALUE: return STRING;
			case TIME_VALUE: return TIME;
			case URI_VALUE: return URI;
		}
		return null;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private final int value;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private final String name;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private final String literal;

	/**
	 * Only this class can construct instances.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private FACET_VALUE_TYPE(int value, String name, String literal)
	{
		this.value = value;
		this.name = name;
		this.literal = literal;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public int getValue()
	{
	  return value;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getName()
	{
	  return name;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getLiteral()
	{
	  return literal;
	}

	/**
	 * Returns the literal value of the enumerator, which is its string representation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String toString()
	{
		return literal;
	}
	
} //FACET_VALUE_TYPE
