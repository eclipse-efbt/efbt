/**
 */
package testmodel.impl;

import java.math.BigDecimal;
import java.math.BigInteger;

import java.util.Collection;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.EObjectImpl;

import org.eclipse.emf.ecore.util.EDataTypeUniqueEList;

import testmodel.AttributeSample;
import testmodel.TestmodelPackage;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Attribute Sample</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link testmodel.impl.AttributeSampleImpl#getSingleIntOptional <em>Single Int Optional</em>}</li>
 *   <li>{@link testmodel.impl.AttributeSampleImpl#getListIntRequired <em>List Int Required</em>}</li>
 *   <li>{@link testmodel.impl.AttributeSampleImpl#getBigDecimal <em>Big Decimal</em>}</li>
 *   <li>{@link testmodel.impl.AttributeSampleImpl#getBigInteger <em>Big Integer</em>}</li>
 *   <li>{@link testmodel.impl.AttributeSampleImpl#getDouble <em>Double</em>}</li>
 * </ul>
 *
 * @generated
 */
public class AttributeSampleImpl extends EObjectImpl implements AttributeSample {
	/**
	 * The default value of the '{@link #getSingleIntOptional() <em>Single Int Optional</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSingleIntOptional()
	 * @generated
	 * @ordered
	 */
	protected static final int SINGLE_INT_OPTIONAL_EDEFAULT = 0;

	/**
	 * The cached value of the '{@link #getSingleIntOptional() <em>Single Int Optional</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSingleIntOptional()
	 * @generated
	 * @ordered
	 */
	protected int singleIntOptional = SINGLE_INT_OPTIONAL_EDEFAULT;

	/**
	 * The cached value of the '{@link #getListIntRequired() <em>List Int Required</em>}' attribute list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getListIntRequired()
	 * @generated
	 * @ordered
	 */
	protected EList<Integer> listIntRequired;

	/**
	 * The default value of the '{@link #getBigDecimal() <em>Big Decimal</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getBigDecimal()
	 * @generated
	 * @ordered
	 */
	protected static final BigDecimal BIG_DECIMAL_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getBigDecimal() <em>Big Decimal</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getBigDecimal()
	 * @generated
	 * @ordered
	 */
	protected BigDecimal bigDecimal = BIG_DECIMAL_EDEFAULT;

	/**
	 * The default value of the '{@link #getBigInteger() <em>Big Integer</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getBigInteger()
	 * @generated
	 * @ordered
	 */
	protected static final BigInteger BIG_INTEGER_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getBigInteger() <em>Big Integer</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getBigInteger()
	 * @generated
	 * @ordered
	 */
	protected BigInteger bigInteger = BIG_INTEGER_EDEFAULT;

	/**
	 * The default value of the '{@link #getDouble() <em>Double</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getDouble()
	 * @generated
	 * @ordered
	 */
	protected static final double DOUBLE_EDEFAULT = 0.0;

	/**
	 * The cached value of the '{@link #getDouble() <em>Double</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getDouble()
	 * @generated
	 * @ordered
	 */
	protected double double_ = DOUBLE_EDEFAULT;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected AttributeSampleImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return TestmodelPackage.Literals.ATTRIBUTE_SAMPLE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public int getSingleIntOptional() {
		return singleIntOptional;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setSingleIntOptional(int newSingleIntOptional) {
		int oldSingleIntOptional = singleIntOptional;
		singleIntOptional = newSingleIntOptional;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TestmodelPackage.ATTRIBUTE_SAMPLE__SINGLE_INT_OPTIONAL, oldSingleIntOptional, singleIntOptional));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<Integer> getListIntRequired() {
		if (listIntRequired == null) {
			listIntRequired = new EDataTypeUniqueEList<Integer>(Integer.class, this, TestmodelPackage.ATTRIBUTE_SAMPLE__LIST_INT_REQUIRED);
		}
		return listIntRequired;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public BigDecimal getBigDecimal() {
		return bigDecimal;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setBigDecimal(BigDecimal newBigDecimal) {
		BigDecimal oldBigDecimal = bigDecimal;
		bigDecimal = newBigDecimal;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TestmodelPackage.ATTRIBUTE_SAMPLE__BIG_DECIMAL, oldBigDecimal, bigDecimal));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public BigInteger getBigInteger() {
		return bigInteger;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setBigInteger(BigInteger newBigInteger) {
		BigInteger oldBigInteger = bigInteger;
		bigInteger = newBigInteger;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TestmodelPackage.ATTRIBUTE_SAMPLE__BIG_INTEGER, oldBigInteger, bigInteger));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public double getDouble() {
		return double_;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setDouble(double newDouble) {
		double oldDouble = double_;
		double_ = newDouble;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, TestmodelPackage.ATTRIBUTE_SAMPLE__DOUBLE, oldDouble, double_));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case TestmodelPackage.ATTRIBUTE_SAMPLE__SINGLE_INT_OPTIONAL:
				return getSingleIntOptional();
			case TestmodelPackage.ATTRIBUTE_SAMPLE__LIST_INT_REQUIRED:
				return getListIntRequired();
			case TestmodelPackage.ATTRIBUTE_SAMPLE__BIG_DECIMAL:
				return getBigDecimal();
			case TestmodelPackage.ATTRIBUTE_SAMPLE__BIG_INTEGER:
				return getBigInteger();
			case TestmodelPackage.ATTRIBUTE_SAMPLE__DOUBLE:
				return getDouble();
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
			case TestmodelPackage.ATTRIBUTE_SAMPLE__SINGLE_INT_OPTIONAL:
				setSingleIntOptional((Integer)newValue);
				return;
			case TestmodelPackage.ATTRIBUTE_SAMPLE__LIST_INT_REQUIRED:
				getListIntRequired().clear();
				getListIntRequired().addAll((Collection<? extends Integer>)newValue);
				return;
			case TestmodelPackage.ATTRIBUTE_SAMPLE__BIG_DECIMAL:
				setBigDecimal((BigDecimal)newValue);
				return;
			case TestmodelPackage.ATTRIBUTE_SAMPLE__BIG_INTEGER:
				setBigInteger((BigInteger)newValue);
				return;
			case TestmodelPackage.ATTRIBUTE_SAMPLE__DOUBLE:
				setDouble((Double)newValue);
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
			case TestmodelPackage.ATTRIBUTE_SAMPLE__SINGLE_INT_OPTIONAL:
				setSingleIntOptional(SINGLE_INT_OPTIONAL_EDEFAULT);
				return;
			case TestmodelPackage.ATTRIBUTE_SAMPLE__LIST_INT_REQUIRED:
				getListIntRequired().clear();
				return;
			case TestmodelPackage.ATTRIBUTE_SAMPLE__BIG_DECIMAL:
				setBigDecimal(BIG_DECIMAL_EDEFAULT);
				return;
			case TestmodelPackage.ATTRIBUTE_SAMPLE__BIG_INTEGER:
				setBigInteger(BIG_INTEGER_EDEFAULT);
				return;
			case TestmodelPackage.ATTRIBUTE_SAMPLE__DOUBLE:
				setDouble(DOUBLE_EDEFAULT);
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
			case TestmodelPackage.ATTRIBUTE_SAMPLE__SINGLE_INT_OPTIONAL:
				return singleIntOptional != SINGLE_INT_OPTIONAL_EDEFAULT;
			case TestmodelPackage.ATTRIBUTE_SAMPLE__LIST_INT_REQUIRED:
				return listIntRequired != null && !listIntRequired.isEmpty();
			case TestmodelPackage.ATTRIBUTE_SAMPLE__BIG_DECIMAL:
				return BIG_DECIMAL_EDEFAULT == null ? bigDecimal != null : !BIG_DECIMAL_EDEFAULT.equals(bigDecimal);
			case TestmodelPackage.ATTRIBUTE_SAMPLE__BIG_INTEGER:
				return BIG_INTEGER_EDEFAULT == null ? bigInteger != null : !BIG_INTEGER_EDEFAULT.equals(bigInteger);
			case TestmodelPackage.ATTRIBUTE_SAMPLE__DOUBLE:
				return double_ != DOUBLE_EDEFAULT;
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
		result.append(" (singleIntOptional: ");
		result.append(singleIntOptional);
		result.append(", listIntRequired: ");
		result.append(listIntRequired);
		result.append(", bigDecimal: ");
		result.append(bigDecimal);
		result.append(", bigInteger: ");
		result.append(bigInteger);
		result.append(", double: ");
		result.append(double_);
		result.append(')');
		return result.toString();
	}

} //AttributeSampleImpl
