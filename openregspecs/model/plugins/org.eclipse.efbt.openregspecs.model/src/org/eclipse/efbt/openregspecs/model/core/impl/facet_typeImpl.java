/**
 */
package org.eclipse.efbt.openregspecs.model.core.impl;

import org.eclipse.efbt.openregspecs.model.core.CorePackage;
import org.eclipse.efbt.openregspecs.model.core.facet_type;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>facet type</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.core.impl.facet_typeImpl#getDecimals <em>Decimals</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.core.impl.facet_typeImpl#getEndTime <em>End Time</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.core.impl.facet_typeImpl#getEndValue <em>End Value</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.core.impl.facet_typeImpl#getInterval <em>Interval</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.core.impl.facet_typeImpl#getIsSequence <em>Is Sequence</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.core.impl.facet_typeImpl#getMaxLength <em>Max Length</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.core.impl.facet_typeImpl#getMaxValue <em>Max Value</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.core.impl.facet_typeImpl#getMinLength <em>Min Length</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.core.impl.facet_typeImpl#getMinValue <em>Min Value</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.core.impl.facet_typeImpl#getPattern <em>Pattern</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.core.impl.facet_typeImpl#getStartTime <em>Start Time</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.core.impl.facet_typeImpl#getStartValue <em>Start Value</em>}</li>
 *   <li>{@link org.eclipse.efbt.openregspecs.model.core.impl.facet_typeImpl#getTimeInterval <em>Time Interval</em>}</li>
 * </ul>
 *
 * @generated
 */
public class facet_typeImpl extends MinimalEObjectImpl.Container implements facet_type {
	/**
	 * The default value of the '{@link #getDecimals() <em>Decimals</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getDecimals()
	 * @generated
	 * @ordered
	 */
	protected static final int DECIMALS_EDEFAULT = 0;

	/**
	 * The cached value of the '{@link #getDecimals() <em>Decimals</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getDecimals()
	 * @generated
	 * @ordered
	 */
	protected int decimals = DECIMALS_EDEFAULT;

	/**
	 * The default value of the '{@link #getEndTime() <em>End Time</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getEndTime()
	 * @generated
	 * @ordered
	 */
	protected static final String END_TIME_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getEndTime() <em>End Time</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getEndTime()
	 * @generated
	 * @ordered
	 */
	protected String endTime = END_TIME_EDEFAULT;

	/**
	 * The default value of the '{@link #getEndValue() <em>End Value</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getEndValue()
	 * @generated
	 * @ordered
	 */
	protected static final int END_VALUE_EDEFAULT = 0;

	/**
	 * The cached value of the '{@link #getEndValue() <em>End Value</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getEndValue()
	 * @generated
	 * @ordered
	 */
	protected int endValue = END_VALUE_EDEFAULT;

	/**
	 * The default value of the '{@link #getInterval() <em>Interval</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getInterval()
	 * @generated
	 * @ordered
	 */
	protected static final String INTERVAL_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getInterval() <em>Interval</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getInterval()
	 * @generated
	 * @ordered
	 */
	protected String interval = INTERVAL_EDEFAULT;

	/**
	 * The default value of the '{@link #getIsSequence() <em>Is Sequence</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getIsSequence()
	 * @generated
	 * @ordered
	 */
	protected static final String IS_SEQUENCE_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getIsSequence() <em>Is Sequence</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getIsSequence()
	 * @generated
	 * @ordered
	 */
	protected String isSequence = IS_SEQUENCE_EDEFAULT;

	/**
	 * The default value of the '{@link #getMaxLength() <em>Max Length</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getMaxLength()
	 * @generated
	 * @ordered
	 */
	protected static final int MAX_LENGTH_EDEFAULT = 0;

	/**
	 * The cached value of the '{@link #getMaxLength() <em>Max Length</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getMaxLength()
	 * @generated
	 * @ordered
	 */
	protected int maxLength = MAX_LENGTH_EDEFAULT;

	/**
	 * The default value of the '{@link #getMaxValue() <em>Max Value</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getMaxValue()
	 * @generated
	 * @ordered
	 */
	protected static final int MAX_VALUE_EDEFAULT = 0;

	/**
	 * The cached value of the '{@link #getMaxValue() <em>Max Value</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getMaxValue()
	 * @generated
	 * @ordered
	 */
	protected int maxValue = MAX_VALUE_EDEFAULT;

	/**
	 * The default value of the '{@link #getMinLength() <em>Min Length</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getMinLength()
	 * @generated
	 * @ordered
	 */
	protected static final int MIN_LENGTH_EDEFAULT = 0;

	/**
	 * The cached value of the '{@link #getMinLength() <em>Min Length</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getMinLength()
	 * @generated
	 * @ordered
	 */
	protected int minLength = MIN_LENGTH_EDEFAULT;

	/**
	 * The default value of the '{@link #getMinValue() <em>Min Value</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getMinValue()
	 * @generated
	 * @ordered
	 */
	protected static final int MIN_VALUE_EDEFAULT = 0;

	/**
	 * The cached value of the '{@link #getMinValue() <em>Min Value</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getMinValue()
	 * @generated
	 * @ordered
	 */
	protected int minValue = MIN_VALUE_EDEFAULT;

	/**
	 * The default value of the '{@link #getPattern() <em>Pattern</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getPattern()
	 * @generated
	 * @ordered
	 */
	protected static final String PATTERN_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getPattern() <em>Pattern</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getPattern()
	 * @generated
	 * @ordered
	 */
	protected String pattern = PATTERN_EDEFAULT;

	/**
	 * The default value of the '{@link #getStartTime() <em>Start Time</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getStartTime()
	 * @generated
	 * @ordered
	 */
	protected static final String START_TIME_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getStartTime() <em>Start Time</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getStartTime()
	 * @generated
	 * @ordered
	 */
	protected String startTime = START_TIME_EDEFAULT;

	/**
	 * The default value of the '{@link #getStartValue() <em>Start Value</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getStartValue()
	 * @generated
	 * @ordered
	 */
	protected static final int START_VALUE_EDEFAULT = 0;

	/**
	 * The cached value of the '{@link #getStartValue() <em>Start Value</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getStartValue()
	 * @generated
	 * @ordered
	 */
	protected int startValue = START_VALUE_EDEFAULT;

	/**
	 * The default value of the '{@link #getTimeInterval() <em>Time Interval</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTimeInterval()
	 * @generated
	 * @ordered
	 */
	protected static final String TIME_INTERVAL_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getTimeInterval() <em>Time Interval</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTimeInterval()
	 * @generated
	 * @ordered
	 */
	protected String timeInterval = TIME_INTERVAL_EDEFAULT;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected facet_typeImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return CorePackage.Literals.FACET_TYPE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public int getDecimals() {
		return decimals;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setDecimals(int newDecimals) {
		int oldDecimals = decimals;
		decimals = newDecimals;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CorePackage.FACET_TYPE__DECIMALS, oldDecimals, decimals));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getEndTime() {
		return endTime;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setEndTime(String newEndTime) {
		String oldEndTime = endTime;
		endTime = newEndTime;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CorePackage.FACET_TYPE__END_TIME, oldEndTime, endTime));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public int getEndValue() {
		return endValue;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setEndValue(int newEndValue) {
		int oldEndValue = endValue;
		endValue = newEndValue;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CorePackage.FACET_TYPE__END_VALUE, oldEndValue, endValue));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getInterval() {
		return interval;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setInterval(String newInterval) {
		String oldInterval = interval;
		interval = newInterval;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CorePackage.FACET_TYPE__INTERVAL, oldInterval, interval));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getIsSequence() {
		return isSequence;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setIsSequence(String newIsSequence) {
		String oldIsSequence = isSequence;
		isSequence = newIsSequence;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CorePackage.FACET_TYPE__IS_SEQUENCE, oldIsSequence, isSequence));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public int getMaxLength() {
		return maxLength;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setMaxLength(int newMaxLength) {
		int oldMaxLength = maxLength;
		maxLength = newMaxLength;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CorePackage.FACET_TYPE__MAX_LENGTH, oldMaxLength, maxLength));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public int getMaxValue() {
		return maxValue;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setMaxValue(int newMaxValue) {
		int oldMaxValue = maxValue;
		maxValue = newMaxValue;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CorePackage.FACET_TYPE__MAX_VALUE, oldMaxValue, maxValue));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public int getMinLength() {
		return minLength;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setMinLength(int newMinLength) {
		int oldMinLength = minLength;
		minLength = newMinLength;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CorePackage.FACET_TYPE__MIN_LENGTH, oldMinLength, minLength));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public int getMinValue() {
		return minValue;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setMinValue(int newMinValue) {
		int oldMinValue = minValue;
		minValue = newMinValue;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CorePackage.FACET_TYPE__MIN_VALUE, oldMinValue, minValue));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getPattern() {
		return pattern;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setPattern(String newPattern) {
		String oldPattern = pattern;
		pattern = newPattern;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CorePackage.FACET_TYPE__PATTERN, oldPattern, pattern));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getStartTime() {
		return startTime;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setStartTime(String newStartTime) {
		String oldStartTime = startTime;
		startTime = newStartTime;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CorePackage.FACET_TYPE__START_TIME, oldStartTime, startTime));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public int getStartValue() {
		return startValue;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setStartValue(int newStartValue) {
		int oldStartValue = startValue;
		startValue = newStartValue;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CorePackage.FACET_TYPE__START_VALUE, oldStartValue, startValue));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getTimeInterval() {
		return timeInterval;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setTimeInterval(String newTimeInterval) {
		String oldTimeInterval = timeInterval;
		timeInterval = newTimeInterval;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, CorePackage.FACET_TYPE__TIME_INTERVAL, oldTimeInterval, timeInterval));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case CorePackage.FACET_TYPE__DECIMALS:
				return getDecimals();
			case CorePackage.FACET_TYPE__END_TIME:
				return getEndTime();
			case CorePackage.FACET_TYPE__END_VALUE:
				return getEndValue();
			case CorePackage.FACET_TYPE__INTERVAL:
				return getInterval();
			case CorePackage.FACET_TYPE__IS_SEQUENCE:
				return getIsSequence();
			case CorePackage.FACET_TYPE__MAX_LENGTH:
				return getMaxLength();
			case CorePackage.FACET_TYPE__MAX_VALUE:
				return getMaxValue();
			case CorePackage.FACET_TYPE__MIN_LENGTH:
				return getMinLength();
			case CorePackage.FACET_TYPE__MIN_VALUE:
				return getMinValue();
			case CorePackage.FACET_TYPE__PATTERN:
				return getPattern();
			case CorePackage.FACET_TYPE__START_TIME:
				return getStartTime();
			case CorePackage.FACET_TYPE__START_VALUE:
				return getStartValue();
			case CorePackage.FACET_TYPE__TIME_INTERVAL:
				return getTimeInterval();
		}
		return super.eGet(featureID, resolve, coreType);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void eSet(int featureID, Object newValue) {
		switch (featureID) {
			case CorePackage.FACET_TYPE__DECIMALS:
				setDecimals((Integer)newValue);
				return;
			case CorePackage.FACET_TYPE__END_TIME:
				setEndTime((String)newValue);
				return;
			case CorePackage.FACET_TYPE__END_VALUE:
				setEndValue((Integer)newValue);
				return;
			case CorePackage.FACET_TYPE__INTERVAL:
				setInterval((String)newValue);
				return;
			case CorePackage.FACET_TYPE__IS_SEQUENCE:
				setIsSequence((String)newValue);
				return;
			case CorePackage.FACET_TYPE__MAX_LENGTH:
				setMaxLength((Integer)newValue);
				return;
			case CorePackage.FACET_TYPE__MAX_VALUE:
				setMaxValue((Integer)newValue);
				return;
			case CorePackage.FACET_TYPE__MIN_LENGTH:
				setMinLength((Integer)newValue);
				return;
			case CorePackage.FACET_TYPE__MIN_VALUE:
				setMinValue((Integer)newValue);
				return;
			case CorePackage.FACET_TYPE__PATTERN:
				setPattern((String)newValue);
				return;
			case CorePackage.FACET_TYPE__START_TIME:
				setStartTime((String)newValue);
				return;
			case CorePackage.FACET_TYPE__START_VALUE:
				setStartValue((Integer)newValue);
				return;
			case CorePackage.FACET_TYPE__TIME_INTERVAL:
				setTimeInterval((String)newValue);
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
			case CorePackage.FACET_TYPE__DECIMALS:
				setDecimals(DECIMALS_EDEFAULT);
				return;
			case CorePackage.FACET_TYPE__END_TIME:
				setEndTime(END_TIME_EDEFAULT);
				return;
			case CorePackage.FACET_TYPE__END_VALUE:
				setEndValue(END_VALUE_EDEFAULT);
				return;
			case CorePackage.FACET_TYPE__INTERVAL:
				setInterval(INTERVAL_EDEFAULT);
				return;
			case CorePackage.FACET_TYPE__IS_SEQUENCE:
				setIsSequence(IS_SEQUENCE_EDEFAULT);
				return;
			case CorePackage.FACET_TYPE__MAX_LENGTH:
				setMaxLength(MAX_LENGTH_EDEFAULT);
				return;
			case CorePackage.FACET_TYPE__MAX_VALUE:
				setMaxValue(MAX_VALUE_EDEFAULT);
				return;
			case CorePackage.FACET_TYPE__MIN_LENGTH:
				setMinLength(MIN_LENGTH_EDEFAULT);
				return;
			case CorePackage.FACET_TYPE__MIN_VALUE:
				setMinValue(MIN_VALUE_EDEFAULT);
				return;
			case CorePackage.FACET_TYPE__PATTERN:
				setPattern(PATTERN_EDEFAULT);
				return;
			case CorePackage.FACET_TYPE__START_TIME:
				setStartTime(START_TIME_EDEFAULT);
				return;
			case CorePackage.FACET_TYPE__START_VALUE:
				setStartValue(START_VALUE_EDEFAULT);
				return;
			case CorePackage.FACET_TYPE__TIME_INTERVAL:
				setTimeInterval(TIME_INTERVAL_EDEFAULT);
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
			case CorePackage.FACET_TYPE__DECIMALS:
				return decimals != DECIMALS_EDEFAULT;
			case CorePackage.FACET_TYPE__END_TIME:
				return END_TIME_EDEFAULT == null ? endTime != null : !END_TIME_EDEFAULT.equals(endTime);
			case CorePackage.FACET_TYPE__END_VALUE:
				return endValue != END_VALUE_EDEFAULT;
			case CorePackage.FACET_TYPE__INTERVAL:
				return INTERVAL_EDEFAULT == null ? interval != null : !INTERVAL_EDEFAULT.equals(interval);
			case CorePackage.FACET_TYPE__IS_SEQUENCE:
				return IS_SEQUENCE_EDEFAULT == null ? isSequence != null : !IS_SEQUENCE_EDEFAULT.equals(isSequence);
			case CorePackage.FACET_TYPE__MAX_LENGTH:
				return maxLength != MAX_LENGTH_EDEFAULT;
			case CorePackage.FACET_TYPE__MAX_VALUE:
				return maxValue != MAX_VALUE_EDEFAULT;
			case CorePackage.FACET_TYPE__MIN_LENGTH:
				return minLength != MIN_LENGTH_EDEFAULT;
			case CorePackage.FACET_TYPE__MIN_VALUE:
				return minValue != MIN_VALUE_EDEFAULT;
			case CorePackage.FACET_TYPE__PATTERN:
				return PATTERN_EDEFAULT == null ? pattern != null : !PATTERN_EDEFAULT.equals(pattern);
			case CorePackage.FACET_TYPE__START_TIME:
				return START_TIME_EDEFAULT == null ? startTime != null : !START_TIME_EDEFAULT.equals(startTime);
			case CorePackage.FACET_TYPE__START_VALUE:
				return startValue != START_VALUE_EDEFAULT;
			case CorePackage.FACET_TYPE__TIME_INTERVAL:
				return TIME_INTERVAL_EDEFAULT == null ? timeInterval != null : !TIME_INTERVAL_EDEFAULT.equals(timeInterval);
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

		StringBuilder result = new StringBuilder(super.toString());
		result.append(" (decimals: ");
		result.append(decimals);
		result.append(", endTime: ");
		result.append(endTime);
		result.append(", endValue: ");
		result.append(endValue);
		result.append(", interval: ");
		result.append(interval);
		result.append(", isSequence: ");
		result.append(isSequence);
		result.append(", maxLength: ");
		result.append(maxLength);
		result.append(", maxValue: ");
		result.append(maxValue);
		result.append(", minLength: ");
		result.append(minLength);
		result.append(", minValue: ");
		result.append(minValue);
		result.append(", pattern: ");
		result.append(pattern);
		result.append(", startTime: ");
		result.append(startTime);
		result.append(", startValue: ");
		result.append(startValue);
		result.append(", timeInterval: ");
		result.append(timeInterval);
		result.append(')');
		return result.toString();
	}

} //facet_typeImpl
