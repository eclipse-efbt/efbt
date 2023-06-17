/**
 */
package org.eclipse.efbt.ecore4reg.model.ecore4reg.impl;

import org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLTransformation;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>VTL Transformation</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLTransformationImpl#getExpression <em>Expression</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLTransformationImpl#getDescription <em>Description</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLTransformationImpl#getTransformation_id <em>Transformation id</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLTransformationImpl#getOrder_in_scheme <em>Order in scheme</em>}</li>
 * </ul>
 *
 * @generated
 */
public class VTLTransformationImpl extends MinimalEObjectImpl.Container implements VTLTransformation {
	/**
	 * The default value of the '{@link #getExpression() <em>Expression</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getExpression()
	 * @generated
	 * @ordered
	 */
	protected static final String EXPRESSION_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getExpression() <em>Expression</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getExpression()
	 * @generated
	 * @ordered
	 */
	protected String expression = EXPRESSION_EDEFAULT;

	/**
	 * The default value of the '{@link #getDescription() <em>Description</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getDescription()
	 * @generated
	 * @ordered
	 */
	protected static final String DESCRIPTION_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getDescription() <em>Description</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getDescription()
	 * @generated
	 * @ordered
	 */
	protected String description = DESCRIPTION_EDEFAULT;

	/**
	 * The default value of the '{@link #getTransformation_id() <em>Transformation id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTransformation_id()
	 * @generated
	 * @ordered
	 */
	protected static final String TRANSFORMATION_ID_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getTransformation_id() <em>Transformation id</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTransformation_id()
	 * @generated
	 * @ordered
	 */
	protected String transformation_id = TRANSFORMATION_ID_EDEFAULT;

	/**
	 * The default value of the '{@link #getOrder_in_scheme() <em>Order in scheme</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getOrder_in_scheme()
	 * @generated
	 * @ordered
	 */
	protected static final String ORDER_IN_SCHEME_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getOrder_in_scheme() <em>Order in scheme</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getOrder_in_scheme()
	 * @generated
	 * @ordered
	 */
	protected String order_in_scheme = ORDER_IN_SCHEME_EDEFAULT;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected VTLTransformationImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Ecore4regPackage.eINSTANCE.getVTLTransformation();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getExpression() {
		return expression;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setExpression(String newExpression) {
		String oldExpression = expression;
		expression = newExpression;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore4regPackage.VTL_TRANSFORMATION__EXPRESSION, oldExpression, expression));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getDescription() {
		return description;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setDescription(String newDescription) {
		String oldDescription = description;
		description = newDescription;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore4regPackage.VTL_TRANSFORMATION__DESCRIPTION, oldDescription, description));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getTransformation_id() {
		return transformation_id;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setTransformation_id(String newTransformation_id) {
		String oldTransformation_id = transformation_id;
		transformation_id = newTransformation_id;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore4regPackage.VTL_TRANSFORMATION__TRANSFORMATION_ID, oldTransformation_id, transformation_id));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getOrder_in_scheme() {
		return order_in_scheme;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setOrder_in_scheme(String newOrder_in_scheme) {
		String oldOrder_in_scheme = order_in_scheme;
		order_in_scheme = newOrder_in_scheme;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore4regPackage.VTL_TRANSFORMATION__ORDER_IN_SCHEME, oldOrder_in_scheme, order_in_scheme));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Ecore4regPackage.VTL_TRANSFORMATION__EXPRESSION:
				return getExpression();
			case Ecore4regPackage.VTL_TRANSFORMATION__DESCRIPTION:
				return getDescription();
			case Ecore4regPackage.VTL_TRANSFORMATION__TRANSFORMATION_ID:
				return getTransformation_id();
			case Ecore4regPackage.VTL_TRANSFORMATION__ORDER_IN_SCHEME:
				return getOrder_in_scheme();
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
			case Ecore4regPackage.VTL_TRANSFORMATION__EXPRESSION:
				setExpression((String)newValue);
				return;
			case Ecore4regPackage.VTL_TRANSFORMATION__DESCRIPTION:
				setDescription((String)newValue);
				return;
			case Ecore4regPackage.VTL_TRANSFORMATION__TRANSFORMATION_ID:
				setTransformation_id((String)newValue);
				return;
			case Ecore4regPackage.VTL_TRANSFORMATION__ORDER_IN_SCHEME:
				setOrder_in_scheme((String)newValue);
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
			case Ecore4regPackage.VTL_TRANSFORMATION__EXPRESSION:
				setExpression(EXPRESSION_EDEFAULT);
				return;
			case Ecore4regPackage.VTL_TRANSFORMATION__DESCRIPTION:
				setDescription(DESCRIPTION_EDEFAULT);
				return;
			case Ecore4regPackage.VTL_TRANSFORMATION__TRANSFORMATION_ID:
				setTransformation_id(TRANSFORMATION_ID_EDEFAULT);
				return;
			case Ecore4regPackage.VTL_TRANSFORMATION__ORDER_IN_SCHEME:
				setOrder_in_scheme(ORDER_IN_SCHEME_EDEFAULT);
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
			case Ecore4regPackage.VTL_TRANSFORMATION__EXPRESSION:
				return EXPRESSION_EDEFAULT == null ? expression != null : !EXPRESSION_EDEFAULT.equals(expression);
			case Ecore4regPackage.VTL_TRANSFORMATION__DESCRIPTION:
				return DESCRIPTION_EDEFAULT == null ? description != null : !DESCRIPTION_EDEFAULT.equals(description);
			case Ecore4regPackage.VTL_TRANSFORMATION__TRANSFORMATION_ID:
				return TRANSFORMATION_ID_EDEFAULT == null ? transformation_id != null : !TRANSFORMATION_ID_EDEFAULT.equals(transformation_id);
			case Ecore4regPackage.VTL_TRANSFORMATION__ORDER_IN_SCHEME:
				return ORDER_IN_SCHEME_EDEFAULT == null ? order_in_scheme != null : !ORDER_IN_SCHEME_EDEFAULT.equals(order_in_scheme);
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
		result.append(" (expression: ");
		result.append(expression);
		result.append(", description: ");
		result.append(description);
		result.append(", transformation_id: ");
		result.append(transformation_id);
		result.append(", order_in_scheme: ");
		result.append(order_in_scheme);
		result.append(')');
		return result.toString();
	}

} //VTLTransformationImpl
