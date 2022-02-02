/**
 */
package column_transformation_logic.impl;

import column_transformation_logic.Column_transformation_logicPackage;
import column_transformation_logic.SpeculativeCubeColumnParameter;

import functions.impl.ParameterImpl;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Speculative Cube Column Parameter</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link column_transformation_logic.impl.SpeculativeCubeColumnParameterImpl#getCube <em>Cube</em>}</li>
 *   <li>{@link column_transformation_logic.impl.SpeculativeCubeColumnParameterImpl#getAttribute <em>Attribute</em>}</li>
 * </ul>
 *
 * @generated
 */
public class SpeculativeCubeColumnParameterImpl extends ParameterImpl implements SpeculativeCubeColumnParameter {
	/**
	 * The cached value of the '{@link #getCube() <em>Cube</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCube()
	 * @generated
	 * @ordered
	 */
	protected EClass cube;

	/**
	 * The cached value of the '{@link #getAttribute() <em>Attribute</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getAttribute()
	 * @generated
	 * @ordered
	 */
	protected EAttribute attribute;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected SpeculativeCubeColumnParameterImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Column_transformation_logicPackage.Literals.SPECULATIVE_CUBE_COLUMN_PARAMETER;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getCube() {
		if (cube != null && cube.eIsProxy()) {
			InternalEObject oldCube = (InternalEObject)cube;
			cube = (EClass)eResolveProxy(oldCube);
			if (cube != oldCube) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Column_transformation_logicPackage.SPECULATIVE_CUBE_COLUMN_PARAMETER__CUBE, oldCube, cube));
			}
		}
		return cube;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass basicGetCube() {
		return cube;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setCube(EClass newCube) {
		EClass oldCube = cube;
		cube = newCube;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Column_transformation_logicPackage.SPECULATIVE_CUBE_COLUMN_PARAMETER__CUBE, oldCube, cube));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getAttribute() {
		if (attribute != null && attribute.eIsProxy()) {
			InternalEObject oldAttribute = (InternalEObject)attribute;
			attribute = (EAttribute)eResolveProxy(oldAttribute);
			if (attribute != oldAttribute) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Column_transformation_logicPackage.SPECULATIVE_CUBE_COLUMN_PARAMETER__ATTRIBUTE, oldAttribute, attribute));
			}
		}
		return attribute;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute basicGetAttribute() {
		return attribute;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setAttribute(EAttribute newAttribute) {
		EAttribute oldAttribute = attribute;
		attribute = newAttribute;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Column_transformation_logicPackage.SPECULATIVE_CUBE_COLUMN_PARAMETER__ATTRIBUTE, oldAttribute, attribute));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Column_transformation_logicPackage.SPECULATIVE_CUBE_COLUMN_PARAMETER__CUBE:
				if (resolve) return getCube();
				return basicGetCube();
			case Column_transformation_logicPackage.SPECULATIVE_CUBE_COLUMN_PARAMETER__ATTRIBUTE:
				if (resolve) return getAttribute();
				return basicGetAttribute();
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
			case Column_transformation_logicPackage.SPECULATIVE_CUBE_COLUMN_PARAMETER__CUBE:
				setCube((EClass)newValue);
				return;
			case Column_transformation_logicPackage.SPECULATIVE_CUBE_COLUMN_PARAMETER__ATTRIBUTE:
				setAttribute((EAttribute)newValue);
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
			case Column_transformation_logicPackage.SPECULATIVE_CUBE_COLUMN_PARAMETER__CUBE:
				setCube((EClass)null);
				return;
			case Column_transformation_logicPackage.SPECULATIVE_CUBE_COLUMN_PARAMETER__ATTRIBUTE:
				setAttribute((EAttribute)null);
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
			case Column_transformation_logicPackage.SPECULATIVE_CUBE_COLUMN_PARAMETER__CUBE:
				return cube != null;
			case Column_transformation_logicPackage.SPECULATIVE_CUBE_COLUMN_PARAMETER__ATTRIBUTE:
				return attribute != null;
		}
		return super.eIsSet(featureID);
	}

} //SpeculativeCubeColumnParameterImpl
