/**
 */
package row_transformation_logic.impl;

import java.util.Collection;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;

import org.eclipse.emf.ecore.util.EObjectResolvingEList;

import row_transformation_logic.CustomRowCreationApproach;
import row_transformation_logic.Row_transformation_logicPackage;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Custom Row Creation Approach</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link row_transformation_logic.impl.CustomRowCreationApproachImpl#getDependantVariables <em>Dependant Variables</em>}</li>
 * </ul>
 *
 * @generated
 */
public class CustomRowCreationApproachImpl extends RowCreationApproachImpl implements CustomRowCreationApproach {
	/**
	 * The cached value of the '{@link #getDependantVariables() <em>Dependant Variables</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getDependantVariables()
	 * @generated
	 * @ordered
	 */
	protected EList<EAttribute> dependantVariables;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected CustomRowCreationApproachImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Row_transformation_logicPackage.Literals.CUSTOM_ROW_CREATION_APPROACH;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<EAttribute> getDependantVariables() {
		if (dependantVariables == null) {
			dependantVariables = new EObjectResolvingEList<EAttribute>(EAttribute.class, this, Row_transformation_logicPackage.CUSTOM_ROW_CREATION_APPROACH__DEPENDANT_VARIABLES);
		}
		return dependantVariables;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Row_transformation_logicPackage.CUSTOM_ROW_CREATION_APPROACH__DEPENDANT_VARIABLES:
				return getDependantVariables();
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
			case Row_transformation_logicPackage.CUSTOM_ROW_CREATION_APPROACH__DEPENDANT_VARIABLES:
				getDependantVariables().clear();
				getDependantVariables().addAll((Collection<? extends EAttribute>)newValue);
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
			case Row_transformation_logicPackage.CUSTOM_ROW_CREATION_APPROACH__DEPENDANT_VARIABLES:
				getDependantVariables().clear();
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
			case Row_transformation_logicPackage.CUSTOM_ROW_CREATION_APPROACH__DEPENDANT_VARIABLES:
				return dependantVariables != null && !dependantVariables.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //CustomRowCreationApproachImpl
