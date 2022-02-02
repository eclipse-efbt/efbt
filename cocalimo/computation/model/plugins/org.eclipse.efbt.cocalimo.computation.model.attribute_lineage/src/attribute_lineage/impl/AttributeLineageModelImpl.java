/**
 */
package attribute_lineage.impl;

import attribute_lineage.AttributeLineageModel;
import attribute_lineage.Attribute_lineagePackage;

import cube_transformation_logic.CubeTransformationLogic;

import entities.BaseEntity;

import functions.FunctionSpec;

import java.util.Collection;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Attribute Lineage Model</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link attribute_lineage.impl.AttributeLineageModelImpl#getRowTransformations <em>Row Transformations</em>}</li>
 *   <li>{@link attribute_lineage.impl.AttributeLineageModelImpl#getBaseSchemas <em>Base Schemas</em>}</li>
 *   <li>{@link attribute_lineage.impl.AttributeLineageModelImpl#getSpecialFunctionSpecs <em>Special Function Specs</em>}</li>
 *   <li>{@link attribute_lineage.impl.AttributeLineageModelImpl#getName <em>Name</em>}</li>
 * </ul>
 *
 * @generated
 */
public class AttributeLineageModelImpl extends MinimalEObjectImpl.Container implements AttributeLineageModel {
	/**
	 * The cached value of the '{@link #getRowTransformations() <em>Row Transformations</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getRowTransformations()
	 * @generated
	 * @ordered
	 */
	protected EList<CubeTransformationLogic> rowTransformations;

	/**
	 * The cached value of the '{@link #getBaseSchemas() <em>Base Schemas</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getBaseSchemas()
	 * @generated
	 * @ordered
	 */
	protected EList<BaseEntity> baseSchemas;

	/**
	 * The cached value of the '{@link #getSpecialFunctionSpecs() <em>Special Function Specs</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSpecialFunctionSpecs()
	 * @generated
	 * @ordered
	 */
	protected EList<FunctionSpec> specialFunctionSpecs;

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
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected AttributeLineageModelImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Attribute_lineagePackage.Literals.ATTRIBUTE_LINEAGE_MODEL;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<CubeTransformationLogic> getRowTransformations() {
		if (rowTransformations == null) {
			rowTransformations = new EObjectContainmentEList<CubeTransformationLogic>(CubeTransformationLogic.class, this, Attribute_lineagePackage.ATTRIBUTE_LINEAGE_MODEL__ROW_TRANSFORMATIONS);
		}
		return rowTransformations;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<BaseEntity> getBaseSchemas() {
		if (baseSchemas == null) {
			baseSchemas = new EObjectContainmentEList<BaseEntity>(BaseEntity.class, this, Attribute_lineagePackage.ATTRIBUTE_LINEAGE_MODEL__BASE_SCHEMAS);
		}
		return baseSchemas;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<FunctionSpec> getSpecialFunctionSpecs() {
		if (specialFunctionSpecs == null) {
			specialFunctionSpecs = new EObjectContainmentEList<FunctionSpec>(FunctionSpec.class, this, Attribute_lineagePackage.ATTRIBUTE_LINEAGE_MODEL__SPECIAL_FUNCTION_SPECS);
		}
		return specialFunctionSpecs;
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
			eNotify(new ENotificationImpl(this, Notification.SET, Attribute_lineagePackage.ATTRIBUTE_LINEAGE_MODEL__NAME, oldName, name));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Attribute_lineagePackage.ATTRIBUTE_LINEAGE_MODEL__ROW_TRANSFORMATIONS:
				return ((InternalEList<?>)getRowTransformations()).basicRemove(otherEnd, msgs);
			case Attribute_lineagePackage.ATTRIBUTE_LINEAGE_MODEL__BASE_SCHEMAS:
				return ((InternalEList<?>)getBaseSchemas()).basicRemove(otherEnd, msgs);
			case Attribute_lineagePackage.ATTRIBUTE_LINEAGE_MODEL__SPECIAL_FUNCTION_SPECS:
				return ((InternalEList<?>)getSpecialFunctionSpecs()).basicRemove(otherEnd, msgs);
		}
		return super.eInverseRemove(otherEnd, featureID, msgs);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Attribute_lineagePackage.ATTRIBUTE_LINEAGE_MODEL__ROW_TRANSFORMATIONS:
				return getRowTransformations();
			case Attribute_lineagePackage.ATTRIBUTE_LINEAGE_MODEL__BASE_SCHEMAS:
				return getBaseSchemas();
			case Attribute_lineagePackage.ATTRIBUTE_LINEAGE_MODEL__SPECIAL_FUNCTION_SPECS:
				return getSpecialFunctionSpecs();
			case Attribute_lineagePackage.ATTRIBUTE_LINEAGE_MODEL__NAME:
				return getName();
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
			case Attribute_lineagePackage.ATTRIBUTE_LINEAGE_MODEL__ROW_TRANSFORMATIONS:
				getRowTransformations().clear();
				getRowTransformations().addAll((Collection<? extends CubeTransformationLogic>)newValue);
				return;
			case Attribute_lineagePackage.ATTRIBUTE_LINEAGE_MODEL__BASE_SCHEMAS:
				getBaseSchemas().clear();
				getBaseSchemas().addAll((Collection<? extends BaseEntity>)newValue);
				return;
			case Attribute_lineagePackage.ATTRIBUTE_LINEAGE_MODEL__SPECIAL_FUNCTION_SPECS:
				getSpecialFunctionSpecs().clear();
				getSpecialFunctionSpecs().addAll((Collection<? extends FunctionSpec>)newValue);
				return;
			case Attribute_lineagePackage.ATTRIBUTE_LINEAGE_MODEL__NAME:
				setName((String)newValue);
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
			case Attribute_lineagePackage.ATTRIBUTE_LINEAGE_MODEL__ROW_TRANSFORMATIONS:
				getRowTransformations().clear();
				return;
			case Attribute_lineagePackage.ATTRIBUTE_LINEAGE_MODEL__BASE_SCHEMAS:
				getBaseSchemas().clear();
				return;
			case Attribute_lineagePackage.ATTRIBUTE_LINEAGE_MODEL__SPECIAL_FUNCTION_SPECS:
				getSpecialFunctionSpecs().clear();
				return;
			case Attribute_lineagePackage.ATTRIBUTE_LINEAGE_MODEL__NAME:
				setName(NAME_EDEFAULT);
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
			case Attribute_lineagePackage.ATTRIBUTE_LINEAGE_MODEL__ROW_TRANSFORMATIONS:
				return rowTransformations != null && !rowTransformations.isEmpty();
			case Attribute_lineagePackage.ATTRIBUTE_LINEAGE_MODEL__BASE_SCHEMAS:
				return baseSchemas != null && !baseSchemas.isEmpty();
			case Attribute_lineagePackage.ATTRIBUTE_LINEAGE_MODEL__SPECIAL_FUNCTION_SPECS:
				return specialFunctionSpecs != null && !specialFunctionSpecs.isEmpty();
			case Attribute_lineagePackage.ATTRIBUTE_LINEAGE_MODEL__NAME:
				return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
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
		result.append(" (name: ");
		result.append(name);
		result.append(')');
		return result.toString();
	}

} //AttributeLineageModelImpl
