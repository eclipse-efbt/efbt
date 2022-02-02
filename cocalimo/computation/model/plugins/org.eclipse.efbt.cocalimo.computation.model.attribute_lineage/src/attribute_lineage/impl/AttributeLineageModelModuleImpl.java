/**
 */
package attribute_lineage.impl;

import attribute_lineage.AttributeLineageModel;
import attribute_lineage.AttributeLineageModelModule;
import attribute_lineage.Attribute_lineagePackage;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.core.model.module_management.impl.ModuleImpl;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Attribute Lineage Model Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link attribute_lineage.impl.AttributeLineageModelModuleImpl#getAttributeLineageModels <em>Attribute Lineage Models</em>}</li>
 * </ul>
 *
 * @generated
 */
public class AttributeLineageModelModuleImpl extends ModuleImpl implements AttributeLineageModelModule {
	/**
	 * The cached value of the '{@link #getAttributeLineageModels() <em>Attribute Lineage Models</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getAttributeLineageModels()
	 * @generated
	 * @ordered
	 */
	protected EList<AttributeLineageModel> attributeLineageModels;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected AttributeLineageModelModuleImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Attribute_lineagePackage.Literals.ATTRIBUTE_LINEAGE_MODEL_MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<AttributeLineageModel> getAttributeLineageModels() {
		if (attributeLineageModels == null) {
			attributeLineageModels = new EObjectContainmentEList<AttributeLineageModel>(AttributeLineageModel.class, this, Attribute_lineagePackage.ATTRIBUTE_LINEAGE_MODEL_MODULE__ATTRIBUTE_LINEAGE_MODELS);
		}
		return attributeLineageModels;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Attribute_lineagePackage.ATTRIBUTE_LINEAGE_MODEL_MODULE__ATTRIBUTE_LINEAGE_MODELS:
				return ((InternalEList<?>)getAttributeLineageModels()).basicRemove(otherEnd, msgs);
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
			case Attribute_lineagePackage.ATTRIBUTE_LINEAGE_MODEL_MODULE__ATTRIBUTE_LINEAGE_MODELS:
				return getAttributeLineageModels();
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
			case Attribute_lineagePackage.ATTRIBUTE_LINEAGE_MODEL_MODULE__ATTRIBUTE_LINEAGE_MODELS:
				getAttributeLineageModels().clear();
				getAttributeLineageModels().addAll((Collection<? extends AttributeLineageModel>)newValue);
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
			case Attribute_lineagePackage.ATTRIBUTE_LINEAGE_MODEL_MODULE__ATTRIBUTE_LINEAGE_MODELS:
				getAttributeLineageModels().clear();
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
			case Attribute_lineagePackage.ATTRIBUTE_LINEAGE_MODEL_MODULE__ATTRIBUTE_LINEAGE_MODELS:
				return attributeLineageModels != null && !attributeLineageModels.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //AttributeLineageModelModuleImpl
