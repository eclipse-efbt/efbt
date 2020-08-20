/**
 */
package org.eclipse.efbt.controller.core.model.platform_call.impl;

import org.eclipse.efbt.controller.core.model.platform_call.CompareAttributeLineageModels;
import org.eclipse.efbt.controller.core.model.platform_call.Platform_callPackage;
import org.eclipse.efbt.controller.core.model.platform_call.SubsetBoolean;

import org.eclipse.efbt.lineage.attributelineage.model.attribute_lineage.AttributeLineageModel;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Compare Attribute Lineage Models</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.controller.core.model.platform_call.impl.CompareAttributeLineageModelsImpl#getResultingModel <em>Resulting Model</em>}</li>
 *   <li>{@link org.eclipse.efbt.controller.core.model.platform_call.impl.CompareAttributeLineageModelsImpl#getSubsetBoolean <em>Subset Boolean</em>}</li>
 *   <li>{@link org.eclipse.efbt.controller.core.model.platform_call.impl.CompareAttributeLineageModelsImpl#getNotIncludedModel <em>Not Included Model</em>}</li>
 *   <li>{@link org.eclipse.efbt.controller.core.model.platform_call.impl.CompareAttributeLineageModelsImpl#getFirstModel <em>First Model</em>}</li>
 *   <li>{@link org.eclipse.efbt.controller.core.model.platform_call.impl.CompareAttributeLineageModelsImpl#getSecondModel <em>Second Model</em>}</li>
 * </ul>
 *
 * @generated
 */
public class CompareAttributeLineageModelsImpl extends PlatformCallImpl implements CompareAttributeLineageModels
{
	/**
	 * The cached value of the '{@link #getResultingModel() <em>Resulting Model</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getResultingModel()
	 * @generated
	 * @ordered
	 */
	protected AttributeLineageModel resultingModel;

	/**
	 * The cached value of the '{@link #getSubsetBoolean() <em>Subset Boolean</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSubsetBoolean()
	 * @generated
	 * @ordered
	 */
	protected SubsetBoolean subsetBoolean;

	/**
	 * The cached value of the '{@link #getNotIncludedModel() <em>Not Included Model</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getNotIncludedModel()
	 * @generated
	 * @ordered
	 */
	protected AttributeLineageModel notIncludedModel;

	/**
	 * The cached value of the '{@link #getFirstModel() <em>First Model</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getFirstModel()
	 * @generated
	 * @ordered
	 */
	protected AttributeLineageModel firstModel;

	/**
	 * The cached value of the '{@link #getSecondModel() <em>Second Model</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSecondModel()
	 * @generated
	 * @ordered
	 */
	protected AttributeLineageModel secondModel;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected CompareAttributeLineageModelsImpl()
	{
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass()
	{
		return Platform_callPackage.Literals.COMPARE_ATTRIBUTE_LINEAGE_MODELS;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public AttributeLineageModel getResultingModel()
	{
		return resultingModel;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetResultingModel(AttributeLineageModel newResultingModel, NotificationChain msgs)
	{
		AttributeLineageModel oldResultingModel = resultingModel;
		resultingModel = newResultingModel;
		if (eNotificationRequired())
		{
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Platform_callPackage.COMPARE_ATTRIBUTE_LINEAGE_MODELS__RESULTING_MODEL, oldResultingModel, newResultingModel);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setResultingModel(AttributeLineageModel newResultingModel)
	{
		if (newResultingModel != resultingModel)
		{
			NotificationChain msgs = null;
			if (resultingModel != null)
				msgs = ((InternalEObject)resultingModel).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Platform_callPackage.COMPARE_ATTRIBUTE_LINEAGE_MODELS__RESULTING_MODEL, null, msgs);
			if (newResultingModel != null)
				msgs = ((InternalEObject)newResultingModel).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Platform_callPackage.COMPARE_ATTRIBUTE_LINEAGE_MODELS__RESULTING_MODEL, null, msgs);
			msgs = basicSetResultingModel(newResultingModel, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Platform_callPackage.COMPARE_ATTRIBUTE_LINEAGE_MODELS__RESULTING_MODEL, newResultingModel, newResultingModel));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public SubsetBoolean getSubsetBoolean()
	{
		return subsetBoolean;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetSubsetBoolean(SubsetBoolean newSubsetBoolean, NotificationChain msgs)
	{
		SubsetBoolean oldSubsetBoolean = subsetBoolean;
		subsetBoolean = newSubsetBoolean;
		if (eNotificationRequired())
		{
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Platform_callPackage.COMPARE_ATTRIBUTE_LINEAGE_MODELS__SUBSET_BOOLEAN, oldSubsetBoolean, newSubsetBoolean);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setSubsetBoolean(SubsetBoolean newSubsetBoolean)
	{
		if (newSubsetBoolean != subsetBoolean)
		{
			NotificationChain msgs = null;
			if (subsetBoolean != null)
				msgs = ((InternalEObject)subsetBoolean).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Platform_callPackage.COMPARE_ATTRIBUTE_LINEAGE_MODELS__SUBSET_BOOLEAN, null, msgs);
			if (newSubsetBoolean != null)
				msgs = ((InternalEObject)newSubsetBoolean).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Platform_callPackage.COMPARE_ATTRIBUTE_LINEAGE_MODELS__SUBSET_BOOLEAN, null, msgs);
			msgs = basicSetSubsetBoolean(newSubsetBoolean, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Platform_callPackage.COMPARE_ATTRIBUTE_LINEAGE_MODELS__SUBSET_BOOLEAN, newSubsetBoolean, newSubsetBoolean));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public AttributeLineageModel getNotIncludedModel()
	{
		return notIncludedModel;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetNotIncludedModel(AttributeLineageModel newNotIncludedModel, NotificationChain msgs)
	{
		AttributeLineageModel oldNotIncludedModel = notIncludedModel;
		notIncludedModel = newNotIncludedModel;
		if (eNotificationRequired())
		{
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Platform_callPackage.COMPARE_ATTRIBUTE_LINEAGE_MODELS__NOT_INCLUDED_MODEL, oldNotIncludedModel, newNotIncludedModel);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setNotIncludedModel(AttributeLineageModel newNotIncludedModel)
	{
		if (newNotIncludedModel != notIncludedModel)
		{
			NotificationChain msgs = null;
			if (notIncludedModel != null)
				msgs = ((InternalEObject)notIncludedModel).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Platform_callPackage.COMPARE_ATTRIBUTE_LINEAGE_MODELS__NOT_INCLUDED_MODEL, null, msgs);
			if (newNotIncludedModel != null)
				msgs = ((InternalEObject)newNotIncludedModel).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Platform_callPackage.COMPARE_ATTRIBUTE_LINEAGE_MODELS__NOT_INCLUDED_MODEL, null, msgs);
			msgs = basicSetNotIncludedModel(newNotIncludedModel, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Platform_callPackage.COMPARE_ATTRIBUTE_LINEAGE_MODELS__NOT_INCLUDED_MODEL, newNotIncludedModel, newNotIncludedModel));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public AttributeLineageModel getFirstModel()
	{
		if (firstModel != null && firstModel.eIsProxy())
		{
			InternalEObject oldFirstModel = (InternalEObject)firstModel;
			firstModel = (AttributeLineageModel)eResolveProxy(oldFirstModel);
			if (firstModel != oldFirstModel)
			{
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Platform_callPackage.COMPARE_ATTRIBUTE_LINEAGE_MODELS__FIRST_MODEL, oldFirstModel, firstModel));
			}
		}
		return firstModel;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public AttributeLineageModel basicGetFirstModel()
	{
		return firstModel;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setFirstModel(AttributeLineageModel newFirstModel)
	{
		AttributeLineageModel oldFirstModel = firstModel;
		firstModel = newFirstModel;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Platform_callPackage.COMPARE_ATTRIBUTE_LINEAGE_MODELS__FIRST_MODEL, oldFirstModel, firstModel));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public AttributeLineageModel getSecondModel()
	{
		if (secondModel != null && secondModel.eIsProxy())
		{
			InternalEObject oldSecondModel = (InternalEObject)secondModel;
			secondModel = (AttributeLineageModel)eResolveProxy(oldSecondModel);
			if (secondModel != oldSecondModel)
			{
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Platform_callPackage.COMPARE_ATTRIBUTE_LINEAGE_MODELS__SECOND_MODEL, oldSecondModel, secondModel));
			}
		}
		return secondModel;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public AttributeLineageModel basicGetSecondModel()
	{
		return secondModel;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setSecondModel(AttributeLineageModel newSecondModel)
	{
		AttributeLineageModel oldSecondModel = secondModel;
		secondModel = newSecondModel;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Platform_callPackage.COMPARE_ATTRIBUTE_LINEAGE_MODELS__SECOND_MODEL, oldSecondModel, secondModel));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs)
	{
		switch (featureID)
		{
			case Platform_callPackage.COMPARE_ATTRIBUTE_LINEAGE_MODELS__RESULTING_MODEL:
				return basicSetResultingModel(null, msgs);
			case Platform_callPackage.COMPARE_ATTRIBUTE_LINEAGE_MODELS__SUBSET_BOOLEAN:
				return basicSetSubsetBoolean(null, msgs);
			case Platform_callPackage.COMPARE_ATTRIBUTE_LINEAGE_MODELS__NOT_INCLUDED_MODEL:
				return basicSetNotIncludedModel(null, msgs);
		}
		return super.eInverseRemove(otherEnd, featureID, msgs);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType)
	{
		switch (featureID)
		{
			case Platform_callPackage.COMPARE_ATTRIBUTE_LINEAGE_MODELS__RESULTING_MODEL:
				return getResultingModel();
			case Platform_callPackage.COMPARE_ATTRIBUTE_LINEAGE_MODELS__SUBSET_BOOLEAN:
				return getSubsetBoolean();
			case Platform_callPackage.COMPARE_ATTRIBUTE_LINEAGE_MODELS__NOT_INCLUDED_MODEL:
				return getNotIncludedModel();
			case Platform_callPackage.COMPARE_ATTRIBUTE_LINEAGE_MODELS__FIRST_MODEL:
				if (resolve) return getFirstModel();
				return basicGetFirstModel();
			case Platform_callPackage.COMPARE_ATTRIBUTE_LINEAGE_MODELS__SECOND_MODEL:
				if (resolve) return getSecondModel();
				return basicGetSecondModel();
		}
		return super.eGet(featureID, resolve, coreType);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void eSet(int featureID, Object newValue)
	{
		switch (featureID)
		{
			case Platform_callPackage.COMPARE_ATTRIBUTE_LINEAGE_MODELS__RESULTING_MODEL:
				setResultingModel((AttributeLineageModel)newValue);
				return;
			case Platform_callPackage.COMPARE_ATTRIBUTE_LINEAGE_MODELS__SUBSET_BOOLEAN:
				setSubsetBoolean((SubsetBoolean)newValue);
				return;
			case Platform_callPackage.COMPARE_ATTRIBUTE_LINEAGE_MODELS__NOT_INCLUDED_MODEL:
				setNotIncludedModel((AttributeLineageModel)newValue);
				return;
			case Platform_callPackage.COMPARE_ATTRIBUTE_LINEAGE_MODELS__FIRST_MODEL:
				setFirstModel((AttributeLineageModel)newValue);
				return;
			case Platform_callPackage.COMPARE_ATTRIBUTE_LINEAGE_MODELS__SECOND_MODEL:
				setSecondModel((AttributeLineageModel)newValue);
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
	public void eUnset(int featureID)
	{
		switch (featureID)
		{
			case Platform_callPackage.COMPARE_ATTRIBUTE_LINEAGE_MODELS__RESULTING_MODEL:
				setResultingModel((AttributeLineageModel)null);
				return;
			case Platform_callPackage.COMPARE_ATTRIBUTE_LINEAGE_MODELS__SUBSET_BOOLEAN:
				setSubsetBoolean((SubsetBoolean)null);
				return;
			case Platform_callPackage.COMPARE_ATTRIBUTE_LINEAGE_MODELS__NOT_INCLUDED_MODEL:
				setNotIncludedModel((AttributeLineageModel)null);
				return;
			case Platform_callPackage.COMPARE_ATTRIBUTE_LINEAGE_MODELS__FIRST_MODEL:
				setFirstModel((AttributeLineageModel)null);
				return;
			case Platform_callPackage.COMPARE_ATTRIBUTE_LINEAGE_MODELS__SECOND_MODEL:
				setSecondModel((AttributeLineageModel)null);
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
	public boolean eIsSet(int featureID)
	{
		switch (featureID)
		{
			case Platform_callPackage.COMPARE_ATTRIBUTE_LINEAGE_MODELS__RESULTING_MODEL:
				return resultingModel != null;
			case Platform_callPackage.COMPARE_ATTRIBUTE_LINEAGE_MODELS__SUBSET_BOOLEAN:
				return subsetBoolean != null;
			case Platform_callPackage.COMPARE_ATTRIBUTE_LINEAGE_MODELS__NOT_INCLUDED_MODEL:
				return notIncludedModel != null;
			case Platform_callPackage.COMPARE_ATTRIBUTE_LINEAGE_MODELS__FIRST_MODEL:
				return firstModel != null;
			case Platform_callPackage.COMPARE_ATTRIBUTE_LINEAGE_MODELS__SECOND_MODEL:
				return secondModel != null;
		}
		return super.eIsSet(featureID);
	}

} //CompareAttributeLineageModelsImpl
