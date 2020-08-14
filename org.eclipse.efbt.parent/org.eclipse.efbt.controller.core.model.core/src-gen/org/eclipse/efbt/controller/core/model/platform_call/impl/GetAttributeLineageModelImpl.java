/**
 */
package org.eclipse.efbt.controller.core.model.platform_call.impl;

import java.util.Collection;

import org.eclipse.efbt.controller.core.model.platform_call.GetAttributeLineageModel;
import org.eclipse.efbt.controller.core.model.platform_call.Platform_callPackage;

import org.eclipse.efbt.language.trl.model.transformation.VersionedComponentsSet;

import org.eclipse.efbt.lineage.attributelineage.model.attribute_lineage.AttributeLineageModel;

import org.eclipse.efbt.lineage.common.model.lineagefunctions.SpeculativeCubeColumnParameter;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

import org.eclipse.emf.ecore.util.EObjectResolvingEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Get Attribute Lineage Model</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.controller.core.model.platform_call.impl.GetAttributeLineageModelImpl#getResultingModel <em>Resulting Model</em>}</li>
 *   <li>{@link org.eclipse.efbt.controller.core.model.platform_call.impl.GetAttributeLineageModelImpl#getTransformationContext <em>Transformation Context</em>}</li>
 *   <li>{@link org.eclipse.efbt.controller.core.model.platform_call.impl.GetAttributeLineageModelImpl#getUnresolvedCubeColumnParams <em>Unresolved Cube Column Params</em>}</li>
 * </ul>
 *
 * @generated
 */
public class GetAttributeLineageModelImpl extends PlatformCallImpl implements GetAttributeLineageModel
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
	 * The cached value of the '{@link #getTransformationContext() <em>Transformation Context</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTransformationContext()
	 * @generated
	 * @ordered
	 */
	protected VersionedComponentsSet transformationContext;

	/**
	 * The cached value of the '{@link #getUnresolvedCubeColumnParams() <em>Unresolved Cube Column Params</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getUnresolvedCubeColumnParams()
	 * @generated
	 * @ordered
	 */
	protected EList<SpeculativeCubeColumnParameter> unresolvedCubeColumnParams;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected GetAttributeLineageModelImpl()
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
		return Platform_callPackage.Literals.GET_ATTRIBUTE_LINEAGE_MODEL;
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
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Platform_callPackage.GET_ATTRIBUTE_LINEAGE_MODEL__RESULTING_MODEL, oldResultingModel, newResultingModel);
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
				msgs = ((InternalEObject)resultingModel).eInverseRemove(this, EOPPOSITE_FEATURE_BASE - Platform_callPackage.GET_ATTRIBUTE_LINEAGE_MODEL__RESULTING_MODEL, null, msgs);
			if (newResultingModel != null)
				msgs = ((InternalEObject)newResultingModel).eInverseAdd(this, EOPPOSITE_FEATURE_BASE - Platform_callPackage.GET_ATTRIBUTE_LINEAGE_MODEL__RESULTING_MODEL, null, msgs);
			msgs = basicSetResultingModel(newResultingModel, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Platform_callPackage.GET_ATTRIBUTE_LINEAGE_MODEL__RESULTING_MODEL, newResultingModel, newResultingModel));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public VersionedComponentsSet getTransformationContext()
	{
		if (transformationContext != null && transformationContext.eIsProxy())
		{
			InternalEObject oldTransformationContext = (InternalEObject)transformationContext;
			transformationContext = (VersionedComponentsSet)eResolveProxy(oldTransformationContext);
			if (transformationContext != oldTransformationContext)
			{
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Platform_callPackage.GET_ATTRIBUTE_LINEAGE_MODEL__TRANSFORMATION_CONTEXT, oldTransformationContext, transformationContext));
			}
		}
		return transformationContext;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VersionedComponentsSet basicGetTransformationContext()
	{
		return transformationContext;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setTransformationContext(VersionedComponentsSet newTransformationContext)
	{
		VersionedComponentsSet oldTransformationContext = transformationContext;
		transformationContext = newTransformationContext;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Platform_callPackage.GET_ATTRIBUTE_LINEAGE_MODEL__TRANSFORMATION_CONTEXT, oldTransformationContext, transformationContext));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<SpeculativeCubeColumnParameter> getUnresolvedCubeColumnParams()
	{
		if (unresolvedCubeColumnParams == null)
		{
			unresolvedCubeColumnParams = new EObjectResolvingEList<SpeculativeCubeColumnParameter>(SpeculativeCubeColumnParameter.class, this, Platform_callPackage.GET_ATTRIBUTE_LINEAGE_MODEL__UNRESOLVED_CUBE_COLUMN_PARAMS);
		}
		return unresolvedCubeColumnParams;
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
			case Platform_callPackage.GET_ATTRIBUTE_LINEAGE_MODEL__RESULTING_MODEL:
				return basicSetResultingModel(null, msgs);
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
			case Platform_callPackage.GET_ATTRIBUTE_LINEAGE_MODEL__RESULTING_MODEL:
				return getResultingModel();
			case Platform_callPackage.GET_ATTRIBUTE_LINEAGE_MODEL__TRANSFORMATION_CONTEXT:
				if (resolve) return getTransformationContext();
				return basicGetTransformationContext();
			case Platform_callPackage.GET_ATTRIBUTE_LINEAGE_MODEL__UNRESOLVED_CUBE_COLUMN_PARAMS:
				return getUnresolvedCubeColumnParams();
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
	public void eSet(int featureID, Object newValue)
	{
		switch (featureID)
		{
			case Platform_callPackage.GET_ATTRIBUTE_LINEAGE_MODEL__RESULTING_MODEL:
				setResultingModel((AttributeLineageModel)newValue);
				return;
			case Platform_callPackage.GET_ATTRIBUTE_LINEAGE_MODEL__TRANSFORMATION_CONTEXT:
				setTransformationContext((VersionedComponentsSet)newValue);
				return;
			case Platform_callPackage.GET_ATTRIBUTE_LINEAGE_MODEL__UNRESOLVED_CUBE_COLUMN_PARAMS:
				getUnresolvedCubeColumnParams().clear();
				getUnresolvedCubeColumnParams().addAll((Collection<? extends SpeculativeCubeColumnParameter>)newValue);
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
			case Platform_callPackage.GET_ATTRIBUTE_LINEAGE_MODEL__RESULTING_MODEL:
				setResultingModel((AttributeLineageModel)null);
				return;
			case Platform_callPackage.GET_ATTRIBUTE_LINEAGE_MODEL__TRANSFORMATION_CONTEXT:
				setTransformationContext((VersionedComponentsSet)null);
				return;
			case Platform_callPackage.GET_ATTRIBUTE_LINEAGE_MODEL__UNRESOLVED_CUBE_COLUMN_PARAMS:
				getUnresolvedCubeColumnParams().clear();
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
			case Platform_callPackage.GET_ATTRIBUTE_LINEAGE_MODEL__RESULTING_MODEL:
				return resultingModel != null;
			case Platform_callPackage.GET_ATTRIBUTE_LINEAGE_MODEL__TRANSFORMATION_CONTEXT:
				return transformationContext != null;
			case Platform_callPackage.GET_ATTRIBUTE_LINEAGE_MODEL__UNRESOLVED_CUBE_COLUMN_PARAMS:
				return unresolvedCubeColumnParams != null && !unresolvedCubeColumnParams.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //GetAttributeLineageModelImpl
