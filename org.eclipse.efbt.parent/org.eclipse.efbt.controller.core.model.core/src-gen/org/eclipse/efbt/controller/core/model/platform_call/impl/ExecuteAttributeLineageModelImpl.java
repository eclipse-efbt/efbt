/**
 */
package org.eclipse.efbt.controller.core.model.platform_call.impl;

import org.eclipse.efbt.controller.core.model.platform_call.ExecuteAttributeLineageModel;
import org.eclipse.efbt.controller.core.model.platform_call.Platform_callPackage;

import org.eclipse.efbt.input_data.smcubes.model.base_column_structured_data.InputData;

import org.eclipse.efbt.lineage.attributelineage.model.attribute_lineage.AttributeLineageModel;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Execute Attribute Lineage Model</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.controller.core.model.platform_call.impl.ExecuteAttributeLineageModelImpl#getAttributeLineageModel <em>Attribute Lineage Model</em>}</li>
 *   <li>{@link org.eclipse.efbt.controller.core.model.platform_call.impl.ExecuteAttributeLineageModelImpl#getInputdir <em>Inputdir</em>}</li>
 *   <li>{@link org.eclipse.efbt.controller.core.model.platform_call.impl.ExecuteAttributeLineageModelImpl#getOutputdir <em>Outputdir</em>}</li>
 *   <li>{@link org.eclipse.efbt.controller.core.model.platform_call.impl.ExecuteAttributeLineageModelImpl#getProgramInputs <em>Program Inputs</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ExecuteAttributeLineageModelImpl extends PlatformCallImpl implements ExecuteAttributeLineageModel
{
	/**
	 * The cached value of the '{@link #getAttributeLineageModel() <em>Attribute Lineage Model</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getAttributeLineageModel()
	 * @generated
	 * @ordered
	 */
	protected AttributeLineageModel attributeLineageModel;

	/**
	 * The default value of the '{@link #getInputdir() <em>Inputdir</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getInputdir()
	 * @generated
	 * @ordered
	 */
	protected static final String INPUTDIR_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getInputdir() <em>Inputdir</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getInputdir()
	 * @generated
	 * @ordered
	 */
	protected String inputdir = INPUTDIR_EDEFAULT;

	/**
	 * The default value of the '{@link #getOutputdir() <em>Outputdir</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getOutputdir()
	 * @generated
	 * @ordered
	 */
	protected static final String OUTPUTDIR_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getOutputdir() <em>Outputdir</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getOutputdir()
	 * @generated
	 * @ordered
	 */
	protected String outputdir = OUTPUTDIR_EDEFAULT;

	/**
	 * The cached value of the '{@link #getProgramInputs() <em>Program Inputs</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getProgramInputs()
	 * @generated
	 * @ordered
	 */
	protected InputData programInputs;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ExecuteAttributeLineageModelImpl()
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
		return Platform_callPackage.Literals.EXECUTE_ATTRIBUTE_LINEAGE_MODEL;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public AttributeLineageModel getAttributeLineageModel()
	{
		if (attributeLineageModel != null && attributeLineageModel.eIsProxy())
		{
			InternalEObject oldAttributeLineageModel = (InternalEObject)attributeLineageModel;
			attributeLineageModel = (AttributeLineageModel)eResolveProxy(oldAttributeLineageModel);
			if (attributeLineageModel != oldAttributeLineageModel)
			{
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Platform_callPackage.EXECUTE_ATTRIBUTE_LINEAGE_MODEL__ATTRIBUTE_LINEAGE_MODEL, oldAttributeLineageModel, attributeLineageModel));
			}
		}
		return attributeLineageModel;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public AttributeLineageModel basicGetAttributeLineageModel()
	{
		return attributeLineageModel;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setAttributeLineageModel(AttributeLineageModel newAttributeLineageModel)
	{
		AttributeLineageModel oldAttributeLineageModel = attributeLineageModel;
		attributeLineageModel = newAttributeLineageModel;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Platform_callPackage.EXECUTE_ATTRIBUTE_LINEAGE_MODEL__ATTRIBUTE_LINEAGE_MODEL, oldAttributeLineageModel, attributeLineageModel));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getInputdir()
	{
		return inputdir;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setInputdir(String newInputdir)
	{
		String oldInputdir = inputdir;
		inputdir = newInputdir;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Platform_callPackage.EXECUTE_ATTRIBUTE_LINEAGE_MODEL__INPUTDIR, oldInputdir, inputdir));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getOutputdir()
	{
		return outputdir;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setOutputdir(String newOutputdir)
	{
		String oldOutputdir = outputdir;
		outputdir = newOutputdir;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Platform_callPackage.EXECUTE_ATTRIBUTE_LINEAGE_MODEL__OUTPUTDIR, oldOutputdir, outputdir));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public InputData getProgramInputs()
	{
		if (programInputs != null && programInputs.eIsProxy())
		{
			InternalEObject oldProgramInputs = (InternalEObject)programInputs;
			programInputs = (InputData)eResolveProxy(oldProgramInputs);
			if (programInputs != oldProgramInputs)
			{
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Platform_callPackage.EXECUTE_ATTRIBUTE_LINEAGE_MODEL__PROGRAM_INPUTS, oldProgramInputs, programInputs));
			}
		}
		return programInputs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public InputData basicGetProgramInputs()
	{
		return programInputs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setProgramInputs(InputData newProgramInputs)
	{
		InputData oldProgramInputs = programInputs;
		programInputs = newProgramInputs;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Platform_callPackage.EXECUTE_ATTRIBUTE_LINEAGE_MODEL__PROGRAM_INPUTS, oldProgramInputs, programInputs));
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
			case Platform_callPackage.EXECUTE_ATTRIBUTE_LINEAGE_MODEL__ATTRIBUTE_LINEAGE_MODEL:
				if (resolve) return getAttributeLineageModel();
				return basicGetAttributeLineageModel();
			case Platform_callPackage.EXECUTE_ATTRIBUTE_LINEAGE_MODEL__INPUTDIR:
				return getInputdir();
			case Platform_callPackage.EXECUTE_ATTRIBUTE_LINEAGE_MODEL__OUTPUTDIR:
				return getOutputdir();
			case Platform_callPackage.EXECUTE_ATTRIBUTE_LINEAGE_MODEL__PROGRAM_INPUTS:
				if (resolve) return getProgramInputs();
				return basicGetProgramInputs();
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
			case Platform_callPackage.EXECUTE_ATTRIBUTE_LINEAGE_MODEL__ATTRIBUTE_LINEAGE_MODEL:
				setAttributeLineageModel((AttributeLineageModel)newValue);
				return;
			case Platform_callPackage.EXECUTE_ATTRIBUTE_LINEAGE_MODEL__INPUTDIR:
				setInputdir((String)newValue);
				return;
			case Platform_callPackage.EXECUTE_ATTRIBUTE_LINEAGE_MODEL__OUTPUTDIR:
				setOutputdir((String)newValue);
				return;
			case Platform_callPackage.EXECUTE_ATTRIBUTE_LINEAGE_MODEL__PROGRAM_INPUTS:
				setProgramInputs((InputData)newValue);
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
			case Platform_callPackage.EXECUTE_ATTRIBUTE_LINEAGE_MODEL__ATTRIBUTE_LINEAGE_MODEL:
				setAttributeLineageModel((AttributeLineageModel)null);
				return;
			case Platform_callPackage.EXECUTE_ATTRIBUTE_LINEAGE_MODEL__INPUTDIR:
				setInputdir(INPUTDIR_EDEFAULT);
				return;
			case Platform_callPackage.EXECUTE_ATTRIBUTE_LINEAGE_MODEL__OUTPUTDIR:
				setOutputdir(OUTPUTDIR_EDEFAULT);
				return;
			case Platform_callPackage.EXECUTE_ATTRIBUTE_LINEAGE_MODEL__PROGRAM_INPUTS:
				setProgramInputs((InputData)null);
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
			case Platform_callPackage.EXECUTE_ATTRIBUTE_LINEAGE_MODEL__ATTRIBUTE_LINEAGE_MODEL:
				return attributeLineageModel != null;
			case Platform_callPackage.EXECUTE_ATTRIBUTE_LINEAGE_MODEL__INPUTDIR:
				return INPUTDIR_EDEFAULT == null ? inputdir != null : !INPUTDIR_EDEFAULT.equals(inputdir);
			case Platform_callPackage.EXECUTE_ATTRIBUTE_LINEAGE_MODEL__OUTPUTDIR:
				return OUTPUTDIR_EDEFAULT == null ? outputdir != null : !OUTPUTDIR_EDEFAULT.equals(outputdir);
			case Platform_callPackage.EXECUTE_ATTRIBUTE_LINEAGE_MODEL__PROGRAM_INPUTS:
				return programInputs != null;
		}
		return super.eIsSet(featureID);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String toString()
	{
		if (eIsProxy()) return super.toString();

		StringBuilder result = new StringBuilder(super.toString());
		result.append(" (inputdir: ");
		result.append(inputdir);
		result.append(", outputdir: ");
		result.append(outputdir);
		result.append(')');
		return result.toString();
	}

} //ExecuteAttributeLineageModelImpl
