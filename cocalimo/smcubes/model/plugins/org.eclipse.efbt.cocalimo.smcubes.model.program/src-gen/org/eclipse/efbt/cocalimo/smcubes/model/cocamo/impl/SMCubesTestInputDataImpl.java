/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.cocamo.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.core.model.test_input_data.impl.TestInputDataImpl;

import org.eclipse.efbt.cocalimo.smcubes.model.base_column_structured_data.BaseColumnStructuredData;

import org.eclipse.efbt.cocalimo.smcubes.model.cocamo.CocamoPackage;
import org.eclipse.efbt.cocalimo.smcubes.model.cocamo.SMCubesTestInputData;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>SM Cubes Test Input Data</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.smcubes.model.cocamo.impl.SMCubesTestInputDataImpl#getSmcubes_inputdata <em>Smcubes inputdata</em>}</li>
 * </ul>
 *
 * @generated
 */
public class SMCubesTestInputDataImpl extends TestInputDataImpl implements SMCubesTestInputData
{
	/**
	 * The cached value of the '{@link #getSmcubes_inputdata() <em>Smcubes inputdata</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSmcubes_inputdata()
	 * @generated
	 * @ordered
	 */
	protected EList<BaseColumnStructuredData> smcubes_inputdata;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected SMCubesTestInputDataImpl()
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
		return CocamoPackage.Literals.SM_CUBES_TEST_INPUT_DATA;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<BaseColumnStructuredData> getSmcubes_inputdata()
	{
		if (smcubes_inputdata == null) {
			smcubes_inputdata = new EObjectContainmentEList<BaseColumnStructuredData>(BaseColumnStructuredData.class, this, CocamoPackage.SM_CUBES_TEST_INPUT_DATA__SMCUBES_INPUTDATA);
		}
		return smcubes_inputdata;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs)
	{
		switch (featureID) {
			case CocamoPackage.SM_CUBES_TEST_INPUT_DATA__SMCUBES_INPUTDATA:
				return ((InternalEList<?>)getSmcubes_inputdata()).basicRemove(otherEnd, msgs);
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
		switch (featureID) {
			case CocamoPackage.SM_CUBES_TEST_INPUT_DATA__SMCUBES_INPUTDATA:
				return getSmcubes_inputdata();
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
		switch (featureID) {
			case CocamoPackage.SM_CUBES_TEST_INPUT_DATA__SMCUBES_INPUTDATA:
				getSmcubes_inputdata().clear();
				getSmcubes_inputdata().addAll((Collection<? extends BaseColumnStructuredData>)newValue);
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
		switch (featureID) {
			case CocamoPackage.SM_CUBES_TEST_INPUT_DATA__SMCUBES_INPUTDATA:
				getSmcubes_inputdata().clear();
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
		switch (featureID) {
			case CocamoPackage.SM_CUBES_TEST_INPUT_DATA__SMCUBES_INPUTDATA:
				return smcubes_inputdata != null && !smcubes_inputdata.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //SMCubesTestInputDataImpl
