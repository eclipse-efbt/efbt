/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.mapping.util;

import org.eclipse.efbt.cocalimo.smcubes.model.mapping.*;

import org.eclipse.emf.common.notify.Adapter;
import org.eclipse.emf.common.notify.Notifier;

import org.eclipse.emf.common.notify.impl.AdapterFactoryImpl;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * The <b>Adapter Factory</b> for the model.
 * It provides an adapter <code>createXXX</code> method for each class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MappingPackage
 * @generated
 */
public class MappingAdapterFactory extends AdapterFactoryImpl
{
	/**
	 * The cached model package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static MappingPackage modelPackage;

	/**
	 * Creates an instance of the adapter factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public MappingAdapterFactory()
	{
		if (modelPackage == null) {
			modelPackage = MappingPackage.eINSTANCE;
		}
	}

	/**
	 * Returns whether this factory is applicable for the type of the object.
	 * <!-- begin-user-doc -->
	 * This implementation returns <code>true</code> if the object is either the model's package or is an instance object of the model.
	 * <!-- end-user-doc -->
	 * @return whether this factory is applicable for the type of the object.
	 * @generated
	 */
	@Override
	public boolean isFactoryForType(Object object)
	{
		if (object == modelPackage) {
			return true;
		}
		if (object instanceof EObject) {
			return ((EObject)object).eClass().getEPackage() == modelPackage;
		}
		return false;
	}

	/**
	 * The switch that delegates to the <code>createXXX</code> methods.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected MappingSwitch<Adapter> modelSwitch =
		new MappingSwitch<Adapter>() {
			@Override
			public Adapter caseCUBE_MAPPING(CUBE_MAPPING object) {
				return createCUBE_MAPPINGAdapter();
			}
			@Override
			public Adapter caseMEMBER_MAPPING(MEMBER_MAPPING object) {
				return createMEMBER_MAPPINGAdapter();
			}
			@Override
			public Adapter caseMEMBER_MAPPING_ITEM(MEMBER_MAPPING_ITEM object) {
				return createMEMBER_MAPPING_ITEMAdapter();
			}
			@Override
			public Adapter caseVARIABLE_MAPPING_ITEM(VARIABLE_MAPPING_ITEM object) {
				return createVARIABLE_MAPPING_ITEMAdapter();
			}
			@Override
			public Adapter caseVARIABLE_MAPPING(VARIABLE_MAPPING object) {
				return createVARIABLE_MAPPINGAdapter();
			}
			@Override
			public Adapter caseMAPPING_TO_CUBE(MAPPING_TO_CUBE object) {
				return createMAPPING_TO_CUBEAdapter();
			}
			@Override
			public Adapter caseVARIABLE_SET_MAPPING(VARIABLE_SET_MAPPING object) {
				return createVARIABLE_SET_MAPPINGAdapter();
			}
			@Override
			public Adapter caseMAPPING_DEFINITION(MAPPING_DEFINITION object) {
				return createMAPPING_DEFINITIONAdapter();
			}
			@Override
			public Adapter defaultCase(EObject object) {
				return createEObjectAdapter();
			}
		};

	/**
	 * Creates an adapter for the <code>target</code>.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param target the object to adapt.
	 * @return the adapter for the <code>target</code>.
	 * @generated
	 */
	@Override
	public Adapter createAdapter(Notifier target)
	{
		return modelSwitch.doSwitch((EObject)target);
	}


	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.CUBE_MAPPING <em>CUBE MAPPING</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.CUBE_MAPPING
	 * @generated
	 */
	public Adapter createCUBE_MAPPINGAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MEMBER_MAPPING <em>MEMBER MAPPING</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MEMBER_MAPPING
	 * @generated
	 */
	public Adapter createMEMBER_MAPPINGAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MEMBER_MAPPING_ITEM <em>MEMBER MAPPING ITEM</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MEMBER_MAPPING_ITEM
	 * @generated
	 */
	public Adapter createMEMBER_MAPPING_ITEMAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_MAPPING_ITEM <em>VARIABLE MAPPING ITEM</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_MAPPING_ITEM
	 * @generated
	 */
	public Adapter createVARIABLE_MAPPING_ITEMAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_MAPPING <em>VARIABLE MAPPING</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_MAPPING
	 * @generated
	 */
	public Adapter createVARIABLE_MAPPINGAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_TO_CUBE <em>MAPPING TO CUBE</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_TO_CUBE
	 * @generated
	 */
	public Adapter createMAPPING_TO_CUBEAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_SET_MAPPING <em>VARIABLE SET MAPPING</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.VARIABLE_SET_MAPPING
	 * @generated
	 */
	public Adapter createVARIABLE_SET_MAPPINGAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_DEFINITION <em>MAPPING DEFINITION</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.mapping.MAPPING_DEFINITION
	 * @generated
	 */
	public Adapter createMAPPING_DEFINITIONAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for the default case.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @generated
	 */
	public Adapter createEObjectAdapter()
	{
		return null;
	}

} //MappingAdapterFactory
