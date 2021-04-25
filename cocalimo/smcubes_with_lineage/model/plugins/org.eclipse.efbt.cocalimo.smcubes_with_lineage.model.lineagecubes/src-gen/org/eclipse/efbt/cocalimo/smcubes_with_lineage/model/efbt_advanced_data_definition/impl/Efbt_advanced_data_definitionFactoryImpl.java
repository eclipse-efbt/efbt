/**
 */
package org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.impl;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.*;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;

import org.eclipse.emf.ecore.impl.EFactoryImpl;

import org.eclipse.emf.ecore.plugin.EcorePlugin;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model <b>Factory</b>.
 * <!-- end-user-doc -->
 * @generated
 */
public class Efbt_advanced_data_definitionFactoryImpl extends EFactoryImpl implements Efbt_advanced_data_definitionFactory
{
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static Efbt_advanced_data_definitionFactory init()
	{
		try {
			Efbt_advanced_data_definitionFactory theEfbt_advanced_data_definitionFactory = (Efbt_advanced_data_definitionFactory)EPackage.Registry.INSTANCE.getEFactory(Efbt_advanced_data_definitionPackage.eNS_URI);
			if (theEfbt_advanced_data_definitionFactory != null) {
				return theEfbt_advanced_data_definitionFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new Efbt_advanced_data_definitionFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Efbt_advanced_data_definitionFactoryImpl()
	{
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EObject create(EClass eClass)
	{
		switch (eClass.getClassifierID()) {
			case Efbt_advanced_data_definitionPackage.ADVANCED_TYPED_VARIABLE: return createAdvancedTypedVariable();
			case Efbt_advanced_data_definitionPackage.ARRAY_TYPED_VARIABLE: return createArrayTypedVariable();
			case Efbt_advanced_data_definitionPackage.ARRAY_TYPED_VARIABLE_MODULE: return createArrayTypedVariableModule();
			case Efbt_advanced_data_definitionPackage.STRUCT_TYPED_VARIABLE: return createStructTypedVariable();
			case Efbt_advanced_data_definitionPackage.STRUCT_TYPED_VARIABLES_MODULE: return createStructTypedVariablesModule();
			default:
				throw new IllegalArgumentException("The class '" + eClass.getName() + "' is not a valid classifier");
		}
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public AdvancedTypedVariable createAdvancedTypedVariable()
	{
		AdvancedTypedVariableImpl advancedTypedVariable = new AdvancedTypedVariableImpl();
		return advancedTypedVariable;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ArrayTypedVariable createArrayTypedVariable()
	{
		ArrayTypedVariableImpl arrayTypedVariable = new ArrayTypedVariableImpl();
		return arrayTypedVariable;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ArrayTypedVariableModule createArrayTypedVariableModule()
	{
		ArrayTypedVariableModuleImpl arrayTypedVariableModule = new ArrayTypedVariableModuleImpl();
		return arrayTypedVariableModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public StructTypedVariable createStructTypedVariable()
	{
		StructTypedVariableImpl structTypedVariable = new StructTypedVariableImpl();
		return structTypedVariable;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public StructTypedVariablesModule createStructTypedVariablesModule()
	{
		StructTypedVariablesModuleImpl structTypedVariablesModule = new StructTypedVariablesModuleImpl();
		return structTypedVariablesModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Efbt_advanced_data_definitionPackage getEfbt_advanced_data_definitionPackage()
	{
		return (Efbt_advanced_data_definitionPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static Efbt_advanced_data_definitionPackage getPackage()
	{
		return Efbt_advanced_data_definitionPackage.eINSTANCE;
	}

} //Efbt_advanced_data_definitionFactoryImpl
