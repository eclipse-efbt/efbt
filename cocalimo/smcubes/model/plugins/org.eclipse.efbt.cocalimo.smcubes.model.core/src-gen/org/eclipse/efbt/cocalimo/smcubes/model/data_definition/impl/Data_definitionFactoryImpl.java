/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.data_definition.impl;

import org.eclipse.efbt.cocalimo.smcubes.model.data_definition.*;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EDataType;
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
public class Data_definitionFactoryImpl extends EFactoryImpl implements Data_definitionFactory
{
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static Data_definitionFactory init()
	{
		try {
			Data_definitionFactory theData_definitionFactory = (Data_definitionFactory)EPackage.Registry.INSTANCE.getEFactory(Data_definitionPackage.eNS_URI);
			if (theData_definitionFactory != null) {
				return theData_definitionFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new Data_definitionFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Data_definitionFactoryImpl()
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
			case Data_definitionPackage.COMBINATION: return createCOMBINATION();
			case Data_definitionPackage.COMBINATION_ITEM: return createCOMBINATION_ITEM();
			case Data_definitionPackage.CUBE: return createCUBE();
			case Data_definitionPackage.CUBE_GROUP: return createCUBE_GROUP();
			case Data_definitionPackage.CUBE_GROUP_ENUMERATION: return createCUBE_GROUP_ENUMERATION();
			case Data_definitionPackage.CUBE_HIERARCHY: return createCUBE_HIERARCHY();
			case Data_definitionPackage.CUBE_HIERARCHY_NODE: return createCUBE_HIERARCHY_NODE();
			case Data_definitionPackage.CUBE_RELATIONSHIP: return createCUBE_RELATIONSHIP();
			case Data_definitionPackage.CUBE_STRUCTURE: return createCUBE_STRUCTURE();
			case Data_definitionPackage.CUBE_STRUCTURE_ITEM: return createCUBE_STRUCTURE_ITEM();
			case Data_definitionPackage.CUBE_TO_COMBINATION: return createCUBE_TO_COMBINATION();
			case Data_definitionPackage.FRAMEWORK: return createFRAMEWORK();
			case Data_definitionPackage.FRAMEWORK_SUBDOMAIN: return createFRAMEWORK_SUBDOMAIN();
			case Data_definitionPackage.FRAMEWORK_VARIABLE_SET: return createFRAMEWORK_VARIABLE_SET();
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
	public Object createFromString(EDataType eDataType, String initialValue)
	{
		switch (eDataType.getClassifierID()) {
			case Data_definitionPackage.TYP_DMNSN:
				return createTYP_DMNSNFromString(eDataType, initialValue);
			case Data_definitionPackage.TYP_RL:
				return createTYP_RLFromString(eDataType, initialValue);
			case Data_definitionPackage.TYP_DMNSN_OBJECT:
				return createTYP_DMNSNObjectFromString(eDataType, initialValue);
			case Data_definitionPackage.TYP_RL_OBJECT:
				return createTYP_RLObjectFromString(eDataType, initialValue);
			default:
				throw new IllegalArgumentException("The datatype '" + eDataType.getName() + "' is not a valid classifier");
		}
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String convertToString(EDataType eDataType, Object instanceValue)
	{
		switch (eDataType.getClassifierID()) {
			case Data_definitionPackage.TYP_DMNSN:
				return convertTYP_DMNSNToString(eDataType, instanceValue);
			case Data_definitionPackage.TYP_RL:
				return convertTYP_RLToString(eDataType, instanceValue);
			case Data_definitionPackage.TYP_DMNSN_OBJECT:
				return convertTYP_DMNSNObjectToString(eDataType, instanceValue);
			case Data_definitionPackage.TYP_RL_OBJECT:
				return convertTYP_RLObjectToString(eDataType, instanceValue);
			default:
				throw new IllegalArgumentException("The datatype '" + eDataType.getName() + "' is not a valid classifier");
		}
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public COMBINATION createCOMBINATION()
	{
		COMBINATIONImpl combination = new COMBINATIONImpl();
		return combination;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public COMBINATION_ITEM createCOMBINATION_ITEM()
	{
		COMBINATION_ITEMImpl combinatioN_ITEM = new COMBINATION_ITEMImpl();
		return combinatioN_ITEM;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public CUBE createCUBE()
	{
		CUBEImpl cube = new CUBEImpl();
		return cube;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public CUBE_GROUP createCUBE_GROUP()
	{
		CUBE_GROUPImpl cubE_GROUP = new CUBE_GROUPImpl();
		return cubE_GROUP;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public CUBE_GROUP_ENUMERATION createCUBE_GROUP_ENUMERATION()
	{
		CUBE_GROUP_ENUMERATIONImpl cubE_GROUP_ENUMERATION = new CUBE_GROUP_ENUMERATIONImpl();
		return cubE_GROUP_ENUMERATION;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public CUBE_HIERARCHY createCUBE_HIERARCHY()
	{
		CUBE_HIERARCHYImpl cubE_HIERARCHY = new CUBE_HIERARCHYImpl();
		return cubE_HIERARCHY;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public CUBE_HIERARCHY_NODE createCUBE_HIERARCHY_NODE()
	{
		CUBE_HIERARCHY_NODEImpl cubE_HIERARCHY_NODE = new CUBE_HIERARCHY_NODEImpl();
		return cubE_HIERARCHY_NODE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public CUBE_RELATIONSHIP createCUBE_RELATIONSHIP()
	{
		CUBE_RELATIONSHIPImpl cubE_RELATIONSHIP = new CUBE_RELATIONSHIPImpl();
		return cubE_RELATIONSHIP;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public CUBE_STRUCTURE createCUBE_STRUCTURE()
	{
		CUBE_STRUCTUREImpl cubE_STRUCTURE = new CUBE_STRUCTUREImpl();
		return cubE_STRUCTURE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public CUBE_STRUCTURE_ITEM createCUBE_STRUCTURE_ITEM()
	{
		CUBE_STRUCTURE_ITEMImpl cubE_STRUCTURE_ITEM = new CUBE_STRUCTURE_ITEMImpl();
		return cubE_STRUCTURE_ITEM;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public CUBE_TO_COMBINATION createCUBE_TO_COMBINATION()
	{
		CUBE_TO_COMBINATIONImpl cubE_TO_COMBINATION = new CUBE_TO_COMBINATIONImpl();
		return cubE_TO_COMBINATION;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public FRAMEWORK createFRAMEWORK()
	{
		FRAMEWORKImpl framework = new FRAMEWORKImpl();
		return framework;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public FRAMEWORK_SUBDOMAIN createFRAMEWORK_SUBDOMAIN()
	{
		FRAMEWORK_SUBDOMAINImpl frameworK_SUBDOMAIN = new FRAMEWORK_SUBDOMAINImpl();
		return frameworK_SUBDOMAIN;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public FRAMEWORK_VARIABLE_SET createFRAMEWORK_VARIABLE_SET()
	{
		FRAMEWORK_VARIABLE_SETImpl frameworK_VARIABLE_SET = new FRAMEWORK_VARIABLE_SETImpl();
		return frameworK_VARIABLE_SET;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TYP_DMNSN createTYP_DMNSNFromString(EDataType eDataType, String initialValue)
	{
		TYP_DMNSN result = TYP_DMNSN.get(initialValue);
		if (result == null) throw new IllegalArgumentException("The value '" + initialValue + "' is not a valid enumerator of '" + eDataType.getName() + "'");
		return result;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String convertTYP_DMNSNToString(EDataType eDataType, Object instanceValue)
	{
		return instanceValue == null ? null : instanceValue.toString();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TYP_RL createTYP_RLFromString(EDataType eDataType, String initialValue)
	{
		TYP_RL result = TYP_RL.get(initialValue);
		if (result == null) throw new IllegalArgumentException("The value '" + initialValue + "' is not a valid enumerator of '" + eDataType.getName() + "'");
		return result;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String convertTYP_RLToString(EDataType eDataType, Object instanceValue)
	{
		return instanceValue == null ? null : instanceValue.toString();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TYP_DMNSN createTYP_DMNSNObjectFromString(EDataType eDataType, String initialValue)
	{
		return createTYP_DMNSNFromString(Data_definitionPackage.Literals.TYP_DMNSN, initialValue);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String convertTYP_DMNSNObjectToString(EDataType eDataType, Object instanceValue)
	{
		return convertTYP_DMNSNToString(Data_definitionPackage.Literals.TYP_DMNSN, instanceValue);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TYP_RL createTYP_RLObjectFromString(EDataType eDataType, String initialValue)
	{
		return createTYP_RLFromString(Data_definitionPackage.Literals.TYP_RL, initialValue);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String convertTYP_RLObjectToString(EDataType eDataType, Object instanceValue)
	{
		return convertTYP_RLToString(Data_definitionPackage.Literals.TYP_RL, instanceValue);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Data_definitionPackage getData_definitionPackage()
	{
		return (Data_definitionPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static Data_definitionPackage getPackage()
	{
		return Data_definitionPackage.eINSTANCE;
	}

} //Data_definitionFactoryImpl
