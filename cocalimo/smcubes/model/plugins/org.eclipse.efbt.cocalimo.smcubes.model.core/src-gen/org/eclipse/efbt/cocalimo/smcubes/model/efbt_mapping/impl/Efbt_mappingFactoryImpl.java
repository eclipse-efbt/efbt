/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.efbt_mapping.impl;

import org.eclipse.efbt.cocalimo.smcubes.model.efbt_mapping.*;

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
public class Efbt_mappingFactoryImpl extends EFactoryImpl implements Efbt_mappingFactory {
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static Efbt_mappingFactory init() {
		try {
			Efbt_mappingFactory theEfbt_mappingFactory = (Efbt_mappingFactory)EPackage.Registry.INSTANCE.getEFactory(Efbt_mappingPackage.eNS_URI);
			if (theEfbt_mappingFactory != null) {
				return theEfbt_mappingFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new Efbt_mappingFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Efbt_mappingFactoryImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EObject create(EClass eClass) {
		switch (eClass.getClassifierID()) {
			case Efbt_mappingPackage.MAPPING_DEFINITION_MODULE: return createMappingDefinitionModule();
			case Efbt_mappingPackage.CUBE_MAPPING_MODULE: return createCubeMappingModule();
			case Efbt_mappingPackage.MEMBER_MAPPING_MODULE: return createMemberMappingModule();
			case Efbt_mappingPackage.VARIABLE_MAPPING_MODULE: return createVariableMappingModule();
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
	public MappingDefinitionModule createMappingDefinitionModule() {
		MappingDefinitionModuleImpl mappingDefinitionModule = new MappingDefinitionModuleImpl();
		return mappingDefinitionModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public CubeMappingModule createCubeMappingModule() {
		CubeMappingModuleImpl cubeMappingModule = new CubeMappingModuleImpl();
		return cubeMappingModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public MemberMappingModule createMemberMappingModule() {
		MemberMappingModuleImpl memberMappingModule = new MemberMappingModuleImpl();
		return memberMappingModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public VariableMappingModule createVariableMappingModule() {
		VariableMappingModuleImpl variableMappingModule = new VariableMappingModuleImpl();
		return variableMappingModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Efbt_mappingPackage getEfbt_mappingPackage() {
		return (Efbt_mappingPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static Efbt_mappingPackage getPackage() {
		return Efbt_mappingPackage.eINSTANCE;
	}

} //Efbt_mappingFactoryImpl
