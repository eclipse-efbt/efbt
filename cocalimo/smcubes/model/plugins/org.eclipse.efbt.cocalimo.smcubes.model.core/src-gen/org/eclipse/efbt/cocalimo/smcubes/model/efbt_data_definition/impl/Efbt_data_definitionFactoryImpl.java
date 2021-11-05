/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.impl;

import org.eclipse.efbt.cocalimo.smcubes.model.efbt_data_definition.*;

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
public class Efbt_data_definitionFactoryImpl extends EFactoryImpl implements Efbt_data_definitionFactory {
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static Efbt_data_definitionFactory init() {
		try {
			Efbt_data_definitionFactory theEfbt_data_definitionFactory = (Efbt_data_definitionFactory)EPackage.Registry.INSTANCE.getEFactory(Efbt_data_definitionPackage.eNS_URI);
			if (theEfbt_data_definitionFactory != null) {
				return theEfbt_data_definitionFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new Efbt_data_definitionFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Efbt_data_definitionFactoryImpl() {
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
			case Efbt_data_definitionPackage.COMBINATION_MODULE: return createCombinationModule();
			case Efbt_data_definitionPackage.CUBE_MODULE: return createCubeModule();
			case Efbt_data_definitionPackage.DOMAIN_MODULE: return createDomainModule();
			case Efbt_data_definitionPackage.MEMBER_HIERARCHY_MODULE: return createMemberHierarchyModule();
			case Efbt_data_definitionPackage.MEMBER_MODULE: return createMemberModule();
			case Efbt_data_definitionPackage.VARIABLE_MODULE: return createVariableModule();
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
	public CombinationModule createCombinationModule() {
		CombinationModuleImpl combinationModule = new CombinationModuleImpl();
		return combinationModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public CubeModule createCubeModule() {
		CubeModuleImpl cubeModule = new CubeModuleImpl();
		return cubeModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public DomainModule createDomainModule() {
		DomainModuleImpl domainModule = new DomainModuleImpl();
		return domainModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public MemberHierarchyModule createMemberHierarchyModule() {
		MemberHierarchyModuleImpl memberHierarchyModule = new MemberHierarchyModuleImpl();
		return memberHierarchyModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public MemberModule createMemberModule() {
		MemberModuleImpl memberModule = new MemberModuleImpl();
		return memberModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public VariableModule createVariableModule() {
		VariableModuleImpl variableModule = new VariableModuleImpl();
		return variableModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Efbt_data_definitionPackage getEfbt_data_definitionPackage() {
		return (Efbt_data_definitionPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static Efbt_data_definitionPackage getPackage() {
		return Efbt_data_definitionPackage.eINSTANCE;
	}

} //Efbt_data_definitionFactoryImpl
