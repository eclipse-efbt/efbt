/**
 */
package org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.impl;

import org.eclipse.efbt.openregspecs.model.openregspecs_smcubes_core_extension.*;

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
public class openregspecs_smcubes_core_extensionFactoryImpl extends EFactoryImpl implements openregspecs_smcubes_core_extensionFactory {
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static openregspecs_smcubes_core_extensionFactory init() {
		try {
			openregspecs_smcubes_core_extensionFactory theopenregspecs_smcubes_core_extensionFactory = (openregspecs_smcubes_core_extensionFactory)EPackage.Registry.INSTANCE.getEFactory(openregspecs_smcubes_core_extensionPackage.eNS_URI);
			if (theopenregspecs_smcubes_core_extensionFactory != null) {
				return theopenregspecs_smcubes_core_extensionFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new openregspecs_smcubes_core_extensionFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public openregspecs_smcubes_core_extensionFactoryImpl() {
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
			case openregspecs_smcubes_core_extensionPackage.DOMAIN_MODULE: return createDomainModule();
			case openregspecs_smcubes_core_extensionPackage.MEMBER_HIERARCHY_MODULE: return createMemberHierarchyModule();
			case openregspecs_smcubes_core_extensionPackage.MEMBER_MODULE: return createMemberModule();
			case openregspecs_smcubes_core_extensionPackage.VARIABLE_MODULE: return createVariableModule();
			case openregspecs_smcubes_core_extensionPackage.SUB_DOMAIN_MODULE: return createSubDomainModule();
			case openregspecs_smcubes_core_extensionPackage.SM_CUBES_CORE_MODEL: return createSMCubesCoreModel();
			default:
				throw new IllegalArgumentException("The class '" + eClass.getName() + "' is not a valid classifier");
		}
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public DomainModule createDomainModule() {
		DomainModuleImpl domainModule = new DomainModuleImpl();
		return domainModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public MemberHierarchyModule createMemberHierarchyModule() {
		MemberHierarchyModuleImpl memberHierarchyModule = new MemberHierarchyModuleImpl();
		return memberHierarchyModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public MemberModule createMemberModule() {
		MemberModuleImpl memberModule = new MemberModuleImpl();
		return memberModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VariableModule createVariableModule() {
		VariableModuleImpl variableModule = new VariableModuleImpl();
		return variableModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SubDomainModule createSubDomainModule() {
		SubDomainModuleImpl subDomainModule = new SubDomainModuleImpl();
		return subDomainModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public SMCubesCoreModel createSMCubesCoreModel() {
		SMCubesCoreModelImpl smCubesCoreModel = new SMCubesCoreModelImpl();
		return smCubesCoreModel;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public openregspecs_smcubes_core_extensionPackage getopenregspecs_smcubes_core_extensionPackage() {
		return (openregspecs_smcubes_core_extensionPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static openregspecs_smcubes_core_extensionPackage getPackage() {
		return openregspecs_smcubes_core_extensionPackage.eINSTANCE;
	}

} //openregspecs_smcubes_core_extensionFactoryImpl
