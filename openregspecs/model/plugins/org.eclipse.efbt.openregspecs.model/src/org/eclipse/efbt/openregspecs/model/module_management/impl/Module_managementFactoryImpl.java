/**
 */
package org.eclipse.efbt.openregspecs.model.module_management.impl;

import org.eclipse.efbt.openregspecs.model.module_management.ModuleDependencies;
import org.eclipse.efbt.openregspecs.model.module_management.ModuleDependency;
import org.eclipse.efbt.openregspecs.model.module_management.ModuleLongName;
import org.eclipse.efbt.openregspecs.model.module_management.Module_managementFactory;
import org.eclipse.efbt.openregspecs.model.module_management.Module_managementPackage;

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
public class Module_managementFactoryImpl extends EFactoryImpl implements Module_managementFactory {
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static Module_managementFactory init() {
		try {
			Module_managementFactory theModule_managementFactory = (Module_managementFactory)EPackage.Registry.INSTANCE.getEFactory(Module_managementPackage.eNS_URI);
			if (theModule_managementFactory != null) {
				return theModule_managementFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new Module_managementFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Module_managementFactoryImpl() {
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
			case Module_managementPackage.MODULE: return createModule();
			case Module_managementPackage.MODULE_LONG_NAME: return createModuleLongName();
			case Module_managementPackage.MODULE_DEPENDENCIES: return createModuleDependencies();
			case Module_managementPackage.MODULE_DEPENDENCY: return createModuleDependency();
			default:
				throw new IllegalArgumentException("The class '" + eClass.getName() + "' is not a valid classifier");
		}
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public org.eclipse.efbt.openregspecs.model.module_management.Module createModule() {
		ModuleImpl module = new ModuleImpl();
		return module;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ModuleLongName createModuleLongName() {
		ModuleLongNameImpl moduleLongName = new ModuleLongNameImpl();
		return moduleLongName;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ModuleDependencies createModuleDependencies() {
		ModuleDependenciesImpl moduleDependencies = new ModuleDependenciesImpl();
		return moduleDependencies;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ModuleDependency createModuleDependency() {
		ModuleDependencyImpl moduleDependency = new ModuleDependencyImpl();
		return moduleDependency;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Module_managementPackage getModule_managementPackage() {
		return (Module_managementPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static Module_managementPackage getPackage() {
		return Module_managementPackage.eINSTANCE;
	}

} //Module_managementFactoryImpl
