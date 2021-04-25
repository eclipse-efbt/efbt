/**
 */
package org.eclipse.efbt.cocalimo.core.model.module_management;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;

/**
 * <!-- begin-user-doc -->
 * The <b>Package</b> for the model.
 * It contains accessors for the meta objects to represent
 * <ul>
 *   <li>each class,</li>
 *   <li>each feature of each class,</li>
 *   <li>each operation of each class,</li>
 *   <li>each enum,</li>
 *   <li>and each data type</li>
 * </ul>
 * <!-- end-user-doc -->
 * <!-- begin-model-doc -->
 * This package describes Module Management classes.
 * These classes allow items to be grouped together and given a version. For example a FunctionSpecModule will contain a group of FunctionSpecs.
 * Modules can also state which other modules they depend upon, allowing for fine grained dependencies between groups of items.
 * 
 * <!-- end-model-doc -->
 * @see org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementFactory
 * @model kind="package"
 *        annotation="license license='Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r'"
 * @generated
 */
public interface Module_managementPackage extends EPackage
{
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "module_management";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/efbt/module_management";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "module_management";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Module_managementPackage eINSTANCE = org.eclipse.efbt.cocalimo.core.model.module_management.impl.Module_managementPackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.module_management.impl.ModuleImpl <em>Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.module_management.impl.ModuleImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.module_management.impl.Module_managementPackageImpl#getModule()
	 * @generated
	 */
	int MODULE = 0;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE__DEPENDENCIES = 0;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE__THE_DESCRIPTION = 1;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE__LICENSE = 2;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE__NAME = 3;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE__VERSION = 4;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE__LONG_NAME = 5;

	/**
	 * The number of structural features of the '<em>Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE_FEATURE_COUNT = 6;

	/**
	 * The number of operations of the '<em>Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.module_management.impl.ModuleLongNameImpl <em>Module Long Name</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.module_management.impl.ModuleLongNameImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.module_management.impl.Module_managementPackageImpl#getModuleLongName()
	 * @generated
	 */
	int MODULE_LONG_NAME = 1;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE_LONG_NAME__NAME = 0;

	/**
	 * The number of structural features of the '<em>Module Long Name</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE_LONG_NAME_FEATURE_COUNT = 1;

	/**
	 * The number of operations of the '<em>Module Long Name</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE_LONG_NAME_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.module_management.impl.ModuleDependenciesImpl <em>Module Dependencies</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.module_management.impl.ModuleDependenciesImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.module_management.impl.Module_managementPackageImpl#getModuleDependencies()
	 * @generated
	 */
	int MODULE_DEPENDENCIES = 2;

	/**
	 * The feature id for the '<em><b>The Modules</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE_DEPENDENCIES__THE_MODULES = 0;

	/**
	 * The number of structural features of the '<em>Module Dependencies</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE_DEPENDENCIES_FEATURE_COUNT = 1;

	/**
	 * The number of operations of the '<em>Module Dependencies</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE_DEPENDENCIES_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.core.model.module_management.impl.ModuleDependencyImpl <em>Module Dependency</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.core.model.module_management.impl.ModuleDependencyImpl
	 * @see org.eclipse.efbt.cocalimo.core.model.module_management.impl.Module_managementPackageImpl#getModuleDependency()
	 * @generated
	 */
	int MODULE_DEPENDENCY = 3;

	/**
	 * The feature id for the '<em><b>Module Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE_DEPENDENCY__MODULE_NAME = 0;

	/**
	 * The feature id for the '<em><b>Module Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE_DEPENDENCY__MODULE_VERSION = 1;

	/**
	 * The feature id for the '<em><b>The Module</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE_DEPENDENCY__THE_MODULE = 2;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE_DEPENDENCY__LONG_NAME = 3;

	/**
	 * The number of structural features of the '<em>Module Dependency</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE_DEPENDENCY_FEATURE_COUNT = 4;

	/**
	 * The number of operations of the '<em>Module Dependency</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MODULE_DEPENDENCY_OPERATION_COUNT = 0;


	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.module_management.Module <em>Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.module_management.Module
	 * @generated
	 */
	EClass getModule();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.cocalimo.core.model.module_management.Module#getDependencies <em>Dependencies</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Dependencies</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.module_management.Module#getDependencies()
	 * @see #getModule()
	 * @generated
	 */
	EReference getModule_Dependencies();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.core.model.module_management.Module#getTheDescription <em>The Description</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>The Description</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.module_management.Module#getTheDescription()
	 * @see #getModule()
	 * @generated
	 */
	EAttribute getModule_TheDescription();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.core.model.module_management.Module#getLicense <em>License</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>License</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.module_management.Module#getLicense()
	 * @see #getModule()
	 * @generated
	 */
	EAttribute getModule_License();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.core.model.module_management.Module#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.module_management.Module#getName()
	 * @see #getModule()
	 * @generated
	 */
	EAttribute getModule_Name();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.core.model.module_management.Module#getVersion <em>Version</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Version</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.module_management.Module#getVersion()
	 * @see #getModule()
	 * @generated
	 */
	EAttribute getModule_Version();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.cocalimo.core.model.module_management.Module#getLongName <em>Long Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Long Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.module_management.Module#getLongName()
	 * @see #getModule()
	 * @generated
	 */
	EReference getModule_LongName();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.module_management.ModuleLongName <em>Module Long Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Module Long Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.module_management.ModuleLongName
	 * @generated
	 */
	EClass getModuleLongName();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.core.model.module_management.ModuleLongName#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.module_management.ModuleLongName#getName()
	 * @see #getModuleLongName()
	 * @generated
	 */
	EAttribute getModuleLongName_Name();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.module_management.ModuleDependencies <em>Module Dependencies</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Module Dependencies</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.module_management.ModuleDependencies
	 * @generated
	 */
	EClass getModuleDependencies();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.core.model.module_management.ModuleDependencies#getTheModules <em>The Modules</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>The Modules</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.module_management.ModuleDependencies#getTheModules()
	 * @see #getModuleDependencies()
	 * @generated
	 */
	EReference getModuleDependencies_TheModules();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.core.model.module_management.ModuleDependency <em>Module Dependency</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Module Dependency</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.module_management.ModuleDependency
	 * @generated
	 */
	EClass getModuleDependency();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.core.model.module_management.ModuleDependency#getModuleName <em>Module Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Module Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.module_management.ModuleDependency#getModuleName()
	 * @see #getModuleDependency()
	 * @generated
	 */
	EAttribute getModuleDependency_ModuleName();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.core.model.module_management.ModuleDependency#getModuleVersion <em>Module Version</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Module Version</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.module_management.ModuleDependency#getModuleVersion()
	 * @see #getModuleDependency()
	 * @generated
	 */
	EAttribute getModuleDependency_ModuleVersion();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.core.model.module_management.ModuleDependency#getTheModule <em>The Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>The Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.module_management.ModuleDependency#getTheModule()
	 * @see #getModuleDependency()
	 * @generated
	 */
	EReference getModuleDependency_TheModule();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.core.model.module_management.ModuleDependency#getLongName <em>Long Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Long Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.core.model.module_management.ModuleDependency#getLongName()
	 * @see #getModuleDependency()
	 * @generated
	 */
	EReference getModuleDependency_LongName();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	Module_managementFactory getModule_managementFactory();

	/**
	 * <!-- begin-user-doc -->
	 * Defines literals for the meta objects that represent
	 * <ul>
	 *   <li>each class,</li>
	 *   <li>each feature of each class,</li>
	 *   <li>each operation of each class,</li>
	 *   <li>each enum,</li>
	 *   <li>and each data type</li>
	 * </ul>
	 * <!-- end-user-doc -->
	 * @generated
	 */
	interface Literals
	{
		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.module_management.impl.ModuleImpl <em>Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.module_management.impl.ModuleImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.module_management.impl.Module_managementPackageImpl#getModule()
		 * @generated
		 */
		EClass MODULE = eINSTANCE.getModule();

		/**
		 * The meta object literal for the '<em><b>Dependencies</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MODULE__DEPENDENCIES = eINSTANCE.getModule_Dependencies();

		/**
		 * The meta object literal for the '<em><b>The Description</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MODULE__THE_DESCRIPTION = eINSTANCE.getModule_TheDescription();

		/**
		 * The meta object literal for the '<em><b>License</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MODULE__LICENSE = eINSTANCE.getModule_License();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MODULE__NAME = eINSTANCE.getModule_Name();

		/**
		 * The meta object literal for the '<em><b>Version</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MODULE__VERSION = eINSTANCE.getModule_Version();

		/**
		 * The meta object literal for the '<em><b>Long Name</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MODULE__LONG_NAME = eINSTANCE.getModule_LongName();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.module_management.impl.ModuleLongNameImpl <em>Module Long Name</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.module_management.impl.ModuleLongNameImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.module_management.impl.Module_managementPackageImpl#getModuleLongName()
		 * @generated
		 */
		EClass MODULE_LONG_NAME = eINSTANCE.getModuleLongName();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MODULE_LONG_NAME__NAME = eINSTANCE.getModuleLongName_Name();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.module_management.impl.ModuleDependenciesImpl <em>Module Dependencies</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.module_management.impl.ModuleDependenciesImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.module_management.impl.Module_managementPackageImpl#getModuleDependencies()
		 * @generated
		 */
		EClass MODULE_DEPENDENCIES = eINSTANCE.getModuleDependencies();

		/**
		 * The meta object literal for the '<em><b>The Modules</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MODULE_DEPENDENCIES__THE_MODULES = eINSTANCE.getModuleDependencies_TheModules();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.core.model.module_management.impl.ModuleDependencyImpl <em>Module Dependency</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.core.model.module_management.impl.ModuleDependencyImpl
		 * @see org.eclipse.efbt.cocalimo.core.model.module_management.impl.Module_managementPackageImpl#getModuleDependency()
		 * @generated
		 */
		EClass MODULE_DEPENDENCY = eINSTANCE.getModuleDependency();

		/**
		 * The meta object literal for the '<em><b>Module Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MODULE_DEPENDENCY__MODULE_NAME = eINSTANCE.getModuleDependency_ModuleName();

		/**
		 * The meta object literal for the '<em><b>Module Version</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute MODULE_DEPENDENCY__MODULE_VERSION = eINSTANCE.getModuleDependency_ModuleVersion();

		/**
		 * The meta object literal for the '<em><b>The Module</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MODULE_DEPENDENCY__THE_MODULE = eINSTANCE.getModuleDependency_TheModule();

		/**
		 * The meta object literal for the '<em><b>Long Name</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MODULE_DEPENDENCY__LONG_NAME = eINSTANCE.getModuleDependency_LongName();

	}

} //Module_managementPackage
