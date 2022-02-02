/**
 */
package attribute_lineage;

import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage;

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
 *  This package describes the AttributeLineageModel
 * <!-- end-model-doc -->
 * @see attribute_lineage.Attribute_lineageFactory
 * @model kind="package"
 *        annotation="license license='Copyright (c) 2020 Bird Software Solutions Ltd\n All rights reserved. This file and the accompanying materials are made available under the terms of the Eclipse Public License v2.0 which accompanies this distribution, and is available at http://www.eclipse.org/legal/epl-v20.html\n\nSPDX-License-Identifier: EPL-2.0 \n\n Contributors:\n Neil Mackenzie - initial API and implementation\r'"
 * @generated
 */
public interface Attribute_lineagePackage extends EPackage {
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "attribute_lineage";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/efbt/attribute_lineage";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "attribute_lineage";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Attribute_lineagePackage eINSTANCE = attribute_lineage.impl.Attribute_lineagePackageImpl.init();

	/**
	 * The meta object id for the '{@link attribute_lineage.impl.AttributeLineageModelImpl <em>Attribute Lineage Model</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see attribute_lineage.impl.AttributeLineageModelImpl
	 * @see attribute_lineage.impl.Attribute_lineagePackageImpl#getAttributeLineageModel()
	 * @generated
	 */
	int ATTRIBUTE_LINEAGE_MODEL = 0;

	/**
	 * The feature id for the '<em><b>Row Transformations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_LINEAGE_MODEL__ROW_TRANSFORMATIONS = 0;

	/**
	 * The feature id for the '<em><b>Base Schemas</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_LINEAGE_MODEL__BASE_SCHEMAS = 1;

	/**
	 * The feature id for the '<em><b>Special Function Specs</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_LINEAGE_MODEL__SPECIAL_FUNCTION_SPECS = 2;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_LINEAGE_MODEL__NAME = 3;

	/**
	 * The number of structural features of the '<em>Attribute Lineage Model</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_LINEAGE_MODEL_FEATURE_COUNT = 4;

	/**
	 * The number of operations of the '<em>Attribute Lineage Model</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_LINEAGE_MODEL_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link attribute_lineage.impl.AttributeLineageModelModuleImpl <em>Attribute Lineage Model Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see attribute_lineage.impl.AttributeLineageModelModuleImpl
	 * @see attribute_lineage.impl.Attribute_lineagePackageImpl#getAttributeLineageModelModule()
	 * @generated
	 */
	int ATTRIBUTE_LINEAGE_MODEL_MODULE = 1;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_LINEAGE_MODEL_MODULE__DEPENDENCIES = Module_managementPackage.MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_LINEAGE_MODEL_MODULE__THE_DESCRIPTION = Module_managementPackage.MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_LINEAGE_MODEL_MODULE__LICENSE = Module_managementPackage.MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_LINEAGE_MODEL_MODULE__NAME = Module_managementPackage.MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_LINEAGE_MODEL_MODULE__VERSION = Module_managementPackage.MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_LINEAGE_MODEL_MODULE__LONG_NAME = Module_managementPackage.MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Attribute Lineage Models</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_LINEAGE_MODEL_MODULE__ATTRIBUTE_LINEAGE_MODELS = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Attribute Lineage Model Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_LINEAGE_MODEL_MODULE_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Attribute Lineage Model Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_LINEAGE_MODEL_MODULE_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link attribute_lineage.impl.SubsetBooleanImpl <em>Subset Boolean</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see attribute_lineage.impl.SubsetBooleanImpl
	 * @see attribute_lineage.impl.Attribute_lineagePackageImpl#getSubsetBoolean()
	 * @generated
	 */
	int SUBSET_BOOLEAN = 4;

	/**
	 * The number of structural features of the '<em>Subset Boolean</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUBSET_BOOLEAN_FEATURE_COUNT = 0;

	/**
	 * The number of operations of the '<em>Subset Boolean</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SUBSET_BOOLEAN_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link attribute_lineage.impl.IsNotSubsetImpl <em>Is Not Subset</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see attribute_lineage.impl.IsNotSubsetImpl
	 * @see attribute_lineage.impl.Attribute_lineagePackageImpl#getIsNotSubset()
	 * @generated
	 */
	int IS_NOT_SUBSET = 2;

	/**
	 * The number of structural features of the '<em>Is Not Subset</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int IS_NOT_SUBSET_FEATURE_COUNT = SUBSET_BOOLEAN_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Is Not Subset</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int IS_NOT_SUBSET_OPERATION_COUNT = SUBSET_BOOLEAN_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link attribute_lineage.impl.IsSubsetImpl <em>Is Subset</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see attribute_lineage.impl.IsSubsetImpl
	 * @see attribute_lineage.impl.Attribute_lineagePackageImpl#getIsSubset()
	 * @generated
	 */
	int IS_SUBSET = 3;

	/**
	 * The number of structural features of the '<em>Is Subset</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int IS_SUBSET_FEATURE_COUNT = SUBSET_BOOLEAN_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Is Subset</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int IS_SUBSET_OPERATION_COUNT = SUBSET_BOOLEAN_OPERATION_COUNT + 0;


	/**
	 * Returns the meta object for class '{@link attribute_lineage.AttributeLineageModel <em>Attribute Lineage Model</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Attribute Lineage Model</em>'.
	 * @see attribute_lineage.AttributeLineageModel
	 * @generated
	 */
	EClass getAttributeLineageModel();

	/**
	 * Returns the meta object for the containment reference list '{@link attribute_lineage.AttributeLineageModel#getRowTransformations <em>Row Transformations</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Row Transformations</em>'.
	 * @see attribute_lineage.AttributeLineageModel#getRowTransformations()
	 * @see #getAttributeLineageModel()
	 * @generated
	 */
	EReference getAttributeLineageModel_RowTransformations();

	/**
	 * Returns the meta object for the containment reference list '{@link attribute_lineage.AttributeLineageModel#getBaseSchemas <em>Base Schemas</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Base Schemas</em>'.
	 * @see attribute_lineage.AttributeLineageModel#getBaseSchemas()
	 * @see #getAttributeLineageModel()
	 * @generated
	 */
	EReference getAttributeLineageModel_BaseSchemas();

	/**
	 * Returns the meta object for the containment reference list '{@link attribute_lineage.AttributeLineageModel#getSpecialFunctionSpecs <em>Special Function Specs</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Special Function Specs</em>'.
	 * @see attribute_lineage.AttributeLineageModel#getSpecialFunctionSpecs()
	 * @see #getAttributeLineageModel()
	 * @generated
	 */
	EReference getAttributeLineageModel_SpecialFunctionSpecs();

	/**
	 * Returns the meta object for the attribute '{@link attribute_lineage.AttributeLineageModel#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see attribute_lineage.AttributeLineageModel#getName()
	 * @see #getAttributeLineageModel()
	 * @generated
	 */
	EAttribute getAttributeLineageModel_Name();

	/**
	 * Returns the meta object for class '{@link attribute_lineage.AttributeLineageModelModule <em>Attribute Lineage Model Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Attribute Lineage Model Module</em>'.
	 * @see attribute_lineage.AttributeLineageModelModule
	 * @generated
	 */
	EClass getAttributeLineageModelModule();

	/**
	 * Returns the meta object for the containment reference list '{@link attribute_lineage.AttributeLineageModelModule#getAttributeLineageModels <em>Attribute Lineage Models</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Attribute Lineage Models</em>'.
	 * @see attribute_lineage.AttributeLineageModelModule#getAttributeLineageModels()
	 * @see #getAttributeLineageModelModule()
	 * @generated
	 */
	EReference getAttributeLineageModelModule_AttributeLineageModels();

	/**
	 * Returns the meta object for class '{@link attribute_lineage.IsNotSubset <em>Is Not Subset</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Is Not Subset</em>'.
	 * @see attribute_lineage.IsNotSubset
	 * @generated
	 */
	EClass getIsNotSubset();

	/**
	 * Returns the meta object for class '{@link attribute_lineage.IsSubset <em>Is Subset</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Is Subset</em>'.
	 * @see attribute_lineage.IsSubset
	 * @generated
	 */
	EClass getIsSubset();

	/**
	 * Returns the meta object for class '{@link attribute_lineage.SubsetBoolean <em>Subset Boolean</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Subset Boolean</em>'.
	 * @see attribute_lineage.SubsetBoolean
	 * @generated
	 */
	EClass getSubsetBoolean();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	Attribute_lineageFactory getAttribute_lineageFactory();

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
	interface Literals {
		/**
		 * The meta object literal for the '{@link attribute_lineage.impl.AttributeLineageModelImpl <em>Attribute Lineage Model</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see attribute_lineage.impl.AttributeLineageModelImpl
		 * @see attribute_lineage.impl.Attribute_lineagePackageImpl#getAttributeLineageModel()
		 * @generated
		 */
		EClass ATTRIBUTE_LINEAGE_MODEL = eINSTANCE.getAttributeLineageModel();

		/**
		 * The meta object literal for the '<em><b>Row Transformations</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ATTRIBUTE_LINEAGE_MODEL__ROW_TRANSFORMATIONS = eINSTANCE.getAttributeLineageModel_RowTransformations();

		/**
		 * The meta object literal for the '<em><b>Base Schemas</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ATTRIBUTE_LINEAGE_MODEL__BASE_SCHEMAS = eINSTANCE.getAttributeLineageModel_BaseSchemas();

		/**
		 * The meta object literal for the '<em><b>Special Function Specs</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ATTRIBUTE_LINEAGE_MODEL__SPECIAL_FUNCTION_SPECS = eINSTANCE.getAttributeLineageModel_SpecialFunctionSpecs();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute ATTRIBUTE_LINEAGE_MODEL__NAME = eINSTANCE.getAttributeLineageModel_Name();

		/**
		 * The meta object literal for the '{@link attribute_lineage.impl.AttributeLineageModelModuleImpl <em>Attribute Lineage Model Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see attribute_lineage.impl.AttributeLineageModelModuleImpl
		 * @see attribute_lineage.impl.Attribute_lineagePackageImpl#getAttributeLineageModelModule()
		 * @generated
		 */
		EClass ATTRIBUTE_LINEAGE_MODEL_MODULE = eINSTANCE.getAttributeLineageModelModule();

		/**
		 * The meta object literal for the '<em><b>Attribute Lineage Models</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ATTRIBUTE_LINEAGE_MODEL_MODULE__ATTRIBUTE_LINEAGE_MODELS = eINSTANCE.getAttributeLineageModelModule_AttributeLineageModels();

		/**
		 * The meta object literal for the '{@link attribute_lineage.impl.IsNotSubsetImpl <em>Is Not Subset</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see attribute_lineage.impl.IsNotSubsetImpl
		 * @see attribute_lineage.impl.Attribute_lineagePackageImpl#getIsNotSubset()
		 * @generated
		 */
		EClass IS_NOT_SUBSET = eINSTANCE.getIsNotSubset();

		/**
		 * The meta object literal for the '{@link attribute_lineage.impl.IsSubsetImpl <em>Is Subset</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see attribute_lineage.impl.IsSubsetImpl
		 * @see attribute_lineage.impl.Attribute_lineagePackageImpl#getIsSubset()
		 * @generated
		 */
		EClass IS_SUBSET = eINSTANCE.getIsSubset();

		/**
		 * The meta object literal for the '{@link attribute_lineage.impl.SubsetBooleanImpl <em>Subset Boolean</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see attribute_lineage.impl.SubsetBooleanImpl
		 * @see attribute_lineage.impl.Attribute_lineagePackageImpl#getSubsetBoolean()
		 * @generated
		 */
		EClass SUBSET_BOOLEAN = eINSTANCE.getSubsetBoolean();

	}

} //Attribute_lineagePackage
