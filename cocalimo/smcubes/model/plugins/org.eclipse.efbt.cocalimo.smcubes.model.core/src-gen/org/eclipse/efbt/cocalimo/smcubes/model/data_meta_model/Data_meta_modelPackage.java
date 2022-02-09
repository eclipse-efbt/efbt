/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model;

import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EDataType;
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
 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Data_meta_modelFactory
 * @model kind="package"
 * @generated
 */
public interface Data_meta_modelPackage extends EPackage {
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "data_meta_model";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/efbt/data_meta_model";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "data_meta_model";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Data_meta_modelPackage eINSTANCE = org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.Data_meta_modelPackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.ElementImpl <em>Element</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.ElementImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.Data_meta_modelPackageImpl#getElement()
	 * @generated
	 */
	int ELEMENT = 3;

	/**
	 * The number of structural features of the '<em>Element</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ELEMENT_FEATURE_COUNT = 0;

	/**
	 * The number of operations of the '<em>Element</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ELEMENT_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.NamedElementImpl <em>Named Element</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.NamedElementImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.Data_meta_modelPackageImpl#getNamedElement()
	 * @generated
	 */
	int NAMED_ELEMENT = 4;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int NAMED_ELEMENT__NAME = ELEMENT_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Named Element</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int NAMED_ELEMENT_FEATURE_COUNT = ELEMENT_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Named Element</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int NAMED_ELEMENT_OPERATION_COUNT = ELEMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.TypedElementImpl <em>Typed Element</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.TypedElementImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.Data_meta_modelPackageImpl#getTypedElement()
	 * @generated
	 */
	int TYPED_ELEMENT = 7;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPED_ELEMENT__NAME = NAMED_ELEMENT__NAME;

	/**
	 * The feature id for the '<em><b>Classifier</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPED_ELEMENT__CLASSIFIER = NAMED_ELEMENT_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Typed Element</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPED_ELEMENT_FEATURE_COUNT = NAMED_ELEMENT_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Typed Element</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int TYPED_ELEMENT_OPERATION_COUNT = NAMED_ELEMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.StructuralFeatureImpl <em>Structural Feature</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.StructuralFeatureImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.Data_meta_modelPackageImpl#getStructuralFeature()
	 * @generated
	 */
	int STRUCTURAL_FEATURE = 6;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int STRUCTURAL_FEATURE__NAME = TYPED_ELEMENT__NAME;

	/**
	 * The feature id for the '<em><b>Classifier</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int STRUCTURAL_FEATURE__CLASSIFIER = TYPED_ELEMENT__CLASSIFIER;

	/**
	 * The number of structural features of the '<em>Structural Feature</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int STRUCTURAL_FEATURE_FEATURE_COUNT = TYPED_ELEMENT_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Structural Feature</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int STRUCTURAL_FEATURE_OPERATION_COUNT = TYPED_ELEMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.AttributeImpl <em>Attribute</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.AttributeImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.Data_meta_modelPackageImpl#getAttribute()
	 * @generated
	 */
	int ATTRIBUTE = 0;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE__NAME = STRUCTURAL_FEATURE__NAME;

	/**
	 * The feature id for the '<em><b>Classifier</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE__CLASSIFIER = STRUCTURAL_FEATURE__CLASSIFIER;

	/**
	 * The feature id for the '<em><b>Is PK</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE__IS_PK = STRUCTURAL_FEATURE_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE__VARIABLE = STRUCTURAL_FEATURE_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Attribute</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_FEATURE_COUNT = STRUCTURAL_FEATURE_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Attribute</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_OPERATION_COUNT = STRUCTURAL_FEATURE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.ForeignKeyAttributeImpl <em>Foreign Key Attribute</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.ForeignKeyAttributeImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.Data_meta_modelPackageImpl#getForeignKeyAttribute()
	 * @generated
	 */
	int FOREIGN_KEY_ATTRIBUTE = 1;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FOREIGN_KEY_ATTRIBUTE__NAME = ATTRIBUTE__NAME;

	/**
	 * The feature id for the '<em><b>Classifier</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FOREIGN_KEY_ATTRIBUTE__CLASSIFIER = ATTRIBUTE__CLASSIFIER;

	/**
	 * The feature id for the '<em><b>Is PK</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FOREIGN_KEY_ATTRIBUTE__IS_PK = ATTRIBUTE__IS_PK;

	/**
	 * The feature id for the '<em><b>Variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FOREIGN_KEY_ATTRIBUTE__VARIABLE = ATTRIBUTE__VARIABLE;

	/**
	 * The feature id for the '<em><b>Entity</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FOREIGN_KEY_ATTRIBUTE__ENTITY = ATTRIBUTE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Foreign Key Attribute</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FOREIGN_KEY_ATTRIBUTE_FEATURE_COUNT = ATTRIBUTE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Foreign Key Attribute</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FOREIGN_KEY_ATTRIBUTE_OPERATION_COUNT = ATTRIBUTE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.ClassifierImpl <em>Classifier</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.ClassifierImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.Data_meta_modelPackageImpl#getClassifier()
	 * @generated
	 */
	int CLASSIFIER = 2;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASSIFIER__NAME = NAMED_ELEMENT__NAME;

	/**
	 * The number of structural features of the '<em>Classifier</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASSIFIER_FEATURE_COUNT = NAMED_ELEMENT_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Classifier</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CLASSIFIER_OPERATION_COUNT = NAMED_ELEMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.EntityModuleImpl <em>Entity Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.EntityModuleImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.Data_meta_modelPackageImpl#getEntityModule()
	 * @generated
	 */
	int ENTITY_MODULE = 5;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_MODULE__DEPENDENCIES = Module_managementPackage.MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_MODULE__THE_DESCRIPTION = Module_managementPackage.MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_MODULE__LICENSE = Module_managementPackage.MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_MODULE__NAME = Module_managementPackage.MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_MODULE__VERSION = Module_managementPackage.MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_MODULE__LONG_NAME = Module_managementPackage.MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Ns URI</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_MODULE__NS_URI = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Ns Prefix</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_MODULE__NS_PREFIX = Module_managementPackage.MODULE_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Entities</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_MODULE__ENTITIES = Module_managementPackage.MODULE_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>Entity Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_MODULE_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 3;

	/**
	 * The number of operations of the '<em>Entity Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_MODULE_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.EntityImpl <em>Entity</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.EntityImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.Data_meta_modelPackageImpl#getEntity()
	 * @generated
	 */
	int ENTITY = 10;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY__NAME = CLASSIFIER__NAME;

	/**
	 * The number of structural features of the '<em>Entity</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_FEATURE_COUNT = CLASSIFIER_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Entity</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_OPERATION_COUNT = CLASSIFIER_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.BaseEntityImpl <em>Base Entity</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.BaseEntityImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.Data_meta_modelPackageImpl#getBaseEntity()
	 * @generated
	 */
	int BASE_ENTITY = 8;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY__NAME = ENTITY__NAME;

	/**
	 * The feature id for the '<em><b>Super Type</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY__SUPER_TYPE = ENTITY_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Attributes</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY__ATTRIBUTES = ENTITY_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Base Entity</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY_FEATURE_COUNT = ENTITY_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Base Entity</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY_OPERATION_COUNT = ENTITY_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.GeneratedEntityImpl <em>Generated Entity</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.GeneratedEntityImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.Data_meta_modelPackageImpl#getGeneratedEntity()
	 * @generated
	 */
	int GENERATED_ENTITY = 9;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY__NAME = ENTITY__NAME;

	/**
	 * The feature id for the '<em><b>EAttributes</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY__EATTRIBUTES = ENTITY_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Generated Entity</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY_FEATURE_COUNT = ENTITY_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Generated Entity</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY_OPERATION_COUNT = ENTITY_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.DerivedEntityImpl <em>Derived Entity</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.DerivedEntityImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.Data_meta_modelPackageImpl#getDerivedEntity()
	 * @generated
	 */
	int DERIVED_ENTITY = 11;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY__NAME = ENTITY__NAME;

	/**
	 * The feature id for the '<em><b>EAttributes</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY__EATTRIBUTES = ENTITY_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>EID Attribute</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY__EID_ATTRIBUTE = ENTITY_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Derived Entity</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_FEATURE_COUNT = ENTITY_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Derived Entity</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_OPERATION_COUNT = ENTITY_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.ReturnedTypedElementImpl <em>Returned Typed Element</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.ReturnedTypedElementImpl
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.Data_meta_modelPackageImpl#getReturnedTypedElement()
	 * @generated
	 */
	int RETURNED_TYPED_ELEMENT = 12;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RETURNED_TYPED_ELEMENT__NAME = TYPED_ELEMENT__NAME;

	/**
	 * The feature id for the '<em><b>Classifier</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RETURNED_TYPED_ELEMENT__CLASSIFIER = TYPED_ELEMENT__CLASSIFIER;

	/**
	 * The feature id for the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RETURNED_TYPED_ELEMENT__ORDERED = TYPED_ELEMENT_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Unique</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RETURNED_TYPED_ELEMENT__UNIQUE = TYPED_ELEMENT_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RETURNED_TYPED_ELEMENT__LOWER_BOUND = TYPED_ELEMENT_FEATURE_COUNT + 2;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RETURNED_TYPED_ELEMENT__UPPER_BOUND = TYPED_ELEMENT_FEATURE_COUNT + 3;

	/**
	 * The feature id for the '<em><b>Many</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RETURNED_TYPED_ELEMENT__MANY = TYPED_ELEMENT_FEATURE_COUNT + 4;

	/**
	 * The feature id for the '<em><b>Required</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RETURNED_TYPED_ELEMENT__REQUIRED = TYPED_ELEMENT_FEATURE_COUNT + 5;

	/**
	 * The feature id for the '<em><b>Entity Type</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RETURNED_TYPED_ELEMENT__ENTITY_TYPE = TYPED_ELEMENT_FEATURE_COUNT + 6;

	/**
	 * The number of structural features of the '<em>Returned Typed Element</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RETURNED_TYPED_ELEMENT_FEATURE_COUNT = TYPED_ELEMENT_FEATURE_COUNT + 7;

	/**
	 * The number of operations of the '<em>Returned Typed Element</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RETURNED_TYPED_ELEMENT_OPERATION_COUNT = TYPED_ELEMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '<em>Boolean</em>' data type.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see java.lang.Object
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.Data_meta_modelPackageImpl#getBoolean()
	 * @generated
	 */
	int BOOLEAN = 13;

	/**
	 * The meta object id for the '<em>Int</em>' data type.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see java.lang.Object
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.Data_meta_modelPackageImpl#getInt()
	 * @generated
	 */
	int INT = 14;

	/**
	 * The meta object id for the '<em>String</em>' data type.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see java.lang.Object
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.Data_meta_modelPackageImpl#getString()
	 * @generated
	 */
	int STRING = 15;


	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Attribute <em>Attribute</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Attribute</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Attribute
	 * @generated
	 */
	EClass getAttribute();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Attribute#getIsPK <em>Is PK</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Is PK</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Attribute#getIsPK()
	 * @see #getAttribute()
	 * @generated
	 */
	EAttribute getAttribute_IsPK();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Attribute#getVariable <em>Variable</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Variable</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Attribute#getVariable()
	 * @see #getAttribute()
	 * @generated
	 */
	EReference getAttribute_Variable();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.ForeignKeyAttribute <em>Foreign Key Attribute</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Foreign Key Attribute</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.ForeignKeyAttribute
	 * @generated
	 */
	EClass getForeignKeyAttribute();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.ForeignKeyAttribute#getEntity <em>Entity</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Entity</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.ForeignKeyAttribute#getEntity()
	 * @see #getForeignKeyAttribute()
	 * @generated
	 */
	EReference getForeignKeyAttribute_Entity();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Classifier <em>Classifier</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Classifier</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Classifier
	 * @generated
	 */
	EClass getClassifier();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Element <em>Element</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Element</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Element
	 * @generated
	 */
	EClass getElement();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.NamedElement <em>Named Element</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Named Element</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.NamedElement
	 * @generated
	 */
	EClass getNamedElement();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.NamedElement#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.NamedElement#getName()
	 * @see #getNamedElement()
	 * @generated
	 */
	EAttribute getNamedElement_Name();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.EntityModule <em>Entity Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Entity Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.EntityModule
	 * @generated
	 */
	EClass getEntityModule();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.EntityModule#getNsURI <em>Ns URI</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Ns URI</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.EntityModule#getNsURI()
	 * @see #getEntityModule()
	 * @generated
	 */
	EAttribute getEntityModule_NsURI();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.EntityModule#getNsPrefix <em>Ns Prefix</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Ns Prefix</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.EntityModule#getNsPrefix()
	 * @see #getEntityModule()
	 * @generated
	 */
	EAttribute getEntityModule_NsPrefix();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.EntityModule#getEntities <em>Entities</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Entities</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.EntityModule#getEntities()
	 * @see #getEntityModule()
	 * @generated
	 */
	EReference getEntityModule_Entities();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.StructuralFeature <em>Structural Feature</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Structural Feature</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.StructuralFeature
	 * @generated
	 */
	EClass getStructuralFeature();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.TypedElement <em>Typed Element</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Typed Element</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.TypedElement
	 * @generated
	 */
	EClass getTypedElement();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.TypedElement#getClassifier <em>Classifier</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Classifier</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.TypedElement#getClassifier()
	 * @see #getTypedElement()
	 * @generated
	 */
	EReference getTypedElement_Classifier();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.BaseEntity <em>Base Entity</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Base Entity</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.BaseEntity
	 * @generated
	 */
	EClass getBaseEntity();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.BaseEntity#getSuperType <em>Super Type</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Super Type</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.BaseEntity#getSuperType()
	 * @see #getBaseEntity()
	 * @generated
	 */
	EReference getBaseEntity_SuperType();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.BaseEntity#getAttributes <em>Attributes</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Attributes</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.BaseEntity#getAttributes()
	 * @see #getBaseEntity()
	 * @generated
	 */
	EReference getBaseEntity_Attributes();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.GeneratedEntity <em>Generated Entity</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Generated Entity</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.GeneratedEntity
	 * @generated
	 */
	EClass getGeneratedEntity();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.GeneratedEntity#getEAttributes <em>EAttributes</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>EAttributes</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.GeneratedEntity#getEAttributes()
	 * @see #getGeneratedEntity()
	 * @generated
	 */
	EReference getGeneratedEntity_EAttributes();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Entity <em>Entity</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Entity</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Entity
	 * @generated
	 */
	EClass getEntity();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.DerivedEntity <em>Derived Entity</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Derived Entity</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.DerivedEntity
	 * @generated
	 */
	EClass getDerivedEntity();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.DerivedEntity#getEAttributes <em>EAttributes</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>EAttributes</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.DerivedEntity#getEAttributes()
	 * @see #getDerivedEntity()
	 * @generated
	 */
	EReference getDerivedEntity_EAttributes();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.DerivedEntity#getEIDAttribute <em>EID Attribute</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>EID Attribute</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.DerivedEntity#getEIDAttribute()
	 * @see #getDerivedEntity()
	 * @generated
	 */
	EReference getDerivedEntity_EIDAttribute();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.ReturnedTypedElement <em>Returned Typed Element</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Returned Typed Element</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.ReturnedTypedElement
	 * @generated
	 */
	EClass getReturnedTypedElement();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.ReturnedTypedElement#getOrdered <em>Ordered</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Ordered</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.ReturnedTypedElement#getOrdered()
	 * @see #getReturnedTypedElement()
	 * @generated
	 */
	EAttribute getReturnedTypedElement_Ordered();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.ReturnedTypedElement#getUnique <em>Unique</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Unique</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.ReturnedTypedElement#getUnique()
	 * @see #getReturnedTypedElement()
	 * @generated
	 */
	EAttribute getReturnedTypedElement_Unique();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.ReturnedTypedElement#getLowerBound <em>Lower Bound</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Lower Bound</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.ReturnedTypedElement#getLowerBound()
	 * @see #getReturnedTypedElement()
	 * @generated
	 */
	EAttribute getReturnedTypedElement_LowerBound();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.ReturnedTypedElement#getUpperBound <em>Upper Bound</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Upper Bound</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.ReturnedTypedElement#getUpperBound()
	 * @see #getReturnedTypedElement()
	 * @generated
	 */
	EAttribute getReturnedTypedElement_UpperBound();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.ReturnedTypedElement#getMany <em>Many</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Many</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.ReturnedTypedElement#getMany()
	 * @see #getReturnedTypedElement()
	 * @generated
	 */
	EAttribute getReturnedTypedElement_Many();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.ReturnedTypedElement#getRequired <em>Required</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Required</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.ReturnedTypedElement#getRequired()
	 * @see #getReturnedTypedElement()
	 * @generated
	 */
	EAttribute getReturnedTypedElement_Required();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.ReturnedTypedElement#getEntityType <em>Entity Type</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Entity Type</em>'.
	 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.ReturnedTypedElement#getEntityType()
	 * @see #getReturnedTypedElement()
	 * @generated
	 */
	EReference getReturnedTypedElement_EntityType();

	/**
	 * Returns the meta object for data type '{@link java.lang.Object <em>Boolean</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for data type '<em>Boolean</em>'.
	 * @see java.lang.Object
	 * @model instanceClass="java.lang.Object"
	 *        extendedMetaData="baseType='http://www.w3.org/2001/XMLSchema#boolean'"
	 * @generated
	 */
	EDataType getBoolean();

	/**
	 * Returns the meta object for data type '{@link java.lang.Object <em>Int</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for data type '<em>Int</em>'.
	 * @see java.lang.Object
	 * @model instanceClass="java.lang.Object"
	 *        extendedMetaData="baseType='http://www.w3.org/2001/XMLSchema#int'"
	 * @generated
	 */
	EDataType getInt();

	/**
	 * Returns the meta object for data type '{@link java.lang.Object <em>String</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for data type '<em>String</em>'.
	 * @see java.lang.Object
	 * @model instanceClass="java.lang.Object"
	 *        extendedMetaData="baseType='http://www.w3.org/2001/XMLSchema#string'"
	 * @generated
	 */
	EDataType getString();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	Data_meta_modelFactory getData_meta_modelFactory();

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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.AttributeImpl <em>Attribute</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.AttributeImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.Data_meta_modelPackageImpl#getAttribute()
		 * @generated
		 */
		EClass ATTRIBUTE = eINSTANCE.getAttribute();

		/**
		 * The meta object literal for the '<em><b>Is PK</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute ATTRIBUTE__IS_PK = eINSTANCE.getAttribute_IsPK();

		/**
		 * The meta object literal for the '<em><b>Variable</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ATTRIBUTE__VARIABLE = eINSTANCE.getAttribute_Variable();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.ForeignKeyAttributeImpl <em>Foreign Key Attribute</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.ForeignKeyAttributeImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.Data_meta_modelPackageImpl#getForeignKeyAttribute()
		 * @generated
		 */
		EClass FOREIGN_KEY_ATTRIBUTE = eINSTANCE.getForeignKeyAttribute();

		/**
		 * The meta object literal for the '<em><b>Entity</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference FOREIGN_KEY_ATTRIBUTE__ENTITY = eINSTANCE.getForeignKeyAttribute_Entity();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.ClassifierImpl <em>Classifier</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.ClassifierImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.Data_meta_modelPackageImpl#getClassifier()
		 * @generated
		 */
		EClass CLASSIFIER = eINSTANCE.getClassifier();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.ElementImpl <em>Element</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.ElementImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.Data_meta_modelPackageImpl#getElement()
		 * @generated
		 */
		EClass ELEMENT = eINSTANCE.getElement();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.NamedElementImpl <em>Named Element</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.NamedElementImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.Data_meta_modelPackageImpl#getNamedElement()
		 * @generated
		 */
		EClass NAMED_ELEMENT = eINSTANCE.getNamedElement();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute NAMED_ELEMENT__NAME = eINSTANCE.getNamedElement_Name();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.EntityModuleImpl <em>Entity Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.EntityModuleImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.Data_meta_modelPackageImpl#getEntityModule()
		 * @generated
		 */
		EClass ENTITY_MODULE = eINSTANCE.getEntityModule();

		/**
		 * The meta object literal for the '<em><b>Ns URI</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute ENTITY_MODULE__NS_URI = eINSTANCE.getEntityModule_NsURI();

		/**
		 * The meta object literal for the '<em><b>Ns Prefix</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute ENTITY_MODULE__NS_PREFIX = eINSTANCE.getEntityModule_NsPrefix();

		/**
		 * The meta object literal for the '<em><b>Entities</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ENTITY_MODULE__ENTITIES = eINSTANCE.getEntityModule_Entities();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.StructuralFeatureImpl <em>Structural Feature</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.StructuralFeatureImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.Data_meta_modelPackageImpl#getStructuralFeature()
		 * @generated
		 */
		EClass STRUCTURAL_FEATURE = eINSTANCE.getStructuralFeature();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.TypedElementImpl <em>Typed Element</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.TypedElementImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.Data_meta_modelPackageImpl#getTypedElement()
		 * @generated
		 */
		EClass TYPED_ELEMENT = eINSTANCE.getTypedElement();

		/**
		 * The meta object literal for the '<em><b>Classifier</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference TYPED_ELEMENT__CLASSIFIER = eINSTANCE.getTypedElement_Classifier();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.BaseEntityImpl <em>Base Entity</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.BaseEntityImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.Data_meta_modelPackageImpl#getBaseEntity()
		 * @generated
		 */
		EClass BASE_ENTITY = eINSTANCE.getBaseEntity();

		/**
		 * The meta object literal for the '<em><b>Super Type</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BASE_ENTITY__SUPER_TYPE = eINSTANCE.getBaseEntity_SuperType();

		/**
		 * The meta object literal for the '<em><b>Attributes</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BASE_ENTITY__ATTRIBUTES = eINSTANCE.getBaseEntity_Attributes();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.GeneratedEntityImpl <em>Generated Entity</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.GeneratedEntityImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.Data_meta_modelPackageImpl#getGeneratedEntity()
		 * @generated
		 */
		EClass GENERATED_ENTITY = eINSTANCE.getGeneratedEntity();

		/**
		 * The meta object literal for the '<em><b>EAttributes</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference GENERATED_ENTITY__EATTRIBUTES = eINSTANCE.getGeneratedEntity_EAttributes();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.EntityImpl <em>Entity</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.EntityImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.Data_meta_modelPackageImpl#getEntity()
		 * @generated
		 */
		EClass ENTITY = eINSTANCE.getEntity();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.DerivedEntityImpl <em>Derived Entity</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.DerivedEntityImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.Data_meta_modelPackageImpl#getDerivedEntity()
		 * @generated
		 */
		EClass DERIVED_ENTITY = eINSTANCE.getDerivedEntity();

		/**
		 * The meta object literal for the '<em><b>EAttributes</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference DERIVED_ENTITY__EATTRIBUTES = eINSTANCE.getDerivedEntity_EAttributes();

		/**
		 * The meta object literal for the '<em><b>EID Attribute</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference DERIVED_ENTITY__EID_ATTRIBUTE = eINSTANCE.getDerivedEntity_EIDAttribute();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.ReturnedTypedElementImpl <em>Returned Typed Element</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.ReturnedTypedElementImpl
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.Data_meta_modelPackageImpl#getReturnedTypedElement()
		 * @generated
		 */
		EClass RETURNED_TYPED_ELEMENT = eINSTANCE.getReturnedTypedElement();

		/**
		 * The meta object literal for the '<em><b>Ordered</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute RETURNED_TYPED_ELEMENT__ORDERED = eINSTANCE.getReturnedTypedElement_Ordered();

		/**
		 * The meta object literal for the '<em><b>Unique</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute RETURNED_TYPED_ELEMENT__UNIQUE = eINSTANCE.getReturnedTypedElement_Unique();

		/**
		 * The meta object literal for the '<em><b>Lower Bound</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute RETURNED_TYPED_ELEMENT__LOWER_BOUND = eINSTANCE.getReturnedTypedElement_LowerBound();

		/**
		 * The meta object literal for the '<em><b>Upper Bound</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute RETURNED_TYPED_ELEMENT__UPPER_BOUND = eINSTANCE.getReturnedTypedElement_UpperBound();

		/**
		 * The meta object literal for the '<em><b>Many</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute RETURNED_TYPED_ELEMENT__MANY = eINSTANCE.getReturnedTypedElement_Many();

		/**
		 * The meta object literal for the '<em><b>Required</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute RETURNED_TYPED_ELEMENT__REQUIRED = eINSTANCE.getReturnedTypedElement_Required();

		/**
		 * The meta object literal for the '<em><b>Entity Type</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference RETURNED_TYPED_ELEMENT__ENTITY_TYPE = eINSTANCE.getReturnedTypedElement_EntityType();

		/**
		 * The meta object literal for the '<em>Boolean</em>' data type.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see java.lang.Object
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.Data_meta_modelPackageImpl#getBoolean()
		 * @generated
		 */
		EDataType BOOLEAN = eINSTANCE.getBoolean();

		/**
		 * The meta object literal for the '<em>Int</em>' data type.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see java.lang.Object
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.Data_meta_modelPackageImpl#getInt()
		 * @generated
		 */
		EDataType INT = eINSTANCE.getInt();

		/**
		 * The meta object literal for the '<em>String</em>' data type.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see java.lang.Object
		 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.impl.Data_meta_modelPackageImpl#getString()
		 * @generated
		 */
		EDataType STRING = eINSTANCE.getString();

	}

} //Data_meta_modelPackage
