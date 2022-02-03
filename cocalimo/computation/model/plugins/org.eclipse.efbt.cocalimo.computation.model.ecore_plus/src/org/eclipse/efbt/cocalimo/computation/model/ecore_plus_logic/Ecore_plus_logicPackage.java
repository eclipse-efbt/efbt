/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.Ecore_plus_structurePackage;

import org.eclipse.efbt.cocalimo.core.model.module_management.Module_managementPackage;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EcorePackage;

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
 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Ecore_plus_logicFactory
 * @model kind="package"
 * @generated
 */
public interface Ecore_plus_logicPackage extends EPackage {
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "ecore_plus_logic";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/efbt/ecore_plus_logic";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "ecore_plus_logic";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Ecore_plus_logicPackage eINSTANCE = org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.DerivedEntityImpl <em>Derived Entity</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.DerivedEntityImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getDerivedEntity()
	 * @generated
	 */
	int DERIVED_ENTITY = 0;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY__EANNOTATIONS = Ecore_plus_structurePackage.ENTITY__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY__NAME = Ecore_plus_structurePackage.ENTITY__NAME;

	/**
	 * The feature id for the '<em><b>Instance Class Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY__INSTANCE_CLASS_NAME = Ecore_plus_structurePackage.ENTITY__INSTANCE_CLASS_NAME;

	/**
	 * The feature id for the '<em><b>Instance Class</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY__INSTANCE_CLASS = Ecore_plus_structurePackage.ENTITY__INSTANCE_CLASS;

	/**
	 * The feature id for the '<em><b>Default Value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY__DEFAULT_VALUE = Ecore_plus_structurePackage.ENTITY__DEFAULT_VALUE;

	/**
	 * The feature id for the '<em><b>Instance Type Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY__INSTANCE_TYPE_NAME = Ecore_plus_structurePackage.ENTITY__INSTANCE_TYPE_NAME;

	/**
	 * The feature id for the '<em><b>EPackage</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY__EPACKAGE = Ecore_plus_structurePackage.ENTITY__EPACKAGE;

	/**
	 * The feature id for the '<em><b>EType Parameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY__ETYPE_PARAMETERS = Ecore_plus_structurePackage.ENTITY__ETYPE_PARAMETERS;

	/**
	 * The feature id for the '<em><b>Abstract</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY__ABSTRACT = Ecore_plus_structurePackage.ENTITY__ABSTRACT;

	/**
	 * The feature id for the '<em><b>Interface</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY__INTERFACE = Ecore_plus_structurePackage.ENTITY__INTERFACE;

	/**
	 * The feature id for the '<em><b>ESuper Types</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY__ESUPER_TYPES = Ecore_plus_structurePackage.ENTITY__ESUPER_TYPES;

	/**
	 * The feature id for the '<em><b>EOperations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY__EOPERATIONS = Ecore_plus_structurePackage.ENTITY__EOPERATIONS;

	/**
	 * The feature id for the '<em><b>EAll Attributes</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY__EALL_ATTRIBUTES = Ecore_plus_structurePackage.ENTITY__EALL_ATTRIBUTES;

	/**
	 * The feature id for the '<em><b>EAll References</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY__EALL_REFERENCES = Ecore_plus_structurePackage.ENTITY__EALL_REFERENCES;

	/**
	 * The feature id for the '<em><b>EReferences</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY__EREFERENCES = Ecore_plus_structurePackage.ENTITY__EREFERENCES;

	/**
	 * The feature id for the '<em><b>EAttributes</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY__EATTRIBUTES = Ecore_plus_structurePackage.ENTITY__EATTRIBUTES;

	/**
	 * The feature id for the '<em><b>EAll Containments</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY__EALL_CONTAINMENTS = Ecore_plus_structurePackage.ENTITY__EALL_CONTAINMENTS;

	/**
	 * The feature id for the '<em><b>EAll Operations</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY__EALL_OPERATIONS = Ecore_plus_structurePackage.ENTITY__EALL_OPERATIONS;

	/**
	 * The feature id for the '<em><b>EAll Structural Features</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY__EALL_STRUCTURAL_FEATURES = Ecore_plus_structurePackage.ENTITY__EALL_STRUCTURAL_FEATURES;

	/**
	 * The feature id for the '<em><b>EAll Super Types</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY__EALL_SUPER_TYPES = Ecore_plus_structurePackage.ENTITY__EALL_SUPER_TYPES;

	/**
	 * The feature id for the '<em><b>EID Attribute</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY__EID_ATTRIBUTE = Ecore_plus_structurePackage.ENTITY__EID_ATTRIBUTE;

	/**
	 * The feature id for the '<em><b>EStructural Features</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY__ESTRUCTURAL_FEATURES = Ecore_plus_structurePackage.ENTITY__ESTRUCTURAL_FEATURES;

	/**
	 * The feature id for the '<em><b>EGeneric Super Types</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY__EGENERIC_SUPER_TYPES = Ecore_plus_structurePackage.ENTITY__EGENERIC_SUPER_TYPES;

	/**
	 * The feature id for the '<em><b>EAll Generic Super Types</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY__EALL_GENERIC_SUPER_TYPES = Ecore_plus_structurePackage.ENTITY__EALL_GENERIC_SUPER_TYPES;

	/**
	 * The feature id for the '<em><b>Source Entities</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY__SOURCE_ENTITIES = Ecore_plus_structurePackage.ENTITY_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Derived Attributes</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY__DERIVED_ATTRIBUTES = Ecore_plus_structurePackage.ENTITY_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Row Creation Approach For Entity</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY__ROW_CREATION_APPROACH_FOR_ENTITY = Ecore_plus_structurePackage.ENTITY_FEATURE_COUNT + 2;

	/**
	 * The feature id for the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY__CUBE = Ecore_plus_structurePackage.ENTITY_FEATURE_COUNT + 3;

	/**
	 * The feature id for the '<em><b>Derived Structs</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY__DERIVED_STRUCTS = Ecore_plus_structurePackage.ENTITY_FEATURE_COUNT + 4;

	/**
	 * The number of structural features of the '<em>Derived Entity</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_FEATURE_COUNT = Ecore_plus_structurePackage.ENTITY_FEATURE_COUNT + 5;

	/**
	 * The operation id for the '<em>Get EAnnotation</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY___GET_EANNOTATION__STRING = Ecore_plus_structurePackage.ENTITY___GET_EANNOTATION__STRING;

	/**
	 * The operation id for the '<em>Is Instance</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY___IS_INSTANCE__OBJECT = Ecore_plus_structurePackage.ENTITY___IS_INSTANCE__OBJECT;

	/**
	 * The operation id for the '<em>Get Classifier ID</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY___GET_CLASSIFIER_ID = Ecore_plus_structurePackage.ENTITY___GET_CLASSIFIER_ID;

	/**
	 * The operation id for the '<em>Is Super Type Of</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY___IS_SUPER_TYPE_OF__ECLASS = Ecore_plus_structurePackage.ENTITY___IS_SUPER_TYPE_OF__ECLASS;

	/**
	 * The operation id for the '<em>Get Feature Count</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY___GET_FEATURE_COUNT = Ecore_plus_structurePackage.ENTITY___GET_FEATURE_COUNT;

	/**
	 * The operation id for the '<em>Get EStructural Feature</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY___GET_ESTRUCTURAL_FEATURE__INT = Ecore_plus_structurePackage.ENTITY___GET_ESTRUCTURAL_FEATURE__INT;

	/**
	 * The operation id for the '<em>Get Feature ID</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY___GET_FEATURE_ID__ESTRUCTURALFEATURE = Ecore_plus_structurePackage.ENTITY___GET_FEATURE_ID__ESTRUCTURALFEATURE;

	/**
	 * The operation id for the '<em>Get EStructural Feature</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY___GET_ESTRUCTURAL_FEATURE__STRING = Ecore_plus_structurePackage.ENTITY___GET_ESTRUCTURAL_FEATURE__STRING;

	/**
	 * The operation id for the '<em>Get Operation Count</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY___GET_OPERATION_COUNT = Ecore_plus_structurePackage.ENTITY___GET_OPERATION_COUNT;

	/**
	 * The operation id for the '<em>Get EOperation</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY___GET_EOPERATION__INT = Ecore_plus_structurePackage.ENTITY___GET_EOPERATION__INT;

	/**
	 * The operation id for the '<em>Get Operation ID</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY___GET_OPERATION_ID__EOPERATION = Ecore_plus_structurePackage.ENTITY___GET_OPERATION_ID__EOPERATION;

	/**
	 * The operation id for the '<em>Get Override</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY___GET_OVERRIDE__EOPERATION = Ecore_plus_structurePackage.ENTITY___GET_OVERRIDE__EOPERATION;

	/**
	 * The operation id for the '<em>Get Feature Type</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY___GET_FEATURE_TYPE__ESTRUCTURALFEATURE = Ecore_plus_structurePackage.ENTITY___GET_FEATURE_TYPE__ESTRUCTURALFEATURE;

	/**
	 * The number of operations of the '<em>Derived Entity</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_OPERATION_COUNT = Ecore_plus_structurePackage.ENTITY_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.IntermediateDerivedEntityImpl <em>Intermediate Derived Entity</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.IntermediateDerivedEntityImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getIntermediateDerivedEntity()
	 * @generated
	 */
	int INTERMEDIATE_DERIVED_ENTITY = 1;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITY__EANNOTATIONS = DERIVED_ENTITY__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITY__NAME = DERIVED_ENTITY__NAME;

	/**
	 * The feature id for the '<em><b>Instance Class Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITY__INSTANCE_CLASS_NAME = DERIVED_ENTITY__INSTANCE_CLASS_NAME;

	/**
	 * The feature id for the '<em><b>Instance Class</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITY__INSTANCE_CLASS = DERIVED_ENTITY__INSTANCE_CLASS;

	/**
	 * The feature id for the '<em><b>Default Value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITY__DEFAULT_VALUE = DERIVED_ENTITY__DEFAULT_VALUE;

	/**
	 * The feature id for the '<em><b>Instance Type Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITY__INSTANCE_TYPE_NAME = DERIVED_ENTITY__INSTANCE_TYPE_NAME;

	/**
	 * The feature id for the '<em><b>EPackage</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITY__EPACKAGE = DERIVED_ENTITY__EPACKAGE;

	/**
	 * The feature id for the '<em><b>EType Parameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITY__ETYPE_PARAMETERS = DERIVED_ENTITY__ETYPE_PARAMETERS;

	/**
	 * The feature id for the '<em><b>Abstract</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITY__ABSTRACT = DERIVED_ENTITY__ABSTRACT;

	/**
	 * The feature id for the '<em><b>Interface</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITY__INTERFACE = DERIVED_ENTITY__INTERFACE;

	/**
	 * The feature id for the '<em><b>ESuper Types</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITY__ESUPER_TYPES = DERIVED_ENTITY__ESUPER_TYPES;

	/**
	 * The feature id for the '<em><b>EOperations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITY__EOPERATIONS = DERIVED_ENTITY__EOPERATIONS;

	/**
	 * The feature id for the '<em><b>EAll Attributes</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITY__EALL_ATTRIBUTES = DERIVED_ENTITY__EALL_ATTRIBUTES;

	/**
	 * The feature id for the '<em><b>EAll References</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITY__EALL_REFERENCES = DERIVED_ENTITY__EALL_REFERENCES;

	/**
	 * The feature id for the '<em><b>EReferences</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITY__EREFERENCES = DERIVED_ENTITY__EREFERENCES;

	/**
	 * The feature id for the '<em><b>EAttributes</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITY__EATTRIBUTES = DERIVED_ENTITY__EATTRIBUTES;

	/**
	 * The feature id for the '<em><b>EAll Containments</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITY__EALL_CONTAINMENTS = DERIVED_ENTITY__EALL_CONTAINMENTS;

	/**
	 * The feature id for the '<em><b>EAll Operations</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITY__EALL_OPERATIONS = DERIVED_ENTITY__EALL_OPERATIONS;

	/**
	 * The feature id for the '<em><b>EAll Structural Features</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITY__EALL_STRUCTURAL_FEATURES = DERIVED_ENTITY__EALL_STRUCTURAL_FEATURES;

	/**
	 * The feature id for the '<em><b>EAll Super Types</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITY__EALL_SUPER_TYPES = DERIVED_ENTITY__EALL_SUPER_TYPES;

	/**
	 * The feature id for the '<em><b>EID Attribute</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITY__EID_ATTRIBUTE = DERIVED_ENTITY__EID_ATTRIBUTE;

	/**
	 * The feature id for the '<em><b>EStructural Features</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITY__ESTRUCTURAL_FEATURES = DERIVED_ENTITY__ESTRUCTURAL_FEATURES;

	/**
	 * The feature id for the '<em><b>EGeneric Super Types</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITY__EGENERIC_SUPER_TYPES = DERIVED_ENTITY__EGENERIC_SUPER_TYPES;

	/**
	 * The feature id for the '<em><b>EAll Generic Super Types</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITY__EALL_GENERIC_SUPER_TYPES = DERIVED_ENTITY__EALL_GENERIC_SUPER_TYPES;

	/**
	 * The feature id for the '<em><b>Source Entities</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITY__SOURCE_ENTITIES = DERIVED_ENTITY__SOURCE_ENTITIES;

	/**
	 * The feature id for the '<em><b>Derived Attributes</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITY__DERIVED_ATTRIBUTES = DERIVED_ENTITY__DERIVED_ATTRIBUTES;

	/**
	 * The feature id for the '<em><b>Row Creation Approach For Entity</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITY__ROW_CREATION_APPROACH_FOR_ENTITY = DERIVED_ENTITY__ROW_CREATION_APPROACH_FOR_ENTITY;

	/**
	 * The feature id for the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITY__CUBE = DERIVED_ENTITY__CUBE;

	/**
	 * The feature id for the '<em><b>Derived Structs</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITY__DERIVED_STRUCTS = DERIVED_ENTITY__DERIVED_STRUCTS;

	/**
	 * The number of structural features of the '<em>Intermediate Derived Entity</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITY_FEATURE_COUNT = DERIVED_ENTITY_FEATURE_COUNT + 0;

	/**
	 * The operation id for the '<em>Get EAnnotation</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITY___GET_EANNOTATION__STRING = DERIVED_ENTITY___GET_EANNOTATION__STRING;

	/**
	 * The operation id for the '<em>Is Instance</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITY___IS_INSTANCE__OBJECT = DERIVED_ENTITY___IS_INSTANCE__OBJECT;

	/**
	 * The operation id for the '<em>Get Classifier ID</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITY___GET_CLASSIFIER_ID = DERIVED_ENTITY___GET_CLASSIFIER_ID;

	/**
	 * The operation id for the '<em>Is Super Type Of</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITY___IS_SUPER_TYPE_OF__ECLASS = DERIVED_ENTITY___IS_SUPER_TYPE_OF__ECLASS;

	/**
	 * The operation id for the '<em>Get Feature Count</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITY___GET_FEATURE_COUNT = DERIVED_ENTITY___GET_FEATURE_COUNT;

	/**
	 * The operation id for the '<em>Get EStructural Feature</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITY___GET_ESTRUCTURAL_FEATURE__INT = DERIVED_ENTITY___GET_ESTRUCTURAL_FEATURE__INT;

	/**
	 * The operation id for the '<em>Get Feature ID</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITY___GET_FEATURE_ID__ESTRUCTURALFEATURE = DERIVED_ENTITY___GET_FEATURE_ID__ESTRUCTURALFEATURE;

	/**
	 * The operation id for the '<em>Get EStructural Feature</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITY___GET_ESTRUCTURAL_FEATURE__STRING = DERIVED_ENTITY___GET_ESTRUCTURAL_FEATURE__STRING;

	/**
	 * The operation id for the '<em>Get Operation Count</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITY___GET_OPERATION_COUNT = DERIVED_ENTITY___GET_OPERATION_COUNT;

	/**
	 * The operation id for the '<em>Get EOperation</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITY___GET_EOPERATION__INT = DERIVED_ENTITY___GET_EOPERATION__INT;

	/**
	 * The operation id for the '<em>Get Operation ID</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITY___GET_OPERATION_ID__EOPERATION = DERIVED_ENTITY___GET_OPERATION_ID__EOPERATION;

	/**
	 * The operation id for the '<em>Get Override</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITY___GET_OVERRIDE__EOPERATION = DERIVED_ENTITY___GET_OVERRIDE__EOPERATION;

	/**
	 * The operation id for the '<em>Get Feature Type</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITY___GET_FEATURE_TYPE__ESTRUCTURALFEATURE = DERIVED_ENTITY___GET_FEATURE_TYPE__ESTRUCTURALFEATURE;

	/**
	 * The number of operations of the '<em>Intermediate Derived Entity</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITY_OPERATION_COUNT = DERIVED_ENTITY_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.GeneratedEntityImpl <em>Generated Entity</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.GeneratedEntityImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getGeneratedEntity()
	 * @generated
	 */
	int GENERATED_ENTITY = 2;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY__EANNOTATIONS = Ecore_plus_structurePackage.ENTITY__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY__NAME = Ecore_plus_structurePackage.ENTITY__NAME;

	/**
	 * The feature id for the '<em><b>Instance Class Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY__INSTANCE_CLASS_NAME = Ecore_plus_structurePackage.ENTITY__INSTANCE_CLASS_NAME;

	/**
	 * The feature id for the '<em><b>Instance Class</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY__INSTANCE_CLASS = Ecore_plus_structurePackage.ENTITY__INSTANCE_CLASS;

	/**
	 * The feature id for the '<em><b>Default Value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY__DEFAULT_VALUE = Ecore_plus_structurePackage.ENTITY__DEFAULT_VALUE;

	/**
	 * The feature id for the '<em><b>Instance Type Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY__INSTANCE_TYPE_NAME = Ecore_plus_structurePackage.ENTITY__INSTANCE_TYPE_NAME;

	/**
	 * The feature id for the '<em><b>EPackage</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY__EPACKAGE = Ecore_plus_structurePackage.ENTITY__EPACKAGE;

	/**
	 * The feature id for the '<em><b>EType Parameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY__ETYPE_PARAMETERS = Ecore_plus_structurePackage.ENTITY__ETYPE_PARAMETERS;

	/**
	 * The feature id for the '<em><b>Abstract</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY__ABSTRACT = Ecore_plus_structurePackage.ENTITY__ABSTRACT;

	/**
	 * The feature id for the '<em><b>Interface</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY__INTERFACE = Ecore_plus_structurePackage.ENTITY__INTERFACE;

	/**
	 * The feature id for the '<em><b>ESuper Types</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY__ESUPER_TYPES = Ecore_plus_structurePackage.ENTITY__ESUPER_TYPES;

	/**
	 * The feature id for the '<em><b>EOperations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY__EOPERATIONS = Ecore_plus_structurePackage.ENTITY__EOPERATIONS;

	/**
	 * The feature id for the '<em><b>EAll Attributes</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY__EALL_ATTRIBUTES = Ecore_plus_structurePackage.ENTITY__EALL_ATTRIBUTES;

	/**
	 * The feature id for the '<em><b>EAll References</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY__EALL_REFERENCES = Ecore_plus_structurePackage.ENTITY__EALL_REFERENCES;

	/**
	 * The feature id for the '<em><b>EReferences</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY__EREFERENCES = Ecore_plus_structurePackage.ENTITY__EREFERENCES;

	/**
	 * The feature id for the '<em><b>EAttributes</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY__EATTRIBUTES = Ecore_plus_structurePackage.ENTITY__EATTRIBUTES;

	/**
	 * The feature id for the '<em><b>EAll Containments</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY__EALL_CONTAINMENTS = Ecore_plus_structurePackage.ENTITY__EALL_CONTAINMENTS;

	/**
	 * The feature id for the '<em><b>EAll Operations</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY__EALL_OPERATIONS = Ecore_plus_structurePackage.ENTITY__EALL_OPERATIONS;

	/**
	 * The feature id for the '<em><b>EAll Structural Features</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY__EALL_STRUCTURAL_FEATURES = Ecore_plus_structurePackage.ENTITY__EALL_STRUCTURAL_FEATURES;

	/**
	 * The feature id for the '<em><b>EAll Super Types</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY__EALL_SUPER_TYPES = Ecore_plus_structurePackage.ENTITY__EALL_SUPER_TYPES;

	/**
	 * The feature id for the '<em><b>EID Attribute</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY__EID_ATTRIBUTE = Ecore_plus_structurePackage.ENTITY__EID_ATTRIBUTE;

	/**
	 * The feature id for the '<em><b>EStructural Features</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY__ESTRUCTURAL_FEATURES = Ecore_plus_structurePackage.ENTITY__ESTRUCTURAL_FEATURES;

	/**
	 * The feature id for the '<em><b>EGeneric Super Types</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY__EGENERIC_SUPER_TYPES = Ecore_plus_structurePackage.ENTITY__EGENERIC_SUPER_TYPES;

	/**
	 * The feature id for the '<em><b>EAll Generic Super Types</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY__EALL_GENERIC_SUPER_TYPES = Ecore_plus_structurePackage.ENTITY__EALL_GENERIC_SUPER_TYPES;

	/**
	 * The number of structural features of the '<em>Generated Entity</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY_FEATURE_COUNT = Ecore_plus_structurePackage.ENTITY_FEATURE_COUNT + 0;

	/**
	 * The operation id for the '<em>Get EAnnotation</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY___GET_EANNOTATION__STRING = Ecore_plus_structurePackage.ENTITY___GET_EANNOTATION__STRING;

	/**
	 * The operation id for the '<em>Is Instance</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY___IS_INSTANCE__OBJECT = Ecore_plus_structurePackage.ENTITY___IS_INSTANCE__OBJECT;

	/**
	 * The operation id for the '<em>Get Classifier ID</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY___GET_CLASSIFIER_ID = Ecore_plus_structurePackage.ENTITY___GET_CLASSIFIER_ID;

	/**
	 * The operation id for the '<em>Is Super Type Of</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY___IS_SUPER_TYPE_OF__ECLASS = Ecore_plus_structurePackage.ENTITY___IS_SUPER_TYPE_OF__ECLASS;

	/**
	 * The operation id for the '<em>Get Feature Count</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY___GET_FEATURE_COUNT = Ecore_plus_structurePackage.ENTITY___GET_FEATURE_COUNT;

	/**
	 * The operation id for the '<em>Get EStructural Feature</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY___GET_ESTRUCTURAL_FEATURE__INT = Ecore_plus_structurePackage.ENTITY___GET_ESTRUCTURAL_FEATURE__INT;

	/**
	 * The operation id for the '<em>Get Feature ID</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY___GET_FEATURE_ID__ESTRUCTURALFEATURE = Ecore_plus_structurePackage.ENTITY___GET_FEATURE_ID__ESTRUCTURALFEATURE;

	/**
	 * The operation id for the '<em>Get EStructural Feature</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY___GET_ESTRUCTURAL_FEATURE__STRING = Ecore_plus_structurePackage.ENTITY___GET_ESTRUCTURAL_FEATURE__STRING;

	/**
	 * The operation id for the '<em>Get Operation Count</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY___GET_OPERATION_COUNT = Ecore_plus_structurePackage.ENTITY___GET_OPERATION_COUNT;

	/**
	 * The operation id for the '<em>Get EOperation</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY___GET_EOPERATION__INT = Ecore_plus_structurePackage.ENTITY___GET_EOPERATION__INT;

	/**
	 * The operation id for the '<em>Get Operation ID</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY___GET_OPERATION_ID__EOPERATION = Ecore_plus_structurePackage.ENTITY___GET_OPERATION_ID__EOPERATION;

	/**
	 * The operation id for the '<em>Get Override</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY___GET_OVERRIDE__EOPERATION = Ecore_plus_structurePackage.ENTITY___GET_OVERRIDE__EOPERATION;

	/**
	 * The operation id for the '<em>Get Feature Type</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY___GET_FEATURE_TYPE__ESTRUCTURALFEATURE = Ecore_plus_structurePackage.ENTITY___GET_FEATURE_TYPE__ESTRUCTURALFEATURE;

	/**
	 * The number of operations of the '<em>Generated Entity</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY_OPERATION_COUNT = Ecore_plus_structurePackage.ENTITY_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.ParameterImpl <em>Parameter</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.ParameterImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getParameter()
	 * @generated
	 */
	int PARAMETER = 12;

	/**
	 * The number of structural features of the '<em>Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARAMETER_FEATURE_COUNT = 0;

	/**
	 * The number of operations of the '<em>Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARAMETER_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.FunctionImpl <em>Function</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.FunctionImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getFunction()
	 * @generated
	 */
	int FUNCTION = 8;

	/**
	 * The feature id for the '<em><b>Parameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTION__PARAMETERS = PARAMETER_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTION__NAME = PARAMETER_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Function</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTION_FEATURE_COUNT = PARAMETER_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Function</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTION_OPERATION_COUNT = PARAMETER_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.AggregateFunctionImpl <em>Aggregate Function</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.AggregateFunctionImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getAggregateFunction()
	 * @generated
	 */
	int AGGREGATE_FUNCTION = 3;

	/**
	 * The feature id for the '<em><b>Parameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_FUNCTION__PARAMETERS = FUNCTION__PARAMETERS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_FUNCTION__NAME = FUNCTION__NAME;

	/**
	 * The feature id for the '<em><b>Function Spec</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_FUNCTION__FUNCTION_SPEC = FUNCTION_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Aggregate Function</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_FUNCTION_FEATURE_COUNT = FUNCTION_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Aggregate Function</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_FUNCTION_OPERATION_COUNT = FUNCTION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.FunctionSpecImpl <em>Function Spec</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.FunctionSpecImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getFunctionSpec()
	 * @generated
	 */
	int FUNCTION_SPEC = 9;

	/**
	 * The feature id for the '<em><b>Parameter Specs</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTION_SPEC__PARAMETER_SPECS = 0;

	/**
	 * The feature id for the '<em><b>Functional Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTION_SPEC__FUNCTIONAL_NAME = 1;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTION_SPEC__NAME = 2;

	/**
	 * The feature id for the '<em><b>Sql Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTION_SPEC__SQL_NAME = 3;

	/**
	 * The number of structural features of the '<em>Function Spec</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTION_SPEC_FEATURE_COUNT = 4;

	/**
	 * The number of operations of the '<em>Function Spec</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTION_SPEC_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.AggregateFunctionSpecImpl <em>Aggregate Function Spec</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.AggregateFunctionSpecImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getAggregateFunctionSpec()
	 * @generated
	 */
	int AGGREGATE_FUNCTION_SPEC = 4;

	/**
	 * The feature id for the '<em><b>Parameter Specs</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_FUNCTION_SPEC__PARAMETER_SPECS = FUNCTION_SPEC__PARAMETER_SPECS;

	/**
	 * The feature id for the '<em><b>Functional Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_FUNCTION_SPEC__FUNCTIONAL_NAME = FUNCTION_SPEC__FUNCTIONAL_NAME;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_FUNCTION_SPEC__NAME = FUNCTION_SPEC__NAME;

	/**
	 * The feature id for the '<em><b>Sql Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_FUNCTION_SPEC__SQL_NAME = FUNCTION_SPEC__SQL_NAME;

	/**
	 * The number of structural features of the '<em>Aggregate Function Spec</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_FUNCTION_SPEC_FEATURE_COUNT = FUNCTION_SPEC_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Aggregate Function Spec</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_FUNCTION_SPEC_OPERATION_COUNT = FUNCTION_SPEC_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BasicFunctionImpl <em>Basic Function</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BasicFunctionImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getBasicFunction()
	 * @generated
	 */
	int BASIC_FUNCTION = 5;

	/**
	 * The feature id for the '<em><b>Parameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_FUNCTION__PARAMETERS = FUNCTION__PARAMETERS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_FUNCTION__NAME = FUNCTION__NAME;

	/**
	 * The feature id for the '<em><b>Function Spec</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_FUNCTION__FUNCTION_SPEC = FUNCTION_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Basic Function</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_FUNCTION_FEATURE_COUNT = FUNCTION_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Basic Function</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_FUNCTION_OPERATION_COUNT = FUNCTION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BasicFunctionSpecImpl <em>Basic Function Spec</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BasicFunctionSpecImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getBasicFunctionSpec()
	 * @generated
	 */
	int BASIC_FUNCTION_SPEC = 6;

	/**
	 * The feature id for the '<em><b>Parameter Specs</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_FUNCTION_SPEC__PARAMETER_SPECS = FUNCTION_SPEC__PARAMETER_SPECS;

	/**
	 * The feature id for the '<em><b>Functional Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_FUNCTION_SPEC__FUNCTIONAL_NAME = FUNCTION_SPEC__FUNCTIONAL_NAME;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_FUNCTION_SPEC__NAME = FUNCTION_SPEC__NAME;

	/**
	 * The feature id for the '<em><b>Sql Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_FUNCTION_SPEC__SQL_NAME = FUNCTION_SPEC__SQL_NAME;

	/**
	 * The feature id for the '<em><b>Is Infix</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_FUNCTION_SPEC__IS_INFIX = FUNCTION_SPEC_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Basic Function Spec</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_FUNCTION_SPEC_FEATURE_COUNT = FUNCTION_SPEC_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Basic Function Spec</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_FUNCTION_SPEC_OPERATION_COUNT = FUNCTION_SPEC_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BooleanFunctionImpl <em>Boolean Function</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BooleanFunctionImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getBooleanFunction()
	 * @generated
	 */
	int BOOLEAN_FUNCTION = 7;

	/**
	 * The feature id for the '<em><b>Parameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BOOLEAN_FUNCTION__PARAMETERS = BASIC_FUNCTION__PARAMETERS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BOOLEAN_FUNCTION__NAME = BASIC_FUNCTION__NAME;

	/**
	 * The feature id for the '<em><b>Function Spec</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BOOLEAN_FUNCTION__FUNCTION_SPEC = BASIC_FUNCTION__FUNCTION_SPEC;

	/**
	 * The number of structural features of the '<em>Boolean Function</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BOOLEAN_FUNCTION_FEATURE_COUNT = BASIC_FUNCTION_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Boolean Function</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BOOLEAN_FUNCTION_OPERATION_COUNT = BASIC_FUNCTION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.FunctionSpecModuleImpl <em>Function Spec Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.FunctionSpecModuleImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getFunctionSpecModule()
	 * @generated
	 */
	int FUNCTION_SPEC_MODULE = 10;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTION_SPEC_MODULE__DEPENDENCIES = Module_managementPackage.MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTION_SPEC_MODULE__THE_DESCRIPTION = Module_managementPackage.MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTION_SPEC_MODULE__LICENSE = Module_managementPackage.MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTION_SPEC_MODULE__NAME = Module_managementPackage.MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTION_SPEC_MODULE__VERSION = Module_managementPackage.MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTION_SPEC_MODULE__LONG_NAME = Module_managementPackage.MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Function Specs</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTION_SPEC_MODULE__FUNCTION_SPECS = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Function Spec Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTION_SPEC_MODULE_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Function Spec Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTION_SPEC_MODULE_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.ParamaterSpecImpl <em>Paramater Spec</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.ParamaterSpecImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getParamaterSpec()
	 * @generated
	 */
	int PARAMATER_SPEC = 11;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARAMATER_SPEC__NAME = 0;

	/**
	 * The number of structural features of the '<em>Paramater Spec</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARAMATER_SPEC_FEATURE_COUNT = 1;

	/**
	 * The number of operations of the '<em>Paramater Spec</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARAMATER_SPEC_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.MemberParameterImpl <em>Member Parameter</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.MemberParameterImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getMemberParameter()
	 * @generated
	 */
	int MEMBER_PARAMETER = 13;

	/**
	 * The feature id for the '<em><b>Param</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_PARAMETER__PARAM = PARAMETER_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Member Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_PARAMETER_FEATURE_COUNT = PARAMETER_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Member Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_PARAMETER_OPERATION_COUNT = PARAMETER_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.ValueParameterImpl <em>Value Parameter</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.ValueParameterImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getValueParameter()
	 * @generated
	 */
	int VALUE_PARAMETER = 14;

	/**
	 * The feature id for the '<em><b>Value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VALUE_PARAMETER__VALUE = PARAMETER_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Data Type</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VALUE_PARAMETER__DATA_TYPE = PARAMETER_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Value Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VALUE_PARAMETER_FEATURE_COUNT = PARAMETER_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Value Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VALUE_PARAMETER_OPERATION_COUNT = PARAMETER_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.DerivedAttributeImpl <em>Derived Attribute</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.DerivedAttributeImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getDerivedAttribute()
	 * @generated
	 */
	int DERIVED_ATTRIBUTE = 18;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ATTRIBUTE__EANNOTATIONS = EcorePackage.EATTRIBUTE__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ATTRIBUTE__NAME = EcorePackage.EATTRIBUTE__NAME;

	/**
	 * The feature id for the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ATTRIBUTE__ORDERED = EcorePackage.EATTRIBUTE__ORDERED;

	/**
	 * The feature id for the '<em><b>Unique</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ATTRIBUTE__UNIQUE = EcorePackage.EATTRIBUTE__UNIQUE;

	/**
	 * The feature id for the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ATTRIBUTE__LOWER_BOUND = EcorePackage.EATTRIBUTE__LOWER_BOUND;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ATTRIBUTE__UPPER_BOUND = EcorePackage.EATTRIBUTE__UPPER_BOUND;

	/**
	 * The feature id for the '<em><b>Many</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ATTRIBUTE__MANY = EcorePackage.EATTRIBUTE__MANY;

	/**
	 * The feature id for the '<em><b>Required</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ATTRIBUTE__REQUIRED = EcorePackage.EATTRIBUTE__REQUIRED;

	/**
	 * The feature id for the '<em><b>EType</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ATTRIBUTE__ETYPE = EcorePackage.EATTRIBUTE__ETYPE;

	/**
	 * The feature id for the '<em><b>EGeneric Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ATTRIBUTE__EGENERIC_TYPE = EcorePackage.EATTRIBUTE__EGENERIC_TYPE;

	/**
	 * The feature id for the '<em><b>Changeable</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ATTRIBUTE__CHANGEABLE = EcorePackage.EATTRIBUTE__CHANGEABLE;

	/**
	 * The feature id for the '<em><b>Volatile</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ATTRIBUTE__VOLATILE = EcorePackage.EATTRIBUTE__VOLATILE;

	/**
	 * The feature id for the '<em><b>Transient</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ATTRIBUTE__TRANSIENT = EcorePackage.EATTRIBUTE__TRANSIENT;

	/**
	 * The feature id for the '<em><b>Default Value Literal</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ATTRIBUTE__DEFAULT_VALUE_LITERAL = EcorePackage.EATTRIBUTE__DEFAULT_VALUE_LITERAL;

	/**
	 * The feature id for the '<em><b>Default Value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ATTRIBUTE__DEFAULT_VALUE = EcorePackage.EATTRIBUTE__DEFAULT_VALUE;

	/**
	 * The feature id for the '<em><b>Unsettable</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ATTRIBUTE__UNSETTABLE = EcorePackage.EATTRIBUTE__UNSETTABLE;

	/**
	 * The feature id for the '<em><b>Derived</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ATTRIBUTE__DERIVED = EcorePackage.EATTRIBUTE__DERIVED;

	/**
	 * The feature id for the '<em><b>EContaining Class</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ATTRIBUTE__ECONTAINING_CLASS = EcorePackage.EATTRIBUTE__ECONTAINING_CLASS;

	/**
	 * The feature id for the '<em><b>ID</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ATTRIBUTE__ID = EcorePackage.EATTRIBUTE__ID;

	/**
	 * The feature id for the '<em><b>EAttribute Type</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ATTRIBUTE__EATTRIBUTE_TYPE = EcorePackage.EATTRIBUTE__EATTRIBUTE_TYPE;

	/**
	 * The feature id for the '<em><b>Variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ATTRIBUTE__VARIABLE = EcorePackage.EATTRIBUTE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Derived Attribute</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ATTRIBUTE_FEATURE_COUNT = EcorePackage.EATTRIBUTE_FEATURE_COUNT + 1;

	/**
	 * The operation id for the '<em>Get EAnnotation</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ATTRIBUTE___GET_EANNOTATION__STRING = EcorePackage.EATTRIBUTE___GET_EANNOTATION__STRING;

	/**
	 * The operation id for the '<em>Get Feature ID</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ATTRIBUTE___GET_FEATURE_ID = EcorePackage.EATTRIBUTE___GET_FEATURE_ID;

	/**
	 * The operation id for the '<em>Get Container Class</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ATTRIBUTE___GET_CONTAINER_CLASS = EcorePackage.EATTRIBUTE___GET_CONTAINER_CLASS;

	/**
	 * The number of operations of the '<em>Derived Attribute</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ATTRIBUTE_OPERATION_COUNT = EcorePackage.EATTRIBUTE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.AggregateDerivedAttributeImpl <em>Aggregate Derived Attribute</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.AggregateDerivedAttributeImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getAggregateDerivedAttribute()
	 * @generated
	 */
	int AGGREGATE_DERIVED_ATTRIBUTE = 15;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_DERIVED_ATTRIBUTE__EANNOTATIONS = DERIVED_ATTRIBUTE__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_DERIVED_ATTRIBUTE__NAME = DERIVED_ATTRIBUTE__NAME;

	/**
	 * The feature id for the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_DERIVED_ATTRIBUTE__ORDERED = DERIVED_ATTRIBUTE__ORDERED;

	/**
	 * The feature id for the '<em><b>Unique</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_DERIVED_ATTRIBUTE__UNIQUE = DERIVED_ATTRIBUTE__UNIQUE;

	/**
	 * The feature id for the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_DERIVED_ATTRIBUTE__LOWER_BOUND = DERIVED_ATTRIBUTE__LOWER_BOUND;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_DERIVED_ATTRIBUTE__UPPER_BOUND = DERIVED_ATTRIBUTE__UPPER_BOUND;

	/**
	 * The feature id for the '<em><b>Many</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_DERIVED_ATTRIBUTE__MANY = DERIVED_ATTRIBUTE__MANY;

	/**
	 * The feature id for the '<em><b>Required</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_DERIVED_ATTRIBUTE__REQUIRED = DERIVED_ATTRIBUTE__REQUIRED;

	/**
	 * The feature id for the '<em><b>EType</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_DERIVED_ATTRIBUTE__ETYPE = DERIVED_ATTRIBUTE__ETYPE;

	/**
	 * The feature id for the '<em><b>EGeneric Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_DERIVED_ATTRIBUTE__EGENERIC_TYPE = DERIVED_ATTRIBUTE__EGENERIC_TYPE;

	/**
	 * The feature id for the '<em><b>Changeable</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_DERIVED_ATTRIBUTE__CHANGEABLE = DERIVED_ATTRIBUTE__CHANGEABLE;

	/**
	 * The feature id for the '<em><b>Volatile</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_DERIVED_ATTRIBUTE__VOLATILE = DERIVED_ATTRIBUTE__VOLATILE;

	/**
	 * The feature id for the '<em><b>Transient</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_DERIVED_ATTRIBUTE__TRANSIENT = DERIVED_ATTRIBUTE__TRANSIENT;

	/**
	 * The feature id for the '<em><b>Default Value Literal</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_DERIVED_ATTRIBUTE__DEFAULT_VALUE_LITERAL = DERIVED_ATTRIBUTE__DEFAULT_VALUE_LITERAL;

	/**
	 * The feature id for the '<em><b>Default Value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_DERIVED_ATTRIBUTE__DEFAULT_VALUE = DERIVED_ATTRIBUTE__DEFAULT_VALUE;

	/**
	 * The feature id for the '<em><b>Unsettable</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_DERIVED_ATTRIBUTE__UNSETTABLE = DERIVED_ATTRIBUTE__UNSETTABLE;

	/**
	 * The feature id for the '<em><b>Derived</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_DERIVED_ATTRIBUTE__DERIVED = DERIVED_ATTRIBUTE__DERIVED;

	/**
	 * The feature id for the '<em><b>EContaining Class</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_DERIVED_ATTRIBUTE__ECONTAINING_CLASS = DERIVED_ATTRIBUTE__ECONTAINING_CLASS;

	/**
	 * The feature id for the '<em><b>ID</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_DERIVED_ATTRIBUTE__ID = DERIVED_ATTRIBUTE__ID;

	/**
	 * The feature id for the '<em><b>EAttribute Type</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_DERIVED_ATTRIBUTE__EATTRIBUTE_TYPE = DERIVED_ATTRIBUTE__EATTRIBUTE_TYPE;

	/**
	 * The feature id for the '<em><b>Variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_DERIVED_ATTRIBUTE__VARIABLE = DERIVED_ATTRIBUTE__VARIABLE;

	/**
	 * The feature id for the '<em><b>Aggregate Function</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_DERIVED_ATTRIBUTE__AGGREGATE_FUNCTION = DERIVED_ATTRIBUTE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Aggregate Derived Attribute</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_DERIVED_ATTRIBUTE_FEATURE_COUNT = DERIVED_ATTRIBUTE_FEATURE_COUNT + 1;

	/**
	 * The operation id for the '<em>Get EAnnotation</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_DERIVED_ATTRIBUTE___GET_EANNOTATION__STRING = DERIVED_ATTRIBUTE___GET_EANNOTATION__STRING;

	/**
	 * The operation id for the '<em>Get Feature ID</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_DERIVED_ATTRIBUTE___GET_FEATURE_ID = DERIVED_ATTRIBUTE___GET_FEATURE_ID;

	/**
	 * The operation id for the '<em>Get Container Class</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_DERIVED_ATTRIBUTE___GET_CONTAINER_CLASS = DERIVED_ATTRIBUTE___GET_CONTAINER_CLASS;

	/**
	 * The number of operations of the '<em>Aggregate Derived Attribute</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_DERIVED_ATTRIBUTE_OPERATION_COUNT = DERIVED_ATTRIBUTE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BasicDerivedAttributeImpl <em>Basic Derived Attribute</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BasicDerivedAttributeImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getBasicDerivedAttribute()
	 * @generated
	 */
	int BASIC_DERIVED_ATTRIBUTE = 16;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_DERIVED_ATTRIBUTE__EANNOTATIONS = DERIVED_ATTRIBUTE__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_DERIVED_ATTRIBUTE__NAME = DERIVED_ATTRIBUTE__NAME;

	/**
	 * The feature id for the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_DERIVED_ATTRIBUTE__ORDERED = DERIVED_ATTRIBUTE__ORDERED;

	/**
	 * The feature id for the '<em><b>Unique</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_DERIVED_ATTRIBUTE__UNIQUE = DERIVED_ATTRIBUTE__UNIQUE;

	/**
	 * The feature id for the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_DERIVED_ATTRIBUTE__LOWER_BOUND = DERIVED_ATTRIBUTE__LOWER_BOUND;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_DERIVED_ATTRIBUTE__UPPER_BOUND = DERIVED_ATTRIBUTE__UPPER_BOUND;

	/**
	 * The feature id for the '<em><b>Many</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_DERIVED_ATTRIBUTE__MANY = DERIVED_ATTRIBUTE__MANY;

	/**
	 * The feature id for the '<em><b>Required</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_DERIVED_ATTRIBUTE__REQUIRED = DERIVED_ATTRIBUTE__REQUIRED;

	/**
	 * The feature id for the '<em><b>EType</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_DERIVED_ATTRIBUTE__ETYPE = DERIVED_ATTRIBUTE__ETYPE;

	/**
	 * The feature id for the '<em><b>EGeneric Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_DERIVED_ATTRIBUTE__EGENERIC_TYPE = DERIVED_ATTRIBUTE__EGENERIC_TYPE;

	/**
	 * The feature id for the '<em><b>Changeable</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_DERIVED_ATTRIBUTE__CHANGEABLE = DERIVED_ATTRIBUTE__CHANGEABLE;

	/**
	 * The feature id for the '<em><b>Volatile</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_DERIVED_ATTRIBUTE__VOLATILE = DERIVED_ATTRIBUTE__VOLATILE;

	/**
	 * The feature id for the '<em><b>Transient</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_DERIVED_ATTRIBUTE__TRANSIENT = DERIVED_ATTRIBUTE__TRANSIENT;

	/**
	 * The feature id for the '<em><b>Default Value Literal</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_DERIVED_ATTRIBUTE__DEFAULT_VALUE_LITERAL = DERIVED_ATTRIBUTE__DEFAULT_VALUE_LITERAL;

	/**
	 * The feature id for the '<em><b>Default Value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_DERIVED_ATTRIBUTE__DEFAULT_VALUE = DERIVED_ATTRIBUTE__DEFAULT_VALUE;

	/**
	 * The feature id for the '<em><b>Unsettable</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_DERIVED_ATTRIBUTE__UNSETTABLE = DERIVED_ATTRIBUTE__UNSETTABLE;

	/**
	 * The feature id for the '<em><b>Derived</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_DERIVED_ATTRIBUTE__DERIVED = DERIVED_ATTRIBUTE__DERIVED;

	/**
	 * The feature id for the '<em><b>EContaining Class</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_DERIVED_ATTRIBUTE__ECONTAINING_CLASS = DERIVED_ATTRIBUTE__ECONTAINING_CLASS;

	/**
	 * The feature id for the '<em><b>ID</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_DERIVED_ATTRIBUTE__ID = DERIVED_ATTRIBUTE__ID;

	/**
	 * The feature id for the '<em><b>EAttribute Type</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_DERIVED_ATTRIBUTE__EATTRIBUTE_TYPE = DERIVED_ATTRIBUTE__EATTRIBUTE_TYPE;

	/**
	 * The feature id for the '<em><b>Variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_DERIVED_ATTRIBUTE__VARIABLE = DERIVED_ATTRIBUTE__VARIABLE;

	/**
	 * The feature id for the '<em><b>Basic Scenario Logic</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_DERIVED_ATTRIBUTE__BASIC_SCENARIO_LOGIC = DERIVED_ATTRIBUTE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Basic Derived Attribute</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_DERIVED_ATTRIBUTE_FEATURE_COUNT = DERIVED_ATTRIBUTE_FEATURE_COUNT + 1;

	/**
	 * The operation id for the '<em>Get EAnnotation</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_DERIVED_ATTRIBUTE___GET_EANNOTATION__STRING = DERIVED_ATTRIBUTE___GET_EANNOTATION__STRING;

	/**
	 * The operation id for the '<em>Get Feature ID</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_DERIVED_ATTRIBUTE___GET_FEATURE_ID = DERIVED_ATTRIBUTE___GET_FEATURE_ID;

	/**
	 * The operation id for the '<em>Get Container Class</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_DERIVED_ATTRIBUTE___GET_CONTAINER_CLASS = DERIVED_ATTRIBUTE___GET_CONTAINER_CLASS;

	/**
	 * The number of operations of the '<em>Basic Derived Attribute</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_DERIVED_ATTRIBUTE_OPERATION_COUNT = DERIVED_ATTRIBUTE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BasicScenarioLogicImpl <em>Basic Scenario Logic</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BasicScenarioLogicImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getBasicScenarioLogic()
	 * @generated
	 */
	int BASIC_SCENARIO_LOGIC = 17;

	/**
	 * The feature id for the '<em><b>Basic Function</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_SCENARIO_LOGIC__BASIC_FUNCTION = 0;

	/**
	 * The feature id for the '<em><b>Sceanrio</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_SCENARIO_LOGIC__SCEANRIO = 1;

	/**
	 * The number of structural features of the '<em>Basic Scenario Logic</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_SCENARIO_LOGIC_FEATURE_COUNT = 2;

	/**
	 * The number of operations of the '<em>Basic Scenario Logic</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_SCENARIO_LOGIC_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.DerivedStructImpl <em>Derived Struct</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.DerivedStructImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getDerivedStruct()
	 * @generated
	 */
	int DERIVED_STRUCT = 19;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_STRUCT__EANNOTATIONS = EcorePackage.EREFERENCE__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_STRUCT__NAME = EcorePackage.EREFERENCE__NAME;

	/**
	 * The feature id for the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_STRUCT__ORDERED = EcorePackage.EREFERENCE__ORDERED;

	/**
	 * The feature id for the '<em><b>Unique</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_STRUCT__UNIQUE = EcorePackage.EREFERENCE__UNIQUE;

	/**
	 * The feature id for the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_STRUCT__LOWER_BOUND = EcorePackage.EREFERENCE__LOWER_BOUND;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_STRUCT__UPPER_BOUND = EcorePackage.EREFERENCE__UPPER_BOUND;

	/**
	 * The feature id for the '<em><b>Many</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_STRUCT__MANY = EcorePackage.EREFERENCE__MANY;

	/**
	 * The feature id for the '<em><b>Required</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_STRUCT__REQUIRED = EcorePackage.EREFERENCE__REQUIRED;

	/**
	 * The feature id for the '<em><b>EType</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_STRUCT__ETYPE = EcorePackage.EREFERENCE__ETYPE;

	/**
	 * The feature id for the '<em><b>EGeneric Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_STRUCT__EGENERIC_TYPE = EcorePackage.EREFERENCE__EGENERIC_TYPE;

	/**
	 * The feature id for the '<em><b>Changeable</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_STRUCT__CHANGEABLE = EcorePackage.EREFERENCE__CHANGEABLE;

	/**
	 * The feature id for the '<em><b>Volatile</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_STRUCT__VOLATILE = EcorePackage.EREFERENCE__VOLATILE;

	/**
	 * The feature id for the '<em><b>Transient</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_STRUCT__TRANSIENT = EcorePackage.EREFERENCE__TRANSIENT;

	/**
	 * The feature id for the '<em><b>Default Value Literal</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_STRUCT__DEFAULT_VALUE_LITERAL = EcorePackage.EREFERENCE__DEFAULT_VALUE_LITERAL;

	/**
	 * The feature id for the '<em><b>Default Value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_STRUCT__DEFAULT_VALUE = EcorePackage.EREFERENCE__DEFAULT_VALUE;

	/**
	 * The feature id for the '<em><b>Unsettable</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_STRUCT__UNSETTABLE = EcorePackage.EREFERENCE__UNSETTABLE;

	/**
	 * The feature id for the '<em><b>Derived</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_STRUCT__DERIVED = EcorePackage.EREFERENCE__DERIVED;

	/**
	 * The feature id for the '<em><b>EContaining Class</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_STRUCT__ECONTAINING_CLASS = EcorePackage.EREFERENCE__ECONTAINING_CLASS;

	/**
	 * The feature id for the '<em><b>Containment</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_STRUCT__CONTAINMENT = EcorePackage.EREFERENCE__CONTAINMENT;

	/**
	 * The feature id for the '<em><b>Container</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_STRUCT__CONTAINER = EcorePackage.EREFERENCE__CONTAINER;

	/**
	 * The feature id for the '<em><b>Resolve Proxies</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_STRUCT__RESOLVE_PROXIES = EcorePackage.EREFERENCE__RESOLVE_PROXIES;

	/**
	 * The feature id for the '<em><b>EOpposite</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_STRUCT__EOPPOSITE = EcorePackage.EREFERENCE__EOPPOSITE;

	/**
	 * The feature id for the '<em><b>EReference Type</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_STRUCT__EREFERENCE_TYPE = EcorePackage.EREFERENCE__EREFERENCE_TYPE;

	/**
	 * The feature id for the '<em><b>EKeys</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_STRUCT__EKEYS = EcorePackage.EREFERENCE__EKEYS;

	/**
	 * The feature id for the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_STRUCT__CUBE = EcorePackage.EREFERENCE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Derived Struct</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_STRUCT_FEATURE_COUNT = EcorePackage.EREFERENCE_FEATURE_COUNT + 1;

	/**
	 * The operation id for the '<em>Get EAnnotation</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_STRUCT___GET_EANNOTATION__STRING = EcorePackage.EREFERENCE___GET_EANNOTATION__STRING;

	/**
	 * The operation id for the '<em>Get Feature ID</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_STRUCT___GET_FEATURE_ID = EcorePackage.EREFERENCE___GET_FEATURE_ID;

	/**
	 * The operation id for the '<em>Get Container Class</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_STRUCT___GET_CONTAINER_CLASS = EcorePackage.EREFERENCE___GET_CONTAINER_CLASS;

	/**
	 * The number of operations of the '<em>Derived Struct</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_STRUCT_OPERATION_COUNT = EcorePackage.EREFERENCE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BaseFeatureImpl <em>Base Feature</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BaseFeatureImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getBaseFeature()
	 * @generated
	 */
	int BASE_FEATURE = 20;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_FEATURE__EANNOTATIONS = EcorePackage.EATTRIBUTE__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_FEATURE__NAME = EcorePackage.EATTRIBUTE__NAME;

	/**
	 * The feature id for the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_FEATURE__ORDERED = EcorePackage.EATTRIBUTE__ORDERED;

	/**
	 * The feature id for the '<em><b>Unique</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_FEATURE__UNIQUE = EcorePackage.EATTRIBUTE__UNIQUE;

	/**
	 * The feature id for the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_FEATURE__LOWER_BOUND = EcorePackage.EATTRIBUTE__LOWER_BOUND;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_FEATURE__UPPER_BOUND = EcorePackage.EATTRIBUTE__UPPER_BOUND;

	/**
	 * The feature id for the '<em><b>Many</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_FEATURE__MANY = EcorePackage.EATTRIBUTE__MANY;

	/**
	 * The feature id for the '<em><b>Required</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_FEATURE__REQUIRED = EcorePackage.EATTRIBUTE__REQUIRED;

	/**
	 * The feature id for the '<em><b>EType</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_FEATURE__ETYPE = EcorePackage.EATTRIBUTE__ETYPE;

	/**
	 * The feature id for the '<em><b>EGeneric Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_FEATURE__EGENERIC_TYPE = EcorePackage.EATTRIBUTE__EGENERIC_TYPE;

	/**
	 * The feature id for the '<em><b>Changeable</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_FEATURE__CHANGEABLE = EcorePackage.EATTRIBUTE__CHANGEABLE;

	/**
	 * The feature id for the '<em><b>Volatile</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_FEATURE__VOLATILE = EcorePackage.EATTRIBUTE__VOLATILE;

	/**
	 * The feature id for the '<em><b>Transient</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_FEATURE__TRANSIENT = EcorePackage.EATTRIBUTE__TRANSIENT;

	/**
	 * The feature id for the '<em><b>Default Value Literal</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_FEATURE__DEFAULT_VALUE_LITERAL = EcorePackage.EATTRIBUTE__DEFAULT_VALUE_LITERAL;

	/**
	 * The feature id for the '<em><b>Default Value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_FEATURE__DEFAULT_VALUE = EcorePackage.EATTRIBUTE__DEFAULT_VALUE;

	/**
	 * The feature id for the '<em><b>Unsettable</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_FEATURE__UNSETTABLE = EcorePackage.EATTRIBUTE__UNSETTABLE;

	/**
	 * The feature id for the '<em><b>Derived</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_FEATURE__DERIVED = EcorePackage.EATTRIBUTE__DERIVED;

	/**
	 * The feature id for the '<em><b>EContaining Class</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_FEATURE__ECONTAINING_CLASS = EcorePackage.EATTRIBUTE__ECONTAINING_CLASS;

	/**
	 * The feature id for the '<em><b>ID</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_FEATURE__ID = EcorePackage.EATTRIBUTE__ID;

	/**
	 * The feature id for the '<em><b>EAttribute Type</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_FEATURE__EATTRIBUTE_TYPE = EcorePackage.EATTRIBUTE__EATTRIBUTE_TYPE;

	/**
	 * The feature id for the '<em><b>Variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_FEATURE__VARIABLE = EcorePackage.EATTRIBUTE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Base Feature</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_FEATURE_FEATURE_COUNT = EcorePackage.EATTRIBUTE_FEATURE_COUNT + 1;

	/**
	 * The operation id for the '<em>Get EAnnotation</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_FEATURE___GET_EANNOTATION__STRING = EcorePackage.EATTRIBUTE___GET_EANNOTATION__STRING;

	/**
	 * The operation id for the '<em>Get Feature ID</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_FEATURE___GET_FEATURE_ID = EcorePackage.EATTRIBUTE___GET_FEATURE_ID;

	/**
	 * The operation id for the '<em>Get Container Class</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_FEATURE___GET_CONTAINER_CLASS = EcorePackage.EATTRIBUTE___GET_CONTAINER_CLASS;

	/**
	 * The number of operations of the '<em>Base Feature</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_FEATURE_OPERATION_COUNT = EcorePackage.EATTRIBUTE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.DerivedFeatureGroupImpl <em>Derived Feature Group</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.DerivedFeatureGroupImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getDerivedFeatureGroup()
	 * @generated
	 */
	int DERIVED_FEATURE_GROUP = 21;

	/**
	 * The feature id for the '<em><b>Derived Features</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_FEATURE_GROUP__DERIVED_FEATURES = 0;

	/**
	 * The number of structural features of the '<em>Derived Feature Group</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_FEATURE_GROUP_FEATURE_COUNT = 1;

	/**
	 * The number of operations of the '<em>Derived Feature Group</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_FEATURE_GROUP_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.FeatureParameterImpl <em>Feature Parameter</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.FeatureParameterImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getFeatureParameter()
	 * @generated
	 */
	int FEATURE_PARAMETER = 22;

	/**
	 * The feature id for the '<em><b>Feature</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FEATURE_PARAMETER__FEATURE = PARAMETER_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Feature Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FEATURE_PARAMETER_FEATURE_COUNT = PARAMETER_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Feature Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FEATURE_PARAMETER_OPERATION_COUNT = PARAMETER_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.ProxyFeatureParameterImpl <em>Proxy Feature Parameter</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.ProxyFeatureParameterImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getProxyFeatureParameter()
	 * @generated
	 */
	int PROXY_FEATURE_PARAMETER = 23;

	/**
	 * The feature id for the '<em><b>Entity</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PROXY_FEATURE_PARAMETER__ENTITY = PARAMETER_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Varaible</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PROXY_FEATURE_PARAMETER__VARAIBLE = PARAMETER_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Proxy Feature Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PROXY_FEATURE_PARAMETER_FEATURE_COUNT = PARAMETER_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Proxy Feature Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PROXY_FEATURE_PARAMETER_OPERATION_COUNT = PARAMETER_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.GetAttributeFromEntityDerivedFeatureImpl <em>Get Attribute From Entity Derived Feature</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.GetAttributeFromEntityDerivedFeatureImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getGetAttributeFromEntityDerivedFeature()
	 * @generated
	 */
	int GET_ATTRIBUTE_FROM_ENTITY_DERIVED_FEATURE = 24;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_ATTRIBUTE_FROM_ENTITY_DERIVED_FEATURE__EANNOTATIONS = DERIVED_ATTRIBUTE__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_ATTRIBUTE_FROM_ENTITY_DERIVED_FEATURE__NAME = DERIVED_ATTRIBUTE__NAME;

	/**
	 * The feature id for the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_ATTRIBUTE_FROM_ENTITY_DERIVED_FEATURE__ORDERED = DERIVED_ATTRIBUTE__ORDERED;

	/**
	 * The feature id for the '<em><b>Unique</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_ATTRIBUTE_FROM_ENTITY_DERIVED_FEATURE__UNIQUE = DERIVED_ATTRIBUTE__UNIQUE;

	/**
	 * The feature id for the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_ATTRIBUTE_FROM_ENTITY_DERIVED_FEATURE__LOWER_BOUND = DERIVED_ATTRIBUTE__LOWER_BOUND;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_ATTRIBUTE_FROM_ENTITY_DERIVED_FEATURE__UPPER_BOUND = DERIVED_ATTRIBUTE__UPPER_BOUND;

	/**
	 * The feature id for the '<em><b>Many</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_ATTRIBUTE_FROM_ENTITY_DERIVED_FEATURE__MANY = DERIVED_ATTRIBUTE__MANY;

	/**
	 * The feature id for the '<em><b>Required</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_ATTRIBUTE_FROM_ENTITY_DERIVED_FEATURE__REQUIRED = DERIVED_ATTRIBUTE__REQUIRED;

	/**
	 * The feature id for the '<em><b>EType</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_ATTRIBUTE_FROM_ENTITY_DERIVED_FEATURE__ETYPE = DERIVED_ATTRIBUTE__ETYPE;

	/**
	 * The feature id for the '<em><b>EGeneric Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_ATTRIBUTE_FROM_ENTITY_DERIVED_FEATURE__EGENERIC_TYPE = DERIVED_ATTRIBUTE__EGENERIC_TYPE;

	/**
	 * The feature id for the '<em><b>Changeable</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_ATTRIBUTE_FROM_ENTITY_DERIVED_FEATURE__CHANGEABLE = DERIVED_ATTRIBUTE__CHANGEABLE;

	/**
	 * The feature id for the '<em><b>Volatile</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_ATTRIBUTE_FROM_ENTITY_DERIVED_FEATURE__VOLATILE = DERIVED_ATTRIBUTE__VOLATILE;

	/**
	 * The feature id for the '<em><b>Transient</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_ATTRIBUTE_FROM_ENTITY_DERIVED_FEATURE__TRANSIENT = DERIVED_ATTRIBUTE__TRANSIENT;

	/**
	 * The feature id for the '<em><b>Default Value Literal</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_ATTRIBUTE_FROM_ENTITY_DERIVED_FEATURE__DEFAULT_VALUE_LITERAL = DERIVED_ATTRIBUTE__DEFAULT_VALUE_LITERAL;

	/**
	 * The feature id for the '<em><b>Default Value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_ATTRIBUTE_FROM_ENTITY_DERIVED_FEATURE__DEFAULT_VALUE = DERIVED_ATTRIBUTE__DEFAULT_VALUE;

	/**
	 * The feature id for the '<em><b>Unsettable</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_ATTRIBUTE_FROM_ENTITY_DERIVED_FEATURE__UNSETTABLE = DERIVED_ATTRIBUTE__UNSETTABLE;

	/**
	 * The feature id for the '<em><b>Derived</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_ATTRIBUTE_FROM_ENTITY_DERIVED_FEATURE__DERIVED = DERIVED_ATTRIBUTE__DERIVED;

	/**
	 * The feature id for the '<em><b>EContaining Class</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_ATTRIBUTE_FROM_ENTITY_DERIVED_FEATURE__ECONTAINING_CLASS = DERIVED_ATTRIBUTE__ECONTAINING_CLASS;

	/**
	 * The feature id for the '<em><b>ID</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_ATTRIBUTE_FROM_ENTITY_DERIVED_FEATURE__ID = DERIVED_ATTRIBUTE__ID;

	/**
	 * The feature id for the '<em><b>EAttribute Type</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_ATTRIBUTE_FROM_ENTITY_DERIVED_FEATURE__EATTRIBUTE_TYPE = DERIVED_ATTRIBUTE__EATTRIBUTE_TYPE;

	/**
	 * The feature id for the '<em><b>Variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_ATTRIBUTE_FROM_ENTITY_DERIVED_FEATURE__VARIABLE = DERIVED_ATTRIBUTE__VARIABLE;

	/**
	 * The feature id for the '<em><b>Attribute In Entity</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_ATTRIBUTE_FROM_ENTITY_DERIVED_FEATURE__ATTRIBUTE_IN_ENTITY = DERIVED_ATTRIBUTE_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Entity Feature</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_ATTRIBUTE_FROM_ENTITY_DERIVED_FEATURE__ENTITY_FEATURE = DERIVED_ATTRIBUTE_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Get Attribute From Entity Derived Feature</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_ATTRIBUTE_FROM_ENTITY_DERIVED_FEATURE_FEATURE_COUNT = DERIVED_ATTRIBUTE_FEATURE_COUNT + 2;

	/**
	 * The operation id for the '<em>Get EAnnotation</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_ATTRIBUTE_FROM_ENTITY_DERIVED_FEATURE___GET_EANNOTATION__STRING = DERIVED_ATTRIBUTE___GET_EANNOTATION__STRING;

	/**
	 * The operation id for the '<em>Get Feature ID</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_ATTRIBUTE_FROM_ENTITY_DERIVED_FEATURE___GET_FEATURE_ID = DERIVED_ATTRIBUTE___GET_FEATURE_ID;

	/**
	 * The operation id for the '<em>Get Container Class</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_ATTRIBUTE_FROM_ENTITY_DERIVED_FEATURE___GET_CONTAINER_CLASS = DERIVED_ATTRIBUTE___GET_CONTAINER_CLASS;

	/**
	 * The number of operations of the '<em>Get Attribute From Entity Derived Feature</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GET_ATTRIBUTE_FROM_ENTITY_DERIVED_FEATURE_OPERATION_COUNT = DERIVED_ATTRIBUTE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.AttributeFromEntityParameterImpl <em>Attribute From Entity Parameter</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.AttributeFromEntityParameterImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getAttributeFromEntityParameter()
	 * @generated
	 */
	int ATTRIBUTE_FROM_ENTITY_PARAMETER = 25;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_FROM_ENTITY_PARAMETER__EANNOTATIONS = EcorePackage.EPARAMETER__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_FROM_ENTITY_PARAMETER__NAME = EcorePackage.EPARAMETER__NAME;

	/**
	 * The feature id for the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_FROM_ENTITY_PARAMETER__ORDERED = EcorePackage.EPARAMETER__ORDERED;

	/**
	 * The feature id for the '<em><b>Unique</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_FROM_ENTITY_PARAMETER__UNIQUE = EcorePackage.EPARAMETER__UNIQUE;

	/**
	 * The feature id for the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_FROM_ENTITY_PARAMETER__LOWER_BOUND = EcorePackage.EPARAMETER__LOWER_BOUND;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_FROM_ENTITY_PARAMETER__UPPER_BOUND = EcorePackage.EPARAMETER__UPPER_BOUND;

	/**
	 * The feature id for the '<em><b>Many</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_FROM_ENTITY_PARAMETER__MANY = EcorePackage.EPARAMETER__MANY;

	/**
	 * The feature id for the '<em><b>Required</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_FROM_ENTITY_PARAMETER__REQUIRED = EcorePackage.EPARAMETER__REQUIRED;

	/**
	 * The feature id for the '<em><b>EType</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_FROM_ENTITY_PARAMETER__ETYPE = EcorePackage.EPARAMETER__ETYPE;

	/**
	 * The feature id for the '<em><b>EGeneric Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_FROM_ENTITY_PARAMETER__EGENERIC_TYPE = EcorePackage.EPARAMETER__EGENERIC_TYPE;

	/**
	 * The feature id for the '<em><b>EOperation</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_FROM_ENTITY_PARAMETER__EOPERATION = EcorePackage.EPARAMETER__EOPERATION;

	/**
	 * The feature id for the '<em><b>Attribute In Entity</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_FROM_ENTITY_PARAMETER__ATTRIBUTE_IN_ENTITY = EcorePackage.EPARAMETER_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Entity Feature</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_FROM_ENTITY_PARAMETER__ENTITY_FEATURE = EcorePackage.EPARAMETER_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Attribute From Entity Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_FROM_ENTITY_PARAMETER_FEATURE_COUNT = EcorePackage.EPARAMETER_FEATURE_COUNT + 2;

	/**
	 * The operation id for the '<em>Get EAnnotation</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_FROM_ENTITY_PARAMETER___GET_EANNOTATION__STRING = EcorePackage.EPARAMETER___GET_EANNOTATION__STRING;

	/**
	 * The number of operations of the '<em>Attribute From Entity Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_FROM_ENTITY_PARAMETER_OPERATION_COUNT = EcorePackage.EPARAMETER_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.ProxyAttributeFromEntityParameterImpl <em>Proxy Attribute From Entity Parameter</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.ProxyAttributeFromEntityParameterImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getProxyAttributeFromEntityParameter()
	 * @generated
	 */
	int PROXY_ATTRIBUTE_FROM_ENTITY_PARAMETER = 26;

	/**
	 * The feature id for the '<em><b>Variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PROXY_ATTRIBUTE_FROM_ENTITY_PARAMETER__VARIABLE = PARAMETER_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Entity</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PROXY_ATTRIBUTE_FROM_ENTITY_PARAMETER__ENTITY = PARAMETER_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PROXY_ATTRIBUTE_FROM_ENTITY_PARAMETER__CUBE = PARAMETER_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>Proxy Attribute From Entity Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PROXY_ATTRIBUTE_FROM_ENTITY_PARAMETER_FEATURE_COUNT = PARAMETER_FEATURE_COUNT + 3;

	/**
	 * The number of operations of the '<em>Proxy Attribute From Entity Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PROXY_ATTRIBUTE_FROM_ENTITY_PARAMETER_OPERATION_COUNT = PARAMETER_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.RowCreationApproachImpl <em>Row Creation Approach</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.RowCreationApproachImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getRowCreationApproach()
	 * @generated
	 */
	int ROW_CREATION_APPROACH = 32;

	/**
	 * The feature id for the '<em><b>Dependant Cube Columns</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_CREATION_APPROACH__DEPENDANT_CUBE_COLUMNS = 0;

	/**
	 * The feature id for the '<em><b>Created Cube Columns</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_CREATION_APPROACH__CREATED_CUBE_COLUMNS = 1;

	/**
	 * The feature id for the '<em><b>Dependant Struct Item Columns</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_CREATION_APPROACH__DEPENDANT_STRUCT_ITEM_COLUMNS = 2;

	/**
	 * The number of structural features of the '<em>Row Creation Approach</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_CREATION_APPROACH_FEATURE_COUNT = 3;

	/**
	 * The number of operations of the '<em>Row Creation Approach</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_CREATION_APPROACH_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.CustomRowCreationApproachImpl <em>Custom Row Creation Approach</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.CustomRowCreationApproachImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getCustomRowCreationApproach()
	 * @generated
	 */
	int CUSTOM_ROW_CREATION_APPROACH = 27;

	/**
	 * The feature id for the '<em><b>Dependant Cube Columns</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUSTOM_ROW_CREATION_APPROACH__DEPENDANT_CUBE_COLUMNS = ROW_CREATION_APPROACH__DEPENDANT_CUBE_COLUMNS;

	/**
	 * The feature id for the '<em><b>Created Cube Columns</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUSTOM_ROW_CREATION_APPROACH__CREATED_CUBE_COLUMNS = ROW_CREATION_APPROACH__CREATED_CUBE_COLUMNS;

	/**
	 * The feature id for the '<em><b>Dependant Struct Item Columns</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUSTOM_ROW_CREATION_APPROACH__DEPENDANT_STRUCT_ITEM_COLUMNS = ROW_CREATION_APPROACH__DEPENDANT_STRUCT_ITEM_COLUMNS;

	/**
	 * The feature id for the '<em><b>Dependant Variables</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUSTOM_ROW_CREATION_APPROACH__DEPENDANT_VARIABLES = ROW_CREATION_APPROACH_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Custom Row Creation Approach</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUSTOM_ROW_CREATION_APPROACH_FEATURE_COUNT = ROW_CREATION_APPROACH_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Custom Row Creation Approach</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUSTOM_ROW_CREATION_APPROACH_OPERATION_COUNT = ROW_CREATION_APPROACH_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.FilterAndGroupToOneRowCreationApproachImpl <em>Filter And Group To One Row Creation Approach</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.FilterAndGroupToOneRowCreationApproachImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getFilterAndGroupToOneRowCreationApproach()
	 * @generated
	 */
	int FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH = 28;

	/**
	 * The feature id for the '<em><b>Dependant Cube Columns</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH__DEPENDANT_CUBE_COLUMNS = ROW_CREATION_APPROACH__DEPENDANT_CUBE_COLUMNS;

	/**
	 * The feature id for the '<em><b>Created Cube Columns</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH__CREATED_CUBE_COLUMNS = ROW_CREATION_APPROACH__CREATED_CUBE_COLUMNS;

	/**
	 * The feature id for the '<em><b>Dependant Struct Item Columns</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH__DEPENDANT_STRUCT_ITEM_COLUMNS = ROW_CREATION_APPROACH__DEPENDANT_STRUCT_ITEM_COLUMNS;

	/**
	 * The feature id for the '<em><b>Filter Function</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH__FILTER_FUNCTION = ROW_CREATION_APPROACH_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Group By Variables</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH__GROUP_BY_VARIABLES = ROW_CREATION_APPROACH_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Filter And Group To One Row Creation Approach</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH_FEATURE_COUNT = ROW_CREATION_APPROACH_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Filter And Group To One Row Creation Approach</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH_OPERATION_COUNT = ROW_CREATION_APPROACH_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.RowCreationApproachForEntityImpl <em>Row Creation Approach For Entity</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.RowCreationApproachForEntityImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getRowCreationApproachForEntity()
	 * @generated
	 */
	int ROW_CREATION_APPROACH_FOR_ENTITY = 29;

	/**
	 * The feature id for the '<em><b>Row Creation Approach</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_CREATION_APPROACH_FOR_ENTITY__ROW_CREATION_APPROACH = 0;

	/**
	 * The feature id for the '<em><b>Entity</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_CREATION_APPROACH_FOR_ENTITY__ENTITY = 1;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_CREATION_APPROACH_FOR_ENTITY__NAME = 2;

	/**
	 * The number of structural features of the '<em>Row Creation Approach For Entity</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_CREATION_APPROACH_FOR_ENTITY_FEATURE_COUNT = 3;

	/**
	 * The number of operations of the '<em>Row Creation Approach For Entity</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_CREATION_APPROACH_FOR_ENTITY_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.OneToOneRowCreationApproachImpl <em>One To One Row Creation Approach</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.OneToOneRowCreationApproachImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getOneToOneRowCreationApproach()
	 * @generated
	 */
	int ONE_TO_ONE_ROW_CREATION_APPROACH = 30;

	/**
	 * The feature id for the '<em><b>Dependant Cube Columns</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_ONE_ROW_CREATION_APPROACH__DEPENDANT_CUBE_COLUMNS = ROW_CREATION_APPROACH__DEPENDANT_CUBE_COLUMNS;

	/**
	 * The feature id for the '<em><b>Created Cube Columns</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_ONE_ROW_CREATION_APPROACH__CREATED_CUBE_COLUMNS = ROW_CREATION_APPROACH__CREATED_CUBE_COLUMNS;

	/**
	 * The feature id for the '<em><b>Dependant Struct Item Columns</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_ONE_ROW_CREATION_APPROACH__DEPENDANT_STRUCT_ITEM_COLUMNS = ROW_CREATION_APPROACH__DEPENDANT_STRUCT_ITEM_COLUMNS;

	/**
	 * The number of structural features of the '<em>One To One Row Creation Approach</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_ONE_ROW_CREATION_APPROACH_FEATURE_COUNT = ROW_CREATION_APPROACH_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>One To One Row Creation Approach</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_ONE_ROW_CREATION_APPROACH_OPERATION_COUNT = ROW_CREATION_APPROACH_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.FilterRowCreationApproachImpl <em>Filter Row Creation Approach</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.FilterRowCreationApproachImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getFilterRowCreationApproach()
	 * @generated
	 */
	int FILTER_ROW_CREATION_APPROACH = 31;

	/**
	 * The feature id for the '<em><b>Dependant Cube Columns</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_ROW_CREATION_APPROACH__DEPENDANT_CUBE_COLUMNS = ROW_CREATION_APPROACH__DEPENDANT_CUBE_COLUMNS;

	/**
	 * The feature id for the '<em><b>Created Cube Columns</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_ROW_CREATION_APPROACH__CREATED_CUBE_COLUMNS = ROW_CREATION_APPROACH__CREATED_CUBE_COLUMNS;

	/**
	 * The feature id for the '<em><b>Dependant Struct Item Columns</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_ROW_CREATION_APPROACH__DEPENDANT_STRUCT_ITEM_COLUMNS = ROW_CREATION_APPROACH__DEPENDANT_STRUCT_ITEM_COLUMNS;

	/**
	 * The feature id for the '<em><b>Filter Function</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_ROW_CREATION_APPROACH__FILTER_FUNCTION = ROW_CREATION_APPROACH_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Filter Row Creation Approach</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_ROW_CREATION_APPROACH_FEATURE_COUNT = ROW_CREATION_APPROACH_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Filter Row Creation Approach</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_ROW_CREATION_APPROACH_OPERATION_COUNT = ROW_CREATION_APPROACH_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.GroupByRowCreationApproachImpl <em>Group By Row Creation Approach</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.GroupByRowCreationApproachImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getGroupByRowCreationApproach()
	 * @generated
	 */
	int GROUP_BY_ROW_CREATION_APPROACH = 33;

	/**
	 * The feature id for the '<em><b>Dependant Cube Columns</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GROUP_BY_ROW_CREATION_APPROACH__DEPENDANT_CUBE_COLUMNS = ROW_CREATION_APPROACH__DEPENDANT_CUBE_COLUMNS;

	/**
	 * The feature id for the '<em><b>Created Cube Columns</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GROUP_BY_ROW_CREATION_APPROACH__CREATED_CUBE_COLUMNS = ROW_CREATION_APPROACH__CREATED_CUBE_COLUMNS;

	/**
	 * The feature id for the '<em><b>Dependant Struct Item Columns</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GROUP_BY_ROW_CREATION_APPROACH__DEPENDANT_STRUCT_ITEM_COLUMNS = ROW_CREATION_APPROACH__DEPENDANT_STRUCT_ITEM_COLUMNS;

	/**
	 * The feature id for the '<em><b>Group By Cube Column Names</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GROUP_BY_ROW_CREATION_APPROACH__GROUP_BY_CUBE_COLUMN_NAMES = ROW_CREATION_APPROACH_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Group By Columns</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GROUP_BY_ROW_CREATION_APPROACH__GROUP_BY_COLUMNS = ROW_CREATION_APPROACH_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Group By Row Creation Approach</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GROUP_BY_ROW_CREATION_APPROACH_FEATURE_COUNT = ROW_CREATION_APPROACH_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Group By Row Creation Approach</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GROUP_BY_ROW_CREATION_APPROACH_OPERATION_COUNT = ROW_CREATION_APPROACH_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.RowJoinFunctionImpl <em>Row Join Function</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.RowJoinFunctionImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getRowJoinFunction()
	 * @generated
	 */
	int ROW_JOIN_FUNCTION = 34;

	/**
	 * The feature id for the '<em><b>Dependant Cube Columns</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_JOIN_FUNCTION__DEPENDANT_CUBE_COLUMNS = ROW_CREATION_APPROACH__DEPENDANT_CUBE_COLUMNS;

	/**
	 * The feature id for the '<em><b>Created Cube Columns</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_JOIN_FUNCTION__CREATED_CUBE_COLUMNS = ROW_CREATION_APPROACH__CREATED_CUBE_COLUMNS;

	/**
	 * The feature id for the '<em><b>Dependant Struct Item Columns</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_JOIN_FUNCTION__DEPENDANT_STRUCT_ITEM_COLUMNS = ROW_CREATION_APPROACH__DEPENDANT_STRUCT_ITEM_COLUMNS;

	/**
	 * The feature id for the '<em><b>Join Function</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_JOIN_FUNCTION__JOIN_FUNCTION = ROW_CREATION_APPROACH_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Row Join Function</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_JOIN_FUNCTION_FEATURE_COUNT = ROW_CREATION_APPROACH_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Row Join Function</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_JOIN_FUNCTION_OPERATION_COUNT = ROW_CREATION_APPROACH_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.UnionRowCreationApproachImpl <em>Union Row Creation Approach</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.UnionRowCreationApproachImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getUnionRowCreationApproach()
	 * @generated
	 */
	int UNION_ROW_CREATION_APPROACH = 35;

	/**
	 * The feature id for the '<em><b>Dependant Cube Columns</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNION_ROW_CREATION_APPROACH__DEPENDANT_CUBE_COLUMNS = ROW_CREATION_APPROACH__DEPENDANT_CUBE_COLUMNS;

	/**
	 * The feature id for the '<em><b>Created Cube Columns</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNION_ROW_CREATION_APPROACH__CREATED_CUBE_COLUMNS = ROW_CREATION_APPROACH__CREATED_CUBE_COLUMNS;

	/**
	 * The feature id for the '<em><b>Dependant Struct Item Columns</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNION_ROW_CREATION_APPROACH__DEPENDANT_STRUCT_ITEM_COLUMNS = ROW_CREATION_APPROACH__DEPENDANT_STRUCT_ITEM_COLUMNS;

	/**
	 * The number of structural features of the '<em>Union Row Creation Approach</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNION_ROW_CREATION_APPROACH_FEATURE_COUNT = ROW_CREATION_APPROACH_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Union Row Creation Approach</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNION_ROW_CREATION_APPROACH_OPERATION_COUNT = ROW_CREATION_APPROACH_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.ExplodeArrayOfStructsRowFunctionImpl <em>Explode Array Of Structs Row Function</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.ExplodeArrayOfStructsRowFunctionImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getExplodeArrayOfStructsRowFunction()
	 * @generated
	 */
	int EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION = 36;

	/**
	 * The feature id for the '<em><b>Dependant Cube Columns</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION__DEPENDANT_CUBE_COLUMNS = ROW_CREATION_APPROACH__DEPENDANT_CUBE_COLUMNS;

	/**
	 * The feature id for the '<em><b>Created Cube Columns</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION__CREATED_CUBE_COLUMNS = ROW_CREATION_APPROACH__CREATED_CUBE_COLUMNS;

	/**
	 * The feature id for the '<em><b>Dependant Struct Item Columns</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION__DEPENDANT_STRUCT_ITEM_COLUMNS = ROW_CREATION_APPROACH__DEPENDANT_STRUCT_ITEM_COLUMNS;

	/**
	 * The feature id for the '<em><b>Array Source Variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION__ARRAY_SOURCE_VARIABLE = ROW_CREATION_APPROACH_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Explode Array Of Structs Row Function</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION_FEATURE_COUNT = ROW_CREATION_APPROACH_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Explode Array Of Structs Row Function</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION_OPERATION_COUNT = ROW_CREATION_APPROACH_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.DerivedEntityModuleImpl <em>Derived Entity Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.DerivedEntityModuleImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getDerivedEntityModule()
	 * @generated
	 */
	int DERIVED_ENTITY_MODULE = 37;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_MODULE__DEPENDENCIES = Module_managementPackage.MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_MODULE__THE_DESCRIPTION = Module_managementPackage.MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_MODULE__LICENSE = Module_managementPackage.MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_MODULE__NAME = Module_managementPackage.MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_MODULE__VERSION = Module_managementPackage.MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_MODULE__LONG_NAME = Module_managementPackage.MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Derived Entities</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_MODULE__DERIVED_ENTITIES = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Derived Entity Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_MODULE_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Derived Entity Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_MODULE_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.IntermediateDerivedEntitiesModuleImpl <em>Intermediate Derived Entities Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.IntermediateDerivedEntitiesModuleImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getIntermediateDerivedEntitiesModule()
	 * @generated
	 */
	int INTERMEDIATE_DERIVED_ENTITIES_MODULE = 38;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITIES_MODULE__DEPENDENCIES = Module_managementPackage.MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITIES_MODULE__THE_DESCRIPTION = Module_managementPackage.MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITIES_MODULE__LICENSE = Module_managementPackage.MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITIES_MODULE__NAME = Module_managementPackage.MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITIES_MODULE__VERSION = Module_managementPackage.MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITIES_MODULE__LONG_NAME = Module_managementPackage.MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Intermediate Derived Entities</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITIES_MODULE__INTERMEDIATE_DERIVED_ENTITIES = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Intermediate Derived Entities Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITIES_MODULE_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Intermediate Derived Entities Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int INTERMEDIATE_DERIVED_ENTITIES_MODULE_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;


	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.GeneratedEntityModuleImpl <em>Generated Entity Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.GeneratedEntityModuleImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getGeneratedEntityModule()
	 * @generated
	 */
	int GENERATED_ENTITY_MODULE = 39;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY_MODULE__DEPENDENCIES = Module_managementPackage.MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY_MODULE__THE_DESCRIPTION = Module_managementPackage.MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY_MODULE__LICENSE = Module_managementPackage.MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY_MODULE__NAME = Module_managementPackage.MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY_MODULE__VERSION = Module_managementPackage.MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY_MODULE__LONG_NAME = Module_managementPackage.MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Generated Entities</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY_MODULE__GENERATED_ENTITIES = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Generated Entity Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY_MODULE_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Generated Entity Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY_MODULE_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;


	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntity <em>Derived Entity</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Derived Entity</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntity
	 * @generated
	 */
	EClass getDerivedEntity();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntity#getSourceEntities <em>Source Entities</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Source Entities</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntity#getSourceEntities()
	 * @see #getDerivedEntity()
	 * @generated
	 */
	EReference getDerivedEntity_SourceEntities();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntity#getDerivedAttributes <em>Derived Attributes</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Derived Attributes</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntity#getDerivedAttributes()
	 * @see #getDerivedEntity()
	 * @generated
	 */
	EReference getDerivedEntity_DerivedAttributes();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntity#getRowCreationApproachForEntity <em>Row Creation Approach For Entity</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Row Creation Approach For Entity</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntity#getRowCreationApproachForEntity()
	 * @see #getDerivedEntity()
	 * @generated
	 */
	EReference getDerivedEntity_RowCreationApproachForEntity();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntity#getCube <em>Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Cube</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntity#getCube()
	 * @see #getDerivedEntity()
	 * @generated
	 */
	EReference getDerivedEntity_Cube();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntity#getDerivedStructs <em>Derived Structs</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Derived Structs</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntity#getDerivedStructs()
	 * @see #getDerivedEntity()
	 * @generated
	 */
	EReference getDerivedEntity_DerivedStructs();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.IntermediateDerivedEntity <em>Intermediate Derived Entity</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Intermediate Derived Entity</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.IntermediateDerivedEntity
	 * @generated
	 */
	EClass getIntermediateDerivedEntity();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.GeneratedEntity <em>Generated Entity</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Generated Entity</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.GeneratedEntity
	 * @generated
	 */
	EClass getGeneratedEntity();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AggregateFunction <em>Aggregate Function</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Aggregate Function</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AggregateFunction
	 * @generated
	 */
	EClass getAggregateFunction();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AggregateFunction#getFunctionSpec <em>Function Spec</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Function Spec</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AggregateFunction#getFunctionSpec()
	 * @see #getAggregateFunction()
	 * @generated
	 */
	EReference getAggregateFunction_FunctionSpec();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AggregateFunctionSpec <em>Aggregate Function Spec</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Aggregate Function Spec</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AggregateFunctionSpec
	 * @generated
	 */
	EClass getAggregateFunctionSpec();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicFunction <em>Basic Function</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Basic Function</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicFunction
	 * @generated
	 */
	EClass getBasicFunction();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicFunction#getFunctionSpec <em>Function Spec</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Function Spec</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicFunction#getFunctionSpec()
	 * @see #getBasicFunction()
	 * @generated
	 */
	EReference getBasicFunction_FunctionSpec();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicFunctionSpec <em>Basic Function Spec</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Basic Function Spec</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicFunctionSpec
	 * @generated
	 */
	EClass getBasicFunctionSpec();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicFunctionSpec#isIsInfix <em>Is Infix</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Is Infix</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicFunctionSpec#isIsInfix()
	 * @see #getBasicFunctionSpec()
	 * @generated
	 */
	EAttribute getBasicFunctionSpec_IsInfix();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BooleanFunction <em>Boolean Function</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Boolean Function</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BooleanFunction
	 * @generated
	 */
	EClass getBooleanFunction();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Function <em>Function</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Function</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Function
	 * @generated
	 */
	EClass getFunction();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Function#getParameters <em>Parameters</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Parameters</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Function#getParameters()
	 * @see #getFunction()
	 * @generated
	 */
	EReference getFunction_Parameters();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Function#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Function#getName()
	 * @see #getFunction()
	 * @generated
	 */
	EAttribute getFunction_Name();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FunctionSpec <em>Function Spec</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Function Spec</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FunctionSpec
	 * @generated
	 */
	EClass getFunctionSpec();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FunctionSpec#getParameterSpecs <em>Parameter Specs</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Parameter Specs</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FunctionSpec#getParameterSpecs()
	 * @see #getFunctionSpec()
	 * @generated
	 */
	EReference getFunctionSpec_ParameterSpecs();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FunctionSpec#getFunctionalName <em>Functional Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Functional Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FunctionSpec#getFunctionalName()
	 * @see #getFunctionSpec()
	 * @generated
	 */
	EAttribute getFunctionSpec_FunctionalName();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FunctionSpec#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FunctionSpec#getName()
	 * @see #getFunctionSpec()
	 * @generated
	 */
	EAttribute getFunctionSpec_Name();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FunctionSpec#getSqlName <em>Sql Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Sql Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FunctionSpec#getSqlName()
	 * @see #getFunctionSpec()
	 * @generated
	 */
	EAttribute getFunctionSpec_SqlName();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FunctionSpecModule <em>Function Spec Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Function Spec Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FunctionSpecModule
	 * @generated
	 */
	EClass getFunctionSpecModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FunctionSpecModule#getFunctionSpecs <em>Function Specs</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Function Specs</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FunctionSpecModule#getFunctionSpecs()
	 * @see #getFunctionSpecModule()
	 * @generated
	 */
	EReference getFunctionSpecModule_FunctionSpecs();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ParamaterSpec <em>Paramater Spec</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Paramater Spec</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ParamaterSpec
	 * @generated
	 */
	EClass getParamaterSpec();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ParamaterSpec#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ParamaterSpec#getName()
	 * @see #getParamaterSpec()
	 * @generated
	 */
	EAttribute getParamaterSpec_Name();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Parameter <em>Parameter</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Parameter</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Parameter
	 * @generated
	 */
	EClass getParameter();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.MemberParameter <em>Member Parameter</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Member Parameter</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.MemberParameter
	 * @generated
	 */
	EClass getMemberParameter();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.MemberParameter#getParam <em>Param</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Param</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.MemberParameter#getParam()
	 * @see #getMemberParameter()
	 * @generated
	 */
	EReference getMemberParameter_Param();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ValueParameter <em>Value Parameter</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Value Parameter</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ValueParameter
	 * @generated
	 */
	EClass getValueParameter();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ValueParameter#getValue <em>Value</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Value</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ValueParameter#getValue()
	 * @see #getValueParameter()
	 * @generated
	 */
	EAttribute getValueParameter_Value();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ValueParameter#getDataType <em>Data Type</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Data Type</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ValueParameter#getDataType()
	 * @see #getValueParameter()
	 * @generated
	 */
	EReference getValueParameter_DataType();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AggregateDerivedAttribute <em>Aggregate Derived Attribute</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Aggregate Derived Attribute</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AggregateDerivedAttribute
	 * @generated
	 */
	EClass getAggregateDerivedAttribute();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AggregateDerivedAttribute#getAggregateFunction <em>Aggregate Function</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Aggregate Function</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AggregateDerivedAttribute#getAggregateFunction()
	 * @see #getAggregateDerivedAttribute()
	 * @generated
	 */
	EReference getAggregateDerivedAttribute_AggregateFunction();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicDerivedAttribute <em>Basic Derived Attribute</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Basic Derived Attribute</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicDerivedAttribute
	 * @generated
	 */
	EClass getBasicDerivedAttribute();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicDerivedAttribute#getBasicScenarioLogic <em>Basic Scenario Logic</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Basic Scenario Logic</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicDerivedAttribute#getBasicScenarioLogic()
	 * @see #getBasicDerivedAttribute()
	 * @generated
	 */
	EReference getBasicDerivedAttribute_BasicScenarioLogic();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicScenarioLogic <em>Basic Scenario Logic</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Basic Scenario Logic</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicScenarioLogic
	 * @generated
	 */
	EClass getBasicScenarioLogic();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicScenarioLogic#getBasicFunction <em>Basic Function</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Basic Function</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicScenarioLogic#getBasicFunction()
	 * @see #getBasicScenarioLogic()
	 * @generated
	 */
	EReference getBasicScenarioLogic_BasicFunction();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicScenarioLogic#getSceanrio <em>Sceanrio</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Sceanrio</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicScenarioLogic#getSceanrio()
	 * @see #getBasicScenarioLogic()
	 * @generated
	 */
	EReference getBasicScenarioLogic_Sceanrio();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedAttribute <em>Derived Attribute</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Derived Attribute</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedAttribute
	 * @generated
	 */
	EClass getDerivedAttribute();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedAttribute#getVariable <em>Variable</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Variable</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedAttribute#getVariable()
	 * @see #getDerivedAttribute()
	 * @generated
	 */
	EReference getDerivedAttribute_Variable();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedStruct <em>Derived Struct</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Derived Struct</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedStruct
	 * @generated
	 */
	EClass getDerivedStruct();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedStruct#getCube <em>Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Cube</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedStruct#getCube()
	 * @see #getDerivedStruct()
	 * @generated
	 */
	EReference getDerivedStruct_Cube();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BaseFeature <em>Base Feature</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Base Feature</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BaseFeature
	 * @generated
	 */
	EClass getBaseFeature();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BaseFeature#getVariable <em>Variable</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Variable</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BaseFeature#getVariable()
	 * @see #getBaseFeature()
	 * @generated
	 */
	EReference getBaseFeature_Variable();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedFeatureGroup <em>Derived Feature Group</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Derived Feature Group</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedFeatureGroup
	 * @generated
	 */
	EClass getDerivedFeatureGroup();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedFeatureGroup#getDerivedFeatures <em>Derived Features</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Derived Features</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedFeatureGroup#getDerivedFeatures()
	 * @see #getDerivedFeatureGroup()
	 * @generated
	 */
	EReference getDerivedFeatureGroup_DerivedFeatures();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FeatureParameter <em>Feature Parameter</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Feature Parameter</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FeatureParameter
	 * @generated
	 */
	EClass getFeatureParameter();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FeatureParameter#getFeature <em>Feature</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Feature</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FeatureParameter#getFeature()
	 * @see #getFeatureParameter()
	 * @generated
	 */
	EReference getFeatureParameter_Feature();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ProxyFeatureParameter <em>Proxy Feature Parameter</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Proxy Feature Parameter</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ProxyFeatureParameter
	 * @generated
	 */
	EClass getProxyFeatureParameter();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ProxyFeatureParameter#getEntity <em>Entity</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Entity</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ProxyFeatureParameter#getEntity()
	 * @see #getProxyFeatureParameter()
	 * @generated
	 */
	EReference getProxyFeatureParameter_Entity();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ProxyFeatureParameter#getVaraible <em>Varaible</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Varaible</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ProxyFeatureParameter#getVaraible()
	 * @see #getProxyFeatureParameter()
	 * @generated
	 */
	EReference getProxyFeatureParameter_Varaible();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.GetAttributeFromEntityDerivedFeature <em>Get Attribute From Entity Derived Feature</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Get Attribute From Entity Derived Feature</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.GetAttributeFromEntityDerivedFeature
	 * @generated
	 */
	EClass getGetAttributeFromEntityDerivedFeature();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.GetAttributeFromEntityDerivedFeature#getAttributeInEntity <em>Attribute In Entity</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Attribute In Entity</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.GetAttributeFromEntityDerivedFeature#getAttributeInEntity()
	 * @see #getGetAttributeFromEntityDerivedFeature()
	 * @generated
	 */
	EReference getGetAttributeFromEntityDerivedFeature_AttributeInEntity();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.GetAttributeFromEntityDerivedFeature#getEntityFeature <em>Entity Feature</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Entity Feature</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.GetAttributeFromEntityDerivedFeature#getEntityFeature()
	 * @see #getGetAttributeFromEntityDerivedFeature()
	 * @generated
	 */
	EReference getGetAttributeFromEntityDerivedFeature_EntityFeature();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AttributeFromEntityParameter <em>Attribute From Entity Parameter</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Attribute From Entity Parameter</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AttributeFromEntityParameter
	 * @generated
	 */
	EClass getAttributeFromEntityParameter();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AttributeFromEntityParameter#getAttributeInEntity <em>Attribute In Entity</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Attribute In Entity</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AttributeFromEntityParameter#getAttributeInEntity()
	 * @see #getAttributeFromEntityParameter()
	 * @generated
	 */
	EReference getAttributeFromEntityParameter_AttributeInEntity();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AttributeFromEntityParameter#getEntityFeature <em>Entity Feature</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Entity Feature</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AttributeFromEntityParameter#getEntityFeature()
	 * @see #getAttributeFromEntityParameter()
	 * @generated
	 */
	EReference getAttributeFromEntityParameter_EntityFeature();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ProxyAttributeFromEntityParameter <em>Proxy Attribute From Entity Parameter</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Proxy Attribute From Entity Parameter</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ProxyAttributeFromEntityParameter
	 * @generated
	 */
	EClass getProxyAttributeFromEntityParameter();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ProxyAttributeFromEntityParameter#getVariable <em>Variable</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Variable</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ProxyAttributeFromEntityParameter#getVariable()
	 * @see #getProxyAttributeFromEntityParameter()
	 * @generated
	 */
	EReference getProxyAttributeFromEntityParameter_Variable();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ProxyAttributeFromEntityParameter#getEntity <em>Entity</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Entity</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ProxyAttributeFromEntityParameter#getEntity()
	 * @see #getProxyAttributeFromEntityParameter()
	 * @generated
	 */
	EReference getProxyAttributeFromEntityParameter_Entity();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ProxyAttributeFromEntityParameter#getCube <em>Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Cube</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ProxyAttributeFromEntityParameter#getCube()
	 * @see #getProxyAttributeFromEntityParameter()
	 * @generated
	 */
	EReference getProxyAttributeFromEntityParameter_Cube();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.CustomRowCreationApproach <em>Custom Row Creation Approach</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Custom Row Creation Approach</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.CustomRowCreationApproach
	 * @generated
	 */
	EClass getCustomRowCreationApproach();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.CustomRowCreationApproach#getDependantVariables <em>Dependant Variables</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Dependant Variables</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.CustomRowCreationApproach#getDependantVariables()
	 * @see #getCustomRowCreationApproach()
	 * @generated
	 */
	EReference getCustomRowCreationApproach_DependantVariables();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FilterAndGroupToOneRowCreationApproach <em>Filter And Group To One Row Creation Approach</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Filter And Group To One Row Creation Approach</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FilterAndGroupToOneRowCreationApproach
	 * @generated
	 */
	EClass getFilterAndGroupToOneRowCreationApproach();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FilterAndGroupToOneRowCreationApproach#getFilterFunction <em>Filter Function</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Filter Function</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FilterAndGroupToOneRowCreationApproach#getFilterFunction()
	 * @see #getFilterAndGroupToOneRowCreationApproach()
	 * @generated
	 */
	EReference getFilterAndGroupToOneRowCreationApproach_FilterFunction();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FilterAndGroupToOneRowCreationApproach#getGroupByVariables <em>Group By Variables</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Group By Variables</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FilterAndGroupToOneRowCreationApproach#getGroupByVariables()
	 * @see #getFilterAndGroupToOneRowCreationApproach()
	 * @generated
	 */
	EReference getFilterAndGroupToOneRowCreationApproach_GroupByVariables();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.RowCreationApproachForEntity <em>Row Creation Approach For Entity</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Row Creation Approach For Entity</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.RowCreationApproachForEntity
	 * @generated
	 */
	EClass getRowCreationApproachForEntity();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.RowCreationApproachForEntity#getRowCreationApproach <em>Row Creation Approach</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Row Creation Approach</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.RowCreationApproachForEntity#getRowCreationApproach()
	 * @see #getRowCreationApproachForEntity()
	 * @generated
	 */
	EReference getRowCreationApproachForEntity_RowCreationApproach();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.RowCreationApproachForEntity#getEntity <em>Entity</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Entity</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.RowCreationApproachForEntity#getEntity()
	 * @see #getRowCreationApproachForEntity()
	 * @generated
	 */
	EReference getRowCreationApproachForEntity_Entity();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.RowCreationApproachForEntity#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.RowCreationApproachForEntity#getName()
	 * @see #getRowCreationApproachForEntity()
	 * @generated
	 */
	EAttribute getRowCreationApproachForEntity_Name();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.OneToOneRowCreationApproach <em>One To One Row Creation Approach</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>One To One Row Creation Approach</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.OneToOneRowCreationApproach
	 * @generated
	 */
	EClass getOneToOneRowCreationApproach();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FilterRowCreationApproach <em>Filter Row Creation Approach</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Filter Row Creation Approach</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FilterRowCreationApproach
	 * @generated
	 */
	EClass getFilterRowCreationApproach();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FilterRowCreationApproach#getFilterFunction <em>Filter Function</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Filter Function</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.FilterRowCreationApproach#getFilterFunction()
	 * @see #getFilterRowCreationApproach()
	 * @generated
	 */
	EReference getFilterRowCreationApproach_FilterFunction();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.RowCreationApproach <em>Row Creation Approach</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Row Creation Approach</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.RowCreationApproach
	 * @generated
	 */
	EClass getRowCreationApproach();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.RowCreationApproach#getDependantCubeColumns <em>Dependant Cube Columns</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Dependant Cube Columns</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.RowCreationApproach#getDependantCubeColumns()
	 * @see #getRowCreationApproach()
	 * @generated
	 */
	EReference getRowCreationApproach_DependantCubeColumns();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.RowCreationApproach#getCreatedCubeColumns <em>Created Cube Columns</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Created Cube Columns</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.RowCreationApproach#getCreatedCubeColumns()
	 * @see #getRowCreationApproach()
	 * @generated
	 */
	EReference getRowCreationApproach_CreatedCubeColumns();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.RowCreationApproach#getDependantStructItemColumns <em>Dependant Struct Item Columns</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Dependant Struct Item Columns</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.RowCreationApproach#getDependantStructItemColumns()
	 * @see #getRowCreationApproach()
	 * @generated
	 */
	EReference getRowCreationApproach_DependantStructItemColumns();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.GroupByRowCreationApproach <em>Group By Row Creation Approach</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Group By Row Creation Approach</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.GroupByRowCreationApproach
	 * @generated
	 */
	EClass getGroupByRowCreationApproach();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.GroupByRowCreationApproach#getGroupByCubeColumnNames <em>Group By Cube Column Names</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Group By Cube Column Names</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.GroupByRowCreationApproach#getGroupByCubeColumnNames()
	 * @see #getGroupByRowCreationApproach()
	 * @generated
	 */
	EReference getGroupByRowCreationApproach_GroupByCubeColumnNames();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.GroupByRowCreationApproach#getGroupByColumns <em>Group By Columns</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Group By Columns</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.GroupByRowCreationApproach#getGroupByColumns()
	 * @see #getGroupByRowCreationApproach()
	 * @generated
	 */
	EReference getGroupByRowCreationApproach_GroupByColumns();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.RowJoinFunction <em>Row Join Function</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Row Join Function</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.RowJoinFunction
	 * @generated
	 */
	EClass getRowJoinFunction();

	/**
	 * Returns the meta object for the containment reference '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.RowJoinFunction#getJoinFunction <em>Join Function</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference '<em>Join Function</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.RowJoinFunction#getJoinFunction()
	 * @see #getRowJoinFunction()
	 * @generated
	 */
	EReference getRowJoinFunction_JoinFunction();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.UnionRowCreationApproach <em>Union Row Creation Approach</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Union Row Creation Approach</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.UnionRowCreationApproach
	 * @generated
	 */
	EClass getUnionRowCreationApproach();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ExplodeArrayOfStructsRowFunction <em>Explode Array Of Structs Row Function</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Explode Array Of Structs Row Function</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ExplodeArrayOfStructsRowFunction
	 * @generated
	 */
	EClass getExplodeArrayOfStructsRowFunction();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ExplodeArrayOfStructsRowFunction#getArraySourceVariable <em>Array Source Variable</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Array Source Variable</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ExplodeArrayOfStructsRowFunction#getArraySourceVariable()
	 * @see #getExplodeArrayOfStructsRowFunction()
	 * @generated
	 */
	EReference getExplodeArrayOfStructsRowFunction_ArraySourceVariable();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntityModule <em>Derived Entity Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Derived Entity Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntityModule
	 * @generated
	 */
	EClass getDerivedEntityModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntityModule#getDerivedEntities <em>Derived Entities</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Derived Entities</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntityModule#getDerivedEntities()
	 * @see #getDerivedEntityModule()
	 * @generated
	 */
	EReference getDerivedEntityModule_DerivedEntities();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.IntermediateDerivedEntitiesModule <em>Intermediate Derived Entities Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Intermediate Derived Entities Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.IntermediateDerivedEntitiesModule
	 * @generated
	 */
	EClass getIntermediateDerivedEntitiesModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.IntermediateDerivedEntitiesModule#getIntermediateDerivedEntities <em>Intermediate Derived Entities</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Intermediate Derived Entities</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.IntermediateDerivedEntitiesModule#getIntermediateDerivedEntities()
	 * @see #getIntermediateDerivedEntitiesModule()
	 * @generated
	 */
	EReference getIntermediateDerivedEntitiesModule_IntermediateDerivedEntities();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.GeneratedEntityModule <em>Generated Entity Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Generated Entity Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.GeneratedEntityModule
	 * @generated
	 */
	EClass getGeneratedEntityModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.GeneratedEntityModule#getGeneratedEntities <em>Generated Entities</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Generated Entities</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.GeneratedEntityModule#getGeneratedEntities()
	 * @see #getGeneratedEntityModule()
	 * @generated
	 */
	EReference getGeneratedEntityModule_GeneratedEntities();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	Ecore_plus_logicFactory getEcore_plus_logicFactory();

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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.DerivedEntityImpl <em>Derived Entity</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.DerivedEntityImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getDerivedEntity()
		 * @generated
		 */
		EClass DERIVED_ENTITY = eINSTANCE.getDerivedEntity();

		/**
		 * The meta object literal for the '<em><b>Source Entities</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference DERIVED_ENTITY__SOURCE_ENTITIES = eINSTANCE.getDerivedEntity_SourceEntities();

		/**
		 * The meta object literal for the '<em><b>Derived Attributes</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference DERIVED_ENTITY__DERIVED_ATTRIBUTES = eINSTANCE.getDerivedEntity_DerivedAttributes();

		/**
		 * The meta object literal for the '<em><b>Row Creation Approach For Entity</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference DERIVED_ENTITY__ROW_CREATION_APPROACH_FOR_ENTITY = eINSTANCE.getDerivedEntity_RowCreationApproachForEntity();

		/**
		 * The meta object literal for the '<em><b>Cube</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference DERIVED_ENTITY__CUBE = eINSTANCE.getDerivedEntity_Cube();

		/**
		 * The meta object literal for the '<em><b>Derived Structs</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference DERIVED_ENTITY__DERIVED_STRUCTS = eINSTANCE.getDerivedEntity_DerivedStructs();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.IntermediateDerivedEntityImpl <em>Intermediate Derived Entity</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.IntermediateDerivedEntityImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getIntermediateDerivedEntity()
		 * @generated
		 */
		EClass INTERMEDIATE_DERIVED_ENTITY = eINSTANCE.getIntermediateDerivedEntity();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.GeneratedEntityImpl <em>Generated Entity</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.GeneratedEntityImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getGeneratedEntity()
		 * @generated
		 */
		EClass GENERATED_ENTITY = eINSTANCE.getGeneratedEntity();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.AggregateFunctionImpl <em>Aggregate Function</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.AggregateFunctionImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getAggregateFunction()
		 * @generated
		 */
		EClass AGGREGATE_FUNCTION = eINSTANCE.getAggregateFunction();

		/**
		 * The meta object literal for the '<em><b>Function Spec</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference AGGREGATE_FUNCTION__FUNCTION_SPEC = eINSTANCE.getAggregateFunction_FunctionSpec();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.AggregateFunctionSpecImpl <em>Aggregate Function Spec</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.AggregateFunctionSpecImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getAggregateFunctionSpec()
		 * @generated
		 */
		EClass AGGREGATE_FUNCTION_SPEC = eINSTANCE.getAggregateFunctionSpec();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BasicFunctionImpl <em>Basic Function</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BasicFunctionImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getBasicFunction()
		 * @generated
		 */
		EClass BASIC_FUNCTION = eINSTANCE.getBasicFunction();

		/**
		 * The meta object literal for the '<em><b>Function Spec</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BASIC_FUNCTION__FUNCTION_SPEC = eINSTANCE.getBasicFunction_FunctionSpec();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BasicFunctionSpecImpl <em>Basic Function Spec</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BasicFunctionSpecImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getBasicFunctionSpec()
		 * @generated
		 */
		EClass BASIC_FUNCTION_SPEC = eINSTANCE.getBasicFunctionSpec();

		/**
		 * The meta object literal for the '<em><b>Is Infix</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute BASIC_FUNCTION_SPEC__IS_INFIX = eINSTANCE.getBasicFunctionSpec_IsInfix();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BooleanFunctionImpl <em>Boolean Function</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BooleanFunctionImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getBooleanFunction()
		 * @generated
		 */
		EClass BOOLEAN_FUNCTION = eINSTANCE.getBooleanFunction();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.FunctionImpl <em>Function</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.FunctionImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getFunction()
		 * @generated
		 */
		EClass FUNCTION = eINSTANCE.getFunction();

		/**
		 * The meta object literal for the '<em><b>Parameters</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference FUNCTION__PARAMETERS = eINSTANCE.getFunction_Parameters();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FUNCTION__NAME = eINSTANCE.getFunction_Name();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.FunctionSpecImpl <em>Function Spec</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.FunctionSpecImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getFunctionSpec()
		 * @generated
		 */
		EClass FUNCTION_SPEC = eINSTANCE.getFunctionSpec();

		/**
		 * The meta object literal for the '<em><b>Parameter Specs</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference FUNCTION_SPEC__PARAMETER_SPECS = eINSTANCE.getFunctionSpec_ParameterSpecs();

		/**
		 * The meta object literal for the '<em><b>Functional Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FUNCTION_SPEC__FUNCTIONAL_NAME = eINSTANCE.getFunctionSpec_FunctionalName();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FUNCTION_SPEC__NAME = eINSTANCE.getFunctionSpec_Name();

		/**
		 * The meta object literal for the '<em><b>Sql Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute FUNCTION_SPEC__SQL_NAME = eINSTANCE.getFunctionSpec_SqlName();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.FunctionSpecModuleImpl <em>Function Spec Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.FunctionSpecModuleImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getFunctionSpecModule()
		 * @generated
		 */
		EClass FUNCTION_SPEC_MODULE = eINSTANCE.getFunctionSpecModule();

		/**
		 * The meta object literal for the '<em><b>Function Specs</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference FUNCTION_SPEC_MODULE__FUNCTION_SPECS = eINSTANCE.getFunctionSpecModule_FunctionSpecs();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.ParamaterSpecImpl <em>Paramater Spec</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.ParamaterSpecImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getParamaterSpec()
		 * @generated
		 */
		EClass PARAMATER_SPEC = eINSTANCE.getParamaterSpec();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute PARAMATER_SPEC__NAME = eINSTANCE.getParamaterSpec_Name();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.ParameterImpl <em>Parameter</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.ParameterImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getParameter()
		 * @generated
		 */
		EClass PARAMETER = eINSTANCE.getParameter();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.MemberParameterImpl <em>Member Parameter</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.MemberParameterImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getMemberParameter()
		 * @generated
		 */
		EClass MEMBER_PARAMETER = eINSTANCE.getMemberParameter();

		/**
		 * The meta object literal for the '<em><b>Param</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MEMBER_PARAMETER__PARAM = eINSTANCE.getMemberParameter_Param();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.ValueParameterImpl <em>Value Parameter</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.ValueParameterImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getValueParameter()
		 * @generated
		 */
		EClass VALUE_PARAMETER = eINSTANCE.getValueParameter();

		/**
		 * The meta object literal for the '<em><b>Value</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute VALUE_PARAMETER__VALUE = eINSTANCE.getValueParameter_Value();

		/**
		 * The meta object literal for the '<em><b>Data Type</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference VALUE_PARAMETER__DATA_TYPE = eINSTANCE.getValueParameter_DataType();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.AggregateDerivedAttributeImpl <em>Aggregate Derived Attribute</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.AggregateDerivedAttributeImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getAggregateDerivedAttribute()
		 * @generated
		 */
		EClass AGGREGATE_DERIVED_ATTRIBUTE = eINSTANCE.getAggregateDerivedAttribute();

		/**
		 * The meta object literal for the '<em><b>Aggregate Function</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference AGGREGATE_DERIVED_ATTRIBUTE__AGGREGATE_FUNCTION = eINSTANCE.getAggregateDerivedAttribute_AggregateFunction();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BasicDerivedAttributeImpl <em>Basic Derived Attribute</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BasicDerivedAttributeImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getBasicDerivedAttribute()
		 * @generated
		 */
		EClass BASIC_DERIVED_ATTRIBUTE = eINSTANCE.getBasicDerivedAttribute();

		/**
		 * The meta object literal for the '<em><b>Basic Scenario Logic</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BASIC_DERIVED_ATTRIBUTE__BASIC_SCENARIO_LOGIC = eINSTANCE.getBasicDerivedAttribute_BasicScenarioLogic();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BasicScenarioLogicImpl <em>Basic Scenario Logic</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BasicScenarioLogicImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getBasicScenarioLogic()
		 * @generated
		 */
		EClass BASIC_SCENARIO_LOGIC = eINSTANCE.getBasicScenarioLogic();

		/**
		 * The meta object literal for the '<em><b>Basic Function</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BASIC_SCENARIO_LOGIC__BASIC_FUNCTION = eINSTANCE.getBasicScenarioLogic_BasicFunction();

		/**
		 * The meta object literal for the '<em><b>Sceanrio</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BASIC_SCENARIO_LOGIC__SCEANRIO = eINSTANCE.getBasicScenarioLogic_Sceanrio();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.DerivedAttributeImpl <em>Derived Attribute</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.DerivedAttributeImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getDerivedAttribute()
		 * @generated
		 */
		EClass DERIVED_ATTRIBUTE = eINSTANCE.getDerivedAttribute();

		/**
		 * The meta object literal for the '<em><b>Variable</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference DERIVED_ATTRIBUTE__VARIABLE = eINSTANCE.getDerivedAttribute_Variable();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.DerivedStructImpl <em>Derived Struct</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.DerivedStructImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getDerivedStruct()
		 * @generated
		 */
		EClass DERIVED_STRUCT = eINSTANCE.getDerivedStruct();

		/**
		 * The meta object literal for the '<em><b>Cube</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference DERIVED_STRUCT__CUBE = eINSTANCE.getDerivedStruct_Cube();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BaseFeatureImpl <em>Base Feature</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BaseFeatureImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getBaseFeature()
		 * @generated
		 */
		EClass BASE_FEATURE = eINSTANCE.getBaseFeature();

		/**
		 * The meta object literal for the '<em><b>Variable</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BASE_FEATURE__VARIABLE = eINSTANCE.getBaseFeature_Variable();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.DerivedFeatureGroupImpl <em>Derived Feature Group</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.DerivedFeatureGroupImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getDerivedFeatureGroup()
		 * @generated
		 */
		EClass DERIVED_FEATURE_GROUP = eINSTANCE.getDerivedFeatureGroup();

		/**
		 * The meta object literal for the '<em><b>Derived Features</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference DERIVED_FEATURE_GROUP__DERIVED_FEATURES = eINSTANCE.getDerivedFeatureGroup_DerivedFeatures();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.FeatureParameterImpl <em>Feature Parameter</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.FeatureParameterImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getFeatureParameter()
		 * @generated
		 */
		EClass FEATURE_PARAMETER = eINSTANCE.getFeatureParameter();

		/**
		 * The meta object literal for the '<em><b>Feature</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference FEATURE_PARAMETER__FEATURE = eINSTANCE.getFeatureParameter_Feature();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.ProxyFeatureParameterImpl <em>Proxy Feature Parameter</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.ProxyFeatureParameterImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getProxyFeatureParameter()
		 * @generated
		 */
		EClass PROXY_FEATURE_PARAMETER = eINSTANCE.getProxyFeatureParameter();

		/**
		 * The meta object literal for the '<em><b>Entity</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference PROXY_FEATURE_PARAMETER__ENTITY = eINSTANCE.getProxyFeatureParameter_Entity();

		/**
		 * The meta object literal for the '<em><b>Varaible</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference PROXY_FEATURE_PARAMETER__VARAIBLE = eINSTANCE.getProxyFeatureParameter_Varaible();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.GetAttributeFromEntityDerivedFeatureImpl <em>Get Attribute From Entity Derived Feature</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.GetAttributeFromEntityDerivedFeatureImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getGetAttributeFromEntityDerivedFeature()
		 * @generated
		 */
		EClass GET_ATTRIBUTE_FROM_ENTITY_DERIVED_FEATURE = eINSTANCE.getGetAttributeFromEntityDerivedFeature();

		/**
		 * The meta object literal for the '<em><b>Attribute In Entity</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference GET_ATTRIBUTE_FROM_ENTITY_DERIVED_FEATURE__ATTRIBUTE_IN_ENTITY = eINSTANCE.getGetAttributeFromEntityDerivedFeature_AttributeInEntity();

		/**
		 * The meta object literal for the '<em><b>Entity Feature</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference GET_ATTRIBUTE_FROM_ENTITY_DERIVED_FEATURE__ENTITY_FEATURE = eINSTANCE.getGetAttributeFromEntityDerivedFeature_EntityFeature();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.AttributeFromEntityParameterImpl <em>Attribute From Entity Parameter</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.AttributeFromEntityParameterImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getAttributeFromEntityParameter()
		 * @generated
		 */
		EClass ATTRIBUTE_FROM_ENTITY_PARAMETER = eINSTANCE.getAttributeFromEntityParameter();

		/**
		 * The meta object literal for the '<em><b>Attribute In Entity</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ATTRIBUTE_FROM_ENTITY_PARAMETER__ATTRIBUTE_IN_ENTITY = eINSTANCE.getAttributeFromEntityParameter_AttributeInEntity();

		/**
		 * The meta object literal for the '<em><b>Entity Feature</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ATTRIBUTE_FROM_ENTITY_PARAMETER__ENTITY_FEATURE = eINSTANCE.getAttributeFromEntityParameter_EntityFeature();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.ProxyAttributeFromEntityParameterImpl <em>Proxy Attribute From Entity Parameter</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.ProxyAttributeFromEntityParameterImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getProxyAttributeFromEntityParameter()
		 * @generated
		 */
		EClass PROXY_ATTRIBUTE_FROM_ENTITY_PARAMETER = eINSTANCE.getProxyAttributeFromEntityParameter();

		/**
		 * The meta object literal for the '<em><b>Variable</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference PROXY_ATTRIBUTE_FROM_ENTITY_PARAMETER__VARIABLE = eINSTANCE.getProxyAttributeFromEntityParameter_Variable();

		/**
		 * The meta object literal for the '<em><b>Entity</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference PROXY_ATTRIBUTE_FROM_ENTITY_PARAMETER__ENTITY = eINSTANCE.getProxyAttributeFromEntityParameter_Entity();

		/**
		 * The meta object literal for the '<em><b>Cube</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference PROXY_ATTRIBUTE_FROM_ENTITY_PARAMETER__CUBE = eINSTANCE.getProxyAttributeFromEntityParameter_Cube();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.CustomRowCreationApproachImpl <em>Custom Row Creation Approach</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.CustomRowCreationApproachImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getCustomRowCreationApproach()
		 * @generated
		 */
		EClass CUSTOM_ROW_CREATION_APPROACH = eINSTANCE.getCustomRowCreationApproach();

		/**
		 * The meta object literal for the '<em><b>Dependant Variables</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CUSTOM_ROW_CREATION_APPROACH__DEPENDANT_VARIABLES = eINSTANCE.getCustomRowCreationApproach_DependantVariables();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.FilterAndGroupToOneRowCreationApproachImpl <em>Filter And Group To One Row Creation Approach</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.FilterAndGroupToOneRowCreationApproachImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getFilterAndGroupToOneRowCreationApproach()
		 * @generated
		 */
		EClass FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH = eINSTANCE.getFilterAndGroupToOneRowCreationApproach();

		/**
		 * The meta object literal for the '<em><b>Filter Function</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH__FILTER_FUNCTION = eINSTANCE.getFilterAndGroupToOneRowCreationApproach_FilterFunction();

		/**
		 * The meta object literal for the '<em><b>Group By Variables</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH__GROUP_BY_VARIABLES = eINSTANCE.getFilterAndGroupToOneRowCreationApproach_GroupByVariables();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.RowCreationApproachForEntityImpl <em>Row Creation Approach For Entity</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.RowCreationApproachForEntityImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getRowCreationApproachForEntity()
		 * @generated
		 */
		EClass ROW_CREATION_APPROACH_FOR_ENTITY = eINSTANCE.getRowCreationApproachForEntity();

		/**
		 * The meta object literal for the '<em><b>Row Creation Approach</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ROW_CREATION_APPROACH_FOR_ENTITY__ROW_CREATION_APPROACH = eINSTANCE.getRowCreationApproachForEntity_RowCreationApproach();

		/**
		 * The meta object literal for the '<em><b>Entity</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ROW_CREATION_APPROACH_FOR_ENTITY__ENTITY = eINSTANCE.getRowCreationApproachForEntity_Entity();

		/**
		 * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute ROW_CREATION_APPROACH_FOR_ENTITY__NAME = eINSTANCE.getRowCreationApproachForEntity_Name();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.OneToOneRowCreationApproachImpl <em>One To One Row Creation Approach</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.OneToOneRowCreationApproachImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getOneToOneRowCreationApproach()
		 * @generated
		 */
		EClass ONE_TO_ONE_ROW_CREATION_APPROACH = eINSTANCE.getOneToOneRowCreationApproach();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.FilterRowCreationApproachImpl <em>Filter Row Creation Approach</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.FilterRowCreationApproachImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getFilterRowCreationApproach()
		 * @generated
		 */
		EClass FILTER_ROW_CREATION_APPROACH = eINSTANCE.getFilterRowCreationApproach();

		/**
		 * The meta object literal for the '<em><b>Filter Function</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference FILTER_ROW_CREATION_APPROACH__FILTER_FUNCTION = eINSTANCE.getFilterRowCreationApproach_FilterFunction();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.RowCreationApproachImpl <em>Row Creation Approach</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.RowCreationApproachImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getRowCreationApproach()
		 * @generated
		 */
		EClass ROW_CREATION_APPROACH = eINSTANCE.getRowCreationApproach();

		/**
		 * The meta object literal for the '<em><b>Dependant Cube Columns</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ROW_CREATION_APPROACH__DEPENDANT_CUBE_COLUMNS = eINSTANCE.getRowCreationApproach_DependantCubeColumns();

		/**
		 * The meta object literal for the '<em><b>Created Cube Columns</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ROW_CREATION_APPROACH__CREATED_CUBE_COLUMNS = eINSTANCE.getRowCreationApproach_CreatedCubeColumns();

		/**
		 * The meta object literal for the '<em><b>Dependant Struct Item Columns</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ROW_CREATION_APPROACH__DEPENDANT_STRUCT_ITEM_COLUMNS = eINSTANCE.getRowCreationApproach_DependantStructItemColumns();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.GroupByRowCreationApproachImpl <em>Group By Row Creation Approach</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.GroupByRowCreationApproachImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getGroupByRowCreationApproach()
		 * @generated
		 */
		EClass GROUP_BY_ROW_CREATION_APPROACH = eINSTANCE.getGroupByRowCreationApproach();

		/**
		 * The meta object literal for the '<em><b>Group By Cube Column Names</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference GROUP_BY_ROW_CREATION_APPROACH__GROUP_BY_CUBE_COLUMN_NAMES = eINSTANCE.getGroupByRowCreationApproach_GroupByCubeColumnNames();

		/**
		 * The meta object literal for the '<em><b>Group By Columns</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference GROUP_BY_ROW_CREATION_APPROACH__GROUP_BY_COLUMNS = eINSTANCE.getGroupByRowCreationApproach_GroupByColumns();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.RowJoinFunctionImpl <em>Row Join Function</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.RowJoinFunctionImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getRowJoinFunction()
		 * @generated
		 */
		EClass ROW_JOIN_FUNCTION = eINSTANCE.getRowJoinFunction();

		/**
		 * The meta object literal for the '<em><b>Join Function</b></em>' containment reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ROW_JOIN_FUNCTION__JOIN_FUNCTION = eINSTANCE.getRowJoinFunction_JoinFunction();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.UnionRowCreationApproachImpl <em>Union Row Creation Approach</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.UnionRowCreationApproachImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getUnionRowCreationApproach()
		 * @generated
		 */
		EClass UNION_ROW_CREATION_APPROACH = eINSTANCE.getUnionRowCreationApproach();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.ExplodeArrayOfStructsRowFunctionImpl <em>Explode Array Of Structs Row Function</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.ExplodeArrayOfStructsRowFunctionImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getExplodeArrayOfStructsRowFunction()
		 * @generated
		 */
		EClass EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION = eINSTANCE.getExplodeArrayOfStructsRowFunction();

		/**
		 * The meta object literal for the '<em><b>Array Source Variable</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION__ARRAY_SOURCE_VARIABLE = eINSTANCE.getExplodeArrayOfStructsRowFunction_ArraySourceVariable();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.DerivedEntityModuleImpl <em>Derived Entity Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.DerivedEntityModuleImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getDerivedEntityModule()
		 * @generated
		 */
		EClass DERIVED_ENTITY_MODULE = eINSTANCE.getDerivedEntityModule();

		/**
		 * The meta object literal for the '<em><b>Derived Entities</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference DERIVED_ENTITY_MODULE__DERIVED_ENTITIES = eINSTANCE.getDerivedEntityModule_DerivedEntities();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.IntermediateDerivedEntitiesModuleImpl <em>Intermediate Derived Entities Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.IntermediateDerivedEntitiesModuleImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getIntermediateDerivedEntitiesModule()
		 * @generated
		 */
		EClass INTERMEDIATE_DERIVED_ENTITIES_MODULE = eINSTANCE.getIntermediateDerivedEntitiesModule();

		/**
		 * The meta object literal for the '<em><b>Intermediate Derived Entities</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference INTERMEDIATE_DERIVED_ENTITIES_MODULE__INTERMEDIATE_DERIVED_ENTITIES = eINSTANCE.getIntermediateDerivedEntitiesModule_IntermediateDerivedEntities();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.GeneratedEntityModuleImpl <em>Generated Entity Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.GeneratedEntityModuleImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getGeneratedEntityModule()
		 * @generated
		 */
		EClass GENERATED_ENTITY_MODULE = eINSTANCE.getGeneratedEntityModule();

		/**
		 * The meta object literal for the '<em><b>Generated Entities</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference GENERATED_ENTITY_MODULE__GENERATED_ENTITIES = eINSTANCE.getGeneratedEntityModule_GeneratedEntities();

	}

} //Ecore_plus_logicPackage
