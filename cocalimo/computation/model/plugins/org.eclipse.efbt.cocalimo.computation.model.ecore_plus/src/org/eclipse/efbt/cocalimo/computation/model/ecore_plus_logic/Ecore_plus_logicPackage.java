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
	 * The number of structural features of the '<em>Derived Entity</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_FEATURE_COUNT = Ecore_plus_structurePackage.ENTITY_FEATURE_COUNT + 1;

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
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.FunctionSpecImpl <em>Function Spec</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.FunctionSpecImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getFunctionSpec()
	 * @generated
	 */
	int FUNCTION_SPEC = 6;

	/**
	 * The feature id for the '<em><b>Functional Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTION_SPEC__FUNCTIONAL_NAME = 0;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTION_SPEC__NAME = 1;

	/**
	 * The feature id for the '<em><b>Sql Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTION_SPEC__SQL_NAME = 2;

	/**
	 * The number of structural features of the '<em>Function Spec</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FUNCTION_SPEC_FEATURE_COUNT = 3;

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
	int AGGREGATE_FUNCTION_SPEC = 3;

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
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BasicFunctionSpecImpl <em>Basic Function Spec</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BasicFunctionSpecImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getBasicFunctionSpec()
	 * @generated
	 */
	int BASIC_FUNCTION_SPEC = 4;

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
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.FunctionSpecModuleImpl <em>Function Spec Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.FunctionSpecModuleImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getFunctionSpecModule()
	 * @generated
	 */
	int FUNCTION_SPEC_MODULE = 7;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.ParameterImpl <em>Parameter</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.ParameterImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getParameter()
	 * @generated
	 */
	int PARAMETER = 8;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.MemberParameterImpl <em>Member Parameter</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.MemberParameterImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getMemberParameter()
	 * @generated
	 */
	int MEMBER_PARAMETER = 9;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.ValueParameterImpl <em>Value Parameter</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.ValueParameterImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getValueParameter()
	 * @generated
	 */
	int VALUE_PARAMETER = 11;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.OperationImpl <em>Operation</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.OperationImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getOperation()
	 * @generated
	 */
	int OPERATION = 32;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int OPERATION__EANNOTATIONS = EcorePackage.EOPERATION__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int OPERATION__NAME = EcorePackage.EOPERATION__NAME;

	/**
	 * The feature id for the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int OPERATION__ORDERED = EcorePackage.EOPERATION__ORDERED;

	/**
	 * The feature id for the '<em><b>Unique</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int OPERATION__UNIQUE = EcorePackage.EOPERATION__UNIQUE;

	/**
	 * The feature id for the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int OPERATION__LOWER_BOUND = EcorePackage.EOPERATION__LOWER_BOUND;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int OPERATION__UPPER_BOUND = EcorePackage.EOPERATION__UPPER_BOUND;

	/**
	 * The feature id for the '<em><b>Many</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int OPERATION__MANY = EcorePackage.EOPERATION__MANY;

	/**
	 * The feature id for the '<em><b>Required</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int OPERATION__REQUIRED = EcorePackage.EOPERATION__REQUIRED;

	/**
	 * The feature id for the '<em><b>EType</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int OPERATION__ETYPE = EcorePackage.EOPERATION__ETYPE;

	/**
	 * The feature id for the '<em><b>EGeneric Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int OPERATION__EGENERIC_TYPE = EcorePackage.EOPERATION__EGENERIC_TYPE;

	/**
	 * The feature id for the '<em><b>EContaining Class</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int OPERATION__ECONTAINING_CLASS = EcorePackage.EOPERATION__ECONTAINING_CLASS;

	/**
	 * The feature id for the '<em><b>EType Parameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int OPERATION__ETYPE_PARAMETERS = EcorePackage.EOPERATION__ETYPE_PARAMETERS;

	/**
	 * The feature id for the '<em><b>EParameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int OPERATION__EPARAMETERS = EcorePackage.EOPERATION__EPARAMETERS;

	/**
	 * The feature id for the '<em><b>EExceptions</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int OPERATION__EEXCEPTIONS = EcorePackage.EOPERATION__EEXCEPTIONS;

	/**
	 * The feature id for the '<em><b>EGeneric Exceptions</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int OPERATION__EGENERIC_EXCEPTIONS = EcorePackage.EOPERATION__EGENERIC_EXCEPTIONS;

	/**
	 * The feature id for the '<em><b>EOperation</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int OPERATION__EOPERATION = EcorePackage.EOPERATION_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Operation</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int OPERATION_FEATURE_COUNT = EcorePackage.EOPERATION_FEATURE_COUNT + 1;

	/**
	 * The operation id for the '<em>Get EAnnotation</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int OPERATION___GET_EANNOTATION__STRING = EcorePackage.EOPERATION___GET_EANNOTATION__STRING;

	/**
	 * The operation id for the '<em>Get Operation ID</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int OPERATION___GET_OPERATION_ID = EcorePackage.EOPERATION___GET_OPERATION_ID;

	/**
	 * The operation id for the '<em>Is Override Of</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int OPERATION___IS_OVERRIDE_OF__EOPERATION = EcorePackage.EOPERATION___IS_OVERRIDE_OF__EOPERATION;

	/**
	 * The number of operations of the '<em>Operation</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int OPERATION_OPERATION_COUNT = EcorePackage.EOPERATION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BasicScenarioOperationImpl <em>Basic Scenario Operation</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BasicScenarioOperationImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getBasicScenarioOperation()
	 * @generated
	 */
	int BASIC_SCENARIO_OPERATION = 12;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.FeatureParameterImpl <em>Feature Parameter</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.FeatureParameterImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getFeatureParameter()
	 * @generated
	 */
	int FEATURE_PARAMETER = 13;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.ProxyOperationParameterImpl <em>Proxy Operation Parameter</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.ProxyOperationParameterImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getProxyOperationParameter()
	 * @generated
	 */
	int PROXY_OPERATION_PARAMETER = 15;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.ProxyFeatureParameterImpl <em>Proxy Feature Parameter</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.ProxyFeatureParameterImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getProxyFeatureParameter()
	 * @generated
	 */
	int PROXY_FEATURE_PARAMETER = 16;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.RowCreationApproachImpl <em>Row Creation Approach</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.RowCreationApproachImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getRowCreationApproach()
	 * @generated
	 */
	int ROW_CREATION_APPROACH = 21;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.CustomRowCreationApproachImpl <em>Custom Row Creation Approach</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.CustomRowCreationApproachImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getCustomRowCreationApproach()
	 * @generated
	 */
	int CUSTOM_ROW_CREATION_APPROACH = 17;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.FilterAndGroupToOneRowCreationApproachImpl <em>Filter And Group To One Row Creation Approach</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.FilterAndGroupToOneRowCreationApproachImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getFilterAndGroupToOneRowCreationApproach()
	 * @generated
	 */
	int FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH = 18;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.OneToOneRowCreationApproachImpl <em>One To One Row Creation Approach</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.OneToOneRowCreationApproachImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getOneToOneRowCreationApproach()
	 * @generated
	 */
	int ONE_TO_ONE_ROW_CREATION_APPROACH = 19;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.FilterRowCreationApproachImpl <em>Filter Row Creation Approach</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.FilterRowCreationApproachImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getFilterRowCreationApproach()
	 * @generated
	 */
	int FILTER_ROW_CREATION_APPROACH = 20;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.GroupByRowCreationApproachImpl <em>Group By Row Creation Approach</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.GroupByRowCreationApproachImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getGroupByRowCreationApproach()
	 * @generated
	 */
	int GROUP_BY_ROW_CREATION_APPROACH = 22;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.RowJoinFunctionImpl <em>Row Join Function</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.RowJoinFunctionImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getRowJoinFunction()
	 * @generated
	 */
	int ROW_JOIN_FUNCTION = 23;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.UnionRowCreationApproachImpl <em>Union Row Creation Approach</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.UnionRowCreationApproachImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getUnionRowCreationApproach()
	 * @generated
	 */
	int UNION_ROW_CREATION_APPROACH = 24;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.ExplodeArrayOfStructsRowFunctionImpl <em>Explode Array Of Structs Row Function</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.ExplodeArrayOfStructsRowFunctionImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getExplodeArrayOfStructsRowFunction()
	 * @generated
	 */
	int EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION = 25;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.DerivedEntityModuleImpl <em>Derived Entity Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.DerivedEntityModuleImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getDerivedEntityModule()
	 * @generated
	 */
	int DERIVED_ENTITY_MODULE = 26;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.IntermediateDerivedEntitiesModuleImpl <em>Intermediate Derived Entities Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.IntermediateDerivedEntitiesModuleImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getIntermediateDerivedEntitiesModule()
	 * @generated
	 */
	int INTERMEDIATE_DERIVED_ENTITIES_MODULE = 27;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.GeneratedEntityModuleImpl <em>Generated Entity Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.GeneratedEntityModuleImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getGeneratedEntityModule()
	 * @generated
	 */
	int GENERATED_ENTITY_MODULE = 28;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.EntityTableImpl <em>Entity Table</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.EntityTableImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getEntityTable()
	 * @generated
	 */
	int ENTITY_TABLE = 29;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.DerivedEntityTableImpl <em>Derived Entity Table</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.DerivedEntityTableImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getDerivedEntityTable()
	 * @generated
	 */
	int DERIVED_ENTITY_TABLE = 30;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BaseEntityTableImpl <em>Base Entity Table</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BaseEntityTableImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getBaseEntityTable()
	 * @generated
	 */
	int BASE_ENTITY_TABLE = 31;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BasicOperationImpl <em>Basic Operation</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BasicOperationImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getBasicOperation()
	 * @generated
	 */
	int BASIC_OPERATION = 33;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_OPERATION__EANNOTATIONS = OPERATION__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_OPERATION__NAME = OPERATION__NAME;

	/**
	 * The feature id for the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_OPERATION__ORDERED = OPERATION__ORDERED;

	/**
	 * The feature id for the '<em><b>Unique</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_OPERATION__UNIQUE = OPERATION__UNIQUE;

	/**
	 * The feature id for the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_OPERATION__LOWER_BOUND = OPERATION__LOWER_BOUND;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_OPERATION__UPPER_BOUND = OPERATION__UPPER_BOUND;

	/**
	 * The feature id for the '<em><b>Many</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_OPERATION__MANY = OPERATION__MANY;

	/**
	 * The feature id for the '<em><b>Required</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_OPERATION__REQUIRED = OPERATION__REQUIRED;

	/**
	 * The feature id for the '<em><b>EType</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_OPERATION__ETYPE = OPERATION__ETYPE;

	/**
	 * The feature id for the '<em><b>EGeneric Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_OPERATION__EGENERIC_TYPE = OPERATION__EGENERIC_TYPE;

	/**
	 * The feature id for the '<em><b>EContaining Class</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_OPERATION__ECONTAINING_CLASS = OPERATION__ECONTAINING_CLASS;

	/**
	 * The feature id for the '<em><b>EType Parameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_OPERATION__ETYPE_PARAMETERS = OPERATION__ETYPE_PARAMETERS;

	/**
	 * The feature id for the '<em><b>EParameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_OPERATION__EPARAMETERS = OPERATION__EPARAMETERS;

	/**
	 * The feature id for the '<em><b>EExceptions</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_OPERATION__EEXCEPTIONS = OPERATION__EEXCEPTIONS;

	/**
	 * The feature id for the '<em><b>EGeneric Exceptions</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_OPERATION__EGENERIC_EXCEPTIONS = OPERATION__EGENERIC_EXCEPTIONS;

	/**
	 * The feature id for the '<em><b>EOperation</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_OPERATION__EOPERATION = OPERATION__EOPERATION;

	/**
	 * The feature id for the '<em><b>Function Spec</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_OPERATION__FUNCTION_SPEC = OPERATION_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Basic Operation</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_OPERATION_FEATURE_COUNT = OPERATION_FEATURE_COUNT + 1;

	/**
	 * The operation id for the '<em>Get EAnnotation</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_OPERATION___GET_EANNOTATION__STRING = OPERATION___GET_EANNOTATION__STRING;

	/**
	 * The operation id for the '<em>Get Operation ID</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_OPERATION___GET_OPERATION_ID = OPERATION___GET_OPERATION_ID;

	/**
	 * The operation id for the '<em>Is Override Of</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_OPERATION___IS_OVERRIDE_OF__EOPERATION = OPERATION___IS_OVERRIDE_OF__EOPERATION;

	/**
	 * The number of operations of the '<em>Basic Operation</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_OPERATION_OPERATION_COUNT = OPERATION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BooleanOperationImpl <em>Boolean Operation</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BooleanOperationImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getBooleanOperation()
	 * @generated
	 */
	int BOOLEAN_OPERATION = 5;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BOOLEAN_OPERATION__EANNOTATIONS = BASIC_OPERATION__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BOOLEAN_OPERATION__NAME = BASIC_OPERATION__NAME;

	/**
	 * The feature id for the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BOOLEAN_OPERATION__ORDERED = BASIC_OPERATION__ORDERED;

	/**
	 * The feature id for the '<em><b>Unique</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BOOLEAN_OPERATION__UNIQUE = BASIC_OPERATION__UNIQUE;

	/**
	 * The feature id for the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BOOLEAN_OPERATION__LOWER_BOUND = BASIC_OPERATION__LOWER_BOUND;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BOOLEAN_OPERATION__UPPER_BOUND = BASIC_OPERATION__UPPER_BOUND;

	/**
	 * The feature id for the '<em><b>Many</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BOOLEAN_OPERATION__MANY = BASIC_OPERATION__MANY;

	/**
	 * The feature id for the '<em><b>Required</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BOOLEAN_OPERATION__REQUIRED = BASIC_OPERATION__REQUIRED;

	/**
	 * The feature id for the '<em><b>EType</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BOOLEAN_OPERATION__ETYPE = BASIC_OPERATION__ETYPE;

	/**
	 * The feature id for the '<em><b>EGeneric Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BOOLEAN_OPERATION__EGENERIC_TYPE = BASIC_OPERATION__EGENERIC_TYPE;

	/**
	 * The feature id for the '<em><b>EContaining Class</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BOOLEAN_OPERATION__ECONTAINING_CLASS = BASIC_OPERATION__ECONTAINING_CLASS;

	/**
	 * The feature id for the '<em><b>EType Parameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BOOLEAN_OPERATION__ETYPE_PARAMETERS = BASIC_OPERATION__ETYPE_PARAMETERS;

	/**
	 * The feature id for the '<em><b>EParameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BOOLEAN_OPERATION__EPARAMETERS = BASIC_OPERATION__EPARAMETERS;

	/**
	 * The feature id for the '<em><b>EExceptions</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BOOLEAN_OPERATION__EEXCEPTIONS = BASIC_OPERATION__EEXCEPTIONS;

	/**
	 * The feature id for the '<em><b>EGeneric Exceptions</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BOOLEAN_OPERATION__EGENERIC_EXCEPTIONS = BASIC_OPERATION__EGENERIC_EXCEPTIONS;

	/**
	 * The feature id for the '<em><b>EOperation</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BOOLEAN_OPERATION__EOPERATION = BASIC_OPERATION__EOPERATION;

	/**
	 * The feature id for the '<em><b>Function Spec</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BOOLEAN_OPERATION__FUNCTION_SPEC = BASIC_OPERATION__FUNCTION_SPEC;

	/**
	 * The number of structural features of the '<em>Boolean Operation</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BOOLEAN_OPERATION_FEATURE_COUNT = BASIC_OPERATION_FEATURE_COUNT + 0;

	/**
	 * The operation id for the '<em>Get EAnnotation</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BOOLEAN_OPERATION___GET_EANNOTATION__STRING = BASIC_OPERATION___GET_EANNOTATION__STRING;

	/**
	 * The operation id for the '<em>Get Operation ID</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BOOLEAN_OPERATION___GET_OPERATION_ID = BASIC_OPERATION___GET_OPERATION_ID;

	/**
	 * The operation id for the '<em>Is Override Of</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BOOLEAN_OPERATION___IS_OVERRIDE_OF__EOPERATION = BASIC_OPERATION___IS_OVERRIDE_OF__EOPERATION;

	/**
	 * The number of operations of the '<em>Boolean Operation</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BOOLEAN_OPERATION_OPERATION_COUNT = BASIC_OPERATION_OPERATION_COUNT + 0;

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
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARAMETER__EANNOTATIONS = EcorePackage.EPARAMETER__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARAMETER__NAME = EcorePackage.EPARAMETER__NAME;

	/**
	 * The feature id for the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARAMETER__ORDERED = EcorePackage.EPARAMETER__ORDERED;

	/**
	 * The feature id for the '<em><b>Unique</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARAMETER__UNIQUE = EcorePackage.EPARAMETER__UNIQUE;

	/**
	 * The feature id for the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARAMETER__LOWER_BOUND = EcorePackage.EPARAMETER__LOWER_BOUND;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARAMETER__UPPER_BOUND = EcorePackage.EPARAMETER__UPPER_BOUND;

	/**
	 * The feature id for the '<em><b>Many</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARAMETER__MANY = EcorePackage.EPARAMETER__MANY;

	/**
	 * The feature id for the '<em><b>Required</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARAMETER__REQUIRED = EcorePackage.EPARAMETER__REQUIRED;

	/**
	 * The feature id for the '<em><b>EType</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARAMETER__ETYPE = EcorePackage.EPARAMETER__ETYPE;

	/**
	 * The feature id for the '<em><b>EGeneric Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARAMETER__EGENERIC_TYPE = EcorePackage.EPARAMETER__EGENERIC_TYPE;

	/**
	 * The feature id for the '<em><b>EOperation</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARAMETER__EOPERATION = EcorePackage.EPARAMETER__EOPERATION;

	/**
	 * The number of structural features of the '<em>Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARAMETER_FEATURE_COUNT = EcorePackage.EPARAMETER_FEATURE_COUNT + 0;

	/**
	 * The operation id for the '<em>Get EAnnotation</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARAMETER___GET_EANNOTATION__STRING = EcorePackage.EPARAMETER___GET_EANNOTATION__STRING;

	/**
	 * The number of operations of the '<em>Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PARAMETER_OPERATION_COUNT = EcorePackage.EPARAMETER_OPERATION_COUNT + 0;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_PARAMETER__EANNOTATIONS = PARAMETER__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_PARAMETER__NAME = PARAMETER__NAME;

	/**
	 * The feature id for the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_PARAMETER__ORDERED = PARAMETER__ORDERED;

	/**
	 * The feature id for the '<em><b>Unique</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_PARAMETER__UNIQUE = PARAMETER__UNIQUE;

	/**
	 * The feature id for the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_PARAMETER__LOWER_BOUND = PARAMETER__LOWER_BOUND;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_PARAMETER__UPPER_BOUND = PARAMETER__UPPER_BOUND;

	/**
	 * The feature id for the '<em><b>Many</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_PARAMETER__MANY = PARAMETER__MANY;

	/**
	 * The feature id for the '<em><b>Required</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_PARAMETER__REQUIRED = PARAMETER__REQUIRED;

	/**
	 * The feature id for the '<em><b>EType</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_PARAMETER__ETYPE = PARAMETER__ETYPE;

	/**
	 * The feature id for the '<em><b>EGeneric Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_PARAMETER__EGENERIC_TYPE = PARAMETER__EGENERIC_TYPE;

	/**
	 * The feature id for the '<em><b>EOperation</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_PARAMETER__EOPERATION = PARAMETER__EOPERATION;

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
	 * The operation id for the '<em>Get EAnnotation</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_PARAMETER___GET_EANNOTATION__STRING = PARAMETER___GET_EANNOTATION__STRING;

	/**
	 * The number of operations of the '<em>Member Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_PARAMETER_OPERATION_COUNT = PARAMETER_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.EntityParameterImpl <em>Entity Parameter</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.EntityParameterImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getEntityParameter()
	 * @generated
	 */
	int ENTITY_PARAMETER = 10;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_PARAMETER__EANNOTATIONS = PARAMETER__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_PARAMETER__NAME = PARAMETER__NAME;

	/**
	 * The feature id for the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_PARAMETER__ORDERED = PARAMETER__ORDERED;

	/**
	 * The feature id for the '<em><b>Unique</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_PARAMETER__UNIQUE = PARAMETER__UNIQUE;

	/**
	 * The feature id for the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_PARAMETER__LOWER_BOUND = PARAMETER__LOWER_BOUND;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_PARAMETER__UPPER_BOUND = PARAMETER__UPPER_BOUND;

	/**
	 * The feature id for the '<em><b>Many</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_PARAMETER__MANY = PARAMETER__MANY;

	/**
	 * The feature id for the '<em><b>Required</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_PARAMETER__REQUIRED = PARAMETER__REQUIRED;

	/**
	 * The feature id for the '<em><b>EType</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_PARAMETER__ETYPE = PARAMETER__ETYPE;

	/**
	 * The feature id for the '<em><b>EGeneric Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_PARAMETER__EGENERIC_TYPE = PARAMETER__EGENERIC_TYPE;

	/**
	 * The feature id for the '<em><b>EOperation</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_PARAMETER__EOPERATION = PARAMETER__EOPERATION;

	/**
	 * The feature id for the '<em><b>Entity</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_PARAMETER__ENTITY = PARAMETER_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Entity Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_PARAMETER_FEATURE_COUNT = PARAMETER_FEATURE_COUNT + 1;

	/**
	 * The operation id for the '<em>Get EAnnotation</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_PARAMETER___GET_EANNOTATION__STRING = PARAMETER___GET_EANNOTATION__STRING;

	/**
	 * The number of operations of the '<em>Entity Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_PARAMETER_OPERATION_COUNT = PARAMETER_OPERATION_COUNT + 0;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VALUE_PARAMETER__EANNOTATIONS = PARAMETER__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VALUE_PARAMETER__NAME = PARAMETER__NAME;

	/**
	 * The feature id for the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VALUE_PARAMETER__ORDERED = PARAMETER__ORDERED;

	/**
	 * The feature id for the '<em><b>Unique</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VALUE_PARAMETER__UNIQUE = PARAMETER__UNIQUE;

	/**
	 * The feature id for the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VALUE_PARAMETER__LOWER_BOUND = PARAMETER__LOWER_BOUND;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VALUE_PARAMETER__UPPER_BOUND = PARAMETER__UPPER_BOUND;

	/**
	 * The feature id for the '<em><b>Many</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VALUE_PARAMETER__MANY = PARAMETER__MANY;

	/**
	 * The feature id for the '<em><b>Required</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VALUE_PARAMETER__REQUIRED = PARAMETER__REQUIRED;

	/**
	 * The feature id for the '<em><b>EType</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VALUE_PARAMETER__ETYPE = PARAMETER__ETYPE;

	/**
	 * The feature id for the '<em><b>EGeneric Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VALUE_PARAMETER__EGENERIC_TYPE = PARAMETER__EGENERIC_TYPE;

	/**
	 * The feature id for the '<em><b>EOperation</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VALUE_PARAMETER__EOPERATION = PARAMETER__EOPERATION;

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
	 * The operation id for the '<em>Get EAnnotation</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VALUE_PARAMETER___GET_EANNOTATION__STRING = PARAMETER___GET_EANNOTATION__STRING;

	/**
	 * The number of operations of the '<em>Value Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VALUE_PARAMETER_OPERATION_COUNT = PARAMETER_OPERATION_COUNT + 0;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_SCENARIO_OPERATION__EANNOTATIONS = OPERATION__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_SCENARIO_OPERATION__NAME = OPERATION__NAME;

	/**
	 * The feature id for the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_SCENARIO_OPERATION__ORDERED = OPERATION__ORDERED;

	/**
	 * The feature id for the '<em><b>Unique</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_SCENARIO_OPERATION__UNIQUE = OPERATION__UNIQUE;

	/**
	 * The feature id for the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_SCENARIO_OPERATION__LOWER_BOUND = OPERATION__LOWER_BOUND;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_SCENARIO_OPERATION__UPPER_BOUND = OPERATION__UPPER_BOUND;

	/**
	 * The feature id for the '<em><b>Many</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_SCENARIO_OPERATION__MANY = OPERATION__MANY;

	/**
	 * The feature id for the '<em><b>Required</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_SCENARIO_OPERATION__REQUIRED = OPERATION__REQUIRED;

	/**
	 * The feature id for the '<em><b>EType</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_SCENARIO_OPERATION__ETYPE = OPERATION__ETYPE;

	/**
	 * The feature id for the '<em><b>EGeneric Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_SCENARIO_OPERATION__EGENERIC_TYPE = OPERATION__EGENERIC_TYPE;

	/**
	 * The feature id for the '<em><b>EContaining Class</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_SCENARIO_OPERATION__ECONTAINING_CLASS = OPERATION__ECONTAINING_CLASS;

	/**
	 * The feature id for the '<em><b>EType Parameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_SCENARIO_OPERATION__ETYPE_PARAMETERS = OPERATION__ETYPE_PARAMETERS;

	/**
	 * The feature id for the '<em><b>EParameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_SCENARIO_OPERATION__EPARAMETERS = OPERATION__EPARAMETERS;

	/**
	 * The feature id for the '<em><b>EExceptions</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_SCENARIO_OPERATION__EEXCEPTIONS = OPERATION__EEXCEPTIONS;

	/**
	 * The feature id for the '<em><b>EGeneric Exceptions</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_SCENARIO_OPERATION__EGENERIC_EXCEPTIONS = OPERATION__EGENERIC_EXCEPTIONS;

	/**
	 * The feature id for the '<em><b>EOperation</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_SCENARIO_OPERATION__EOPERATION = OPERATION__EOPERATION;

	/**
	 * The feature id for the '<em><b>Sceanrio</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_SCENARIO_OPERATION__SCEANRIO = OPERATION_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Function Spec</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_SCENARIO_OPERATION__FUNCTION_SPEC = OPERATION_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Basic Scenario Operation</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_SCENARIO_OPERATION_FEATURE_COUNT = OPERATION_FEATURE_COUNT + 2;

	/**
	 * The operation id for the '<em>Get EAnnotation</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_SCENARIO_OPERATION___GET_EANNOTATION__STRING = OPERATION___GET_EANNOTATION__STRING;

	/**
	 * The operation id for the '<em>Get Operation ID</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_SCENARIO_OPERATION___GET_OPERATION_ID = OPERATION___GET_OPERATION_ID;

	/**
	 * The operation id for the '<em>Is Override Of</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_SCENARIO_OPERATION___IS_OVERRIDE_OF__EOPERATION = OPERATION___IS_OVERRIDE_OF__EOPERATION;

	/**
	 * The number of operations of the '<em>Basic Scenario Operation</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_SCENARIO_OPERATION_OPERATION_COUNT = OPERATION_OPERATION_COUNT + 0;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FEATURE_PARAMETER__EANNOTATIONS = PARAMETER__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FEATURE_PARAMETER__NAME = PARAMETER__NAME;

	/**
	 * The feature id for the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FEATURE_PARAMETER__ORDERED = PARAMETER__ORDERED;

	/**
	 * The feature id for the '<em><b>Unique</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FEATURE_PARAMETER__UNIQUE = PARAMETER__UNIQUE;

	/**
	 * The feature id for the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FEATURE_PARAMETER__LOWER_BOUND = PARAMETER__LOWER_BOUND;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FEATURE_PARAMETER__UPPER_BOUND = PARAMETER__UPPER_BOUND;

	/**
	 * The feature id for the '<em><b>Many</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FEATURE_PARAMETER__MANY = PARAMETER__MANY;

	/**
	 * The feature id for the '<em><b>Required</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FEATURE_PARAMETER__REQUIRED = PARAMETER__REQUIRED;

	/**
	 * The feature id for the '<em><b>EType</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FEATURE_PARAMETER__ETYPE = PARAMETER__ETYPE;

	/**
	 * The feature id for the '<em><b>EGeneric Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FEATURE_PARAMETER__EGENERIC_TYPE = PARAMETER__EGENERIC_TYPE;

	/**
	 * The feature id for the '<em><b>EOperation</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FEATURE_PARAMETER__EOPERATION = PARAMETER__EOPERATION;

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
	 * The operation id for the '<em>Get EAnnotation</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FEATURE_PARAMETER___GET_EANNOTATION__STRING = PARAMETER___GET_EANNOTATION__STRING;

	/**
	 * The number of operations of the '<em>Feature Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FEATURE_PARAMETER_OPERATION_COUNT = PARAMETER_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.OperationParameterImpl <em>Operation Parameter</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.OperationParameterImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getOperationParameter()
	 * @generated
	 */
	int OPERATION_PARAMETER = 14;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int OPERATION_PARAMETER__EANNOTATIONS = PARAMETER__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int OPERATION_PARAMETER__NAME = PARAMETER__NAME;

	/**
	 * The feature id for the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int OPERATION_PARAMETER__ORDERED = PARAMETER__ORDERED;

	/**
	 * The feature id for the '<em><b>Unique</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int OPERATION_PARAMETER__UNIQUE = PARAMETER__UNIQUE;

	/**
	 * The feature id for the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int OPERATION_PARAMETER__LOWER_BOUND = PARAMETER__LOWER_BOUND;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int OPERATION_PARAMETER__UPPER_BOUND = PARAMETER__UPPER_BOUND;

	/**
	 * The feature id for the '<em><b>Many</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int OPERATION_PARAMETER__MANY = PARAMETER__MANY;

	/**
	 * The feature id for the '<em><b>Required</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int OPERATION_PARAMETER__REQUIRED = PARAMETER__REQUIRED;

	/**
	 * The feature id for the '<em><b>EType</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int OPERATION_PARAMETER__ETYPE = PARAMETER__ETYPE;

	/**
	 * The feature id for the '<em><b>EGeneric Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int OPERATION_PARAMETER__EGENERIC_TYPE = PARAMETER__EGENERIC_TYPE;

	/**
	 * The feature id for the '<em><b>EOperation</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int OPERATION_PARAMETER__EOPERATION = PARAMETER__EOPERATION;

	/**
	 * The feature id for the '<em><b>Operation</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int OPERATION_PARAMETER__OPERATION = PARAMETER_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Operation Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int OPERATION_PARAMETER_FEATURE_COUNT = PARAMETER_FEATURE_COUNT + 1;

	/**
	 * The operation id for the '<em>Get EAnnotation</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int OPERATION_PARAMETER___GET_EANNOTATION__STRING = PARAMETER___GET_EANNOTATION__STRING;

	/**
	 * The number of operations of the '<em>Operation Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int OPERATION_PARAMETER_OPERATION_COUNT = PARAMETER_OPERATION_COUNT + 0;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PROXY_OPERATION_PARAMETER__EANNOTATIONS = PARAMETER__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PROXY_OPERATION_PARAMETER__NAME = PARAMETER__NAME;

	/**
	 * The feature id for the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PROXY_OPERATION_PARAMETER__ORDERED = PARAMETER__ORDERED;

	/**
	 * The feature id for the '<em><b>Unique</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PROXY_OPERATION_PARAMETER__UNIQUE = PARAMETER__UNIQUE;

	/**
	 * The feature id for the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PROXY_OPERATION_PARAMETER__LOWER_BOUND = PARAMETER__LOWER_BOUND;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PROXY_OPERATION_PARAMETER__UPPER_BOUND = PARAMETER__UPPER_BOUND;

	/**
	 * The feature id for the '<em><b>Many</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PROXY_OPERATION_PARAMETER__MANY = PARAMETER__MANY;

	/**
	 * The feature id for the '<em><b>Required</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PROXY_OPERATION_PARAMETER__REQUIRED = PARAMETER__REQUIRED;

	/**
	 * The feature id for the '<em><b>EType</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PROXY_OPERATION_PARAMETER__ETYPE = PARAMETER__ETYPE;

	/**
	 * The feature id for the '<em><b>EGeneric Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PROXY_OPERATION_PARAMETER__EGENERIC_TYPE = PARAMETER__EGENERIC_TYPE;

	/**
	 * The feature id for the '<em><b>EOperation</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PROXY_OPERATION_PARAMETER__EOPERATION = PARAMETER__EOPERATION;

	/**
	 * The feature id for the '<em><b>Entity</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PROXY_OPERATION_PARAMETER__ENTITY = PARAMETER_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Operation name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PROXY_OPERATION_PARAMETER__OPERATION_NAME = PARAMETER_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Proxy Operation Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PROXY_OPERATION_PARAMETER_FEATURE_COUNT = PARAMETER_FEATURE_COUNT + 2;

	/**
	 * The operation id for the '<em>Get EAnnotation</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PROXY_OPERATION_PARAMETER___GET_EANNOTATION__STRING = PARAMETER___GET_EANNOTATION__STRING;

	/**
	 * The number of operations of the '<em>Proxy Operation Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PROXY_OPERATION_PARAMETER_OPERATION_COUNT = PARAMETER_OPERATION_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Entity</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PROXY_FEATURE_PARAMETER__ENTITY = 0;

	/**
	 * The feature id for the '<em><b>Feature name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PROXY_FEATURE_PARAMETER__FEATURE_NAME = 1;

	/**
	 * The number of structural features of the '<em>Proxy Feature Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PROXY_FEATURE_PARAMETER_FEATURE_COUNT = 2;

	/**
	 * The number of operations of the '<em>Proxy Feature Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int PROXY_FEATURE_PARAMETER_OPERATION_COUNT = 0;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_CREATION_APPROACH__EANNOTATIONS = EcorePackage.EOPERATION__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_CREATION_APPROACH__NAME = EcorePackage.EOPERATION__NAME;

	/**
	 * The feature id for the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_CREATION_APPROACH__ORDERED = EcorePackage.EOPERATION__ORDERED;

	/**
	 * The feature id for the '<em><b>Unique</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_CREATION_APPROACH__UNIQUE = EcorePackage.EOPERATION__UNIQUE;

	/**
	 * The feature id for the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_CREATION_APPROACH__LOWER_BOUND = EcorePackage.EOPERATION__LOWER_BOUND;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_CREATION_APPROACH__UPPER_BOUND = EcorePackage.EOPERATION__UPPER_BOUND;

	/**
	 * The feature id for the '<em><b>Many</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_CREATION_APPROACH__MANY = EcorePackage.EOPERATION__MANY;

	/**
	 * The feature id for the '<em><b>Required</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_CREATION_APPROACH__REQUIRED = EcorePackage.EOPERATION__REQUIRED;

	/**
	 * The feature id for the '<em><b>EType</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_CREATION_APPROACH__ETYPE = EcorePackage.EOPERATION__ETYPE;

	/**
	 * The feature id for the '<em><b>EGeneric Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_CREATION_APPROACH__EGENERIC_TYPE = EcorePackage.EOPERATION__EGENERIC_TYPE;

	/**
	 * The feature id for the '<em><b>EContaining Class</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_CREATION_APPROACH__ECONTAINING_CLASS = EcorePackage.EOPERATION__ECONTAINING_CLASS;

	/**
	 * The feature id for the '<em><b>EType Parameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_CREATION_APPROACH__ETYPE_PARAMETERS = EcorePackage.EOPERATION__ETYPE_PARAMETERS;

	/**
	 * The feature id for the '<em><b>EParameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_CREATION_APPROACH__EPARAMETERS = EcorePackage.EOPERATION__EPARAMETERS;

	/**
	 * The feature id for the '<em><b>EExceptions</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_CREATION_APPROACH__EEXCEPTIONS = EcorePackage.EOPERATION__EEXCEPTIONS;

	/**
	 * The feature id for the '<em><b>EGeneric Exceptions</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_CREATION_APPROACH__EGENERIC_EXCEPTIONS = EcorePackage.EOPERATION__EGENERIC_EXCEPTIONS;

	/**
	 * The feature id for the '<em><b>Created Cube Columns</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_CREATION_APPROACH__CREATED_CUBE_COLUMNS = EcorePackage.EOPERATION_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Row Creation Approach</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_CREATION_APPROACH_FEATURE_COUNT = EcorePackage.EOPERATION_FEATURE_COUNT + 1;

	/**
	 * The operation id for the '<em>Get EAnnotation</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_CREATION_APPROACH___GET_EANNOTATION__STRING = EcorePackage.EOPERATION___GET_EANNOTATION__STRING;

	/**
	 * The operation id for the '<em>Get Operation ID</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_CREATION_APPROACH___GET_OPERATION_ID = EcorePackage.EOPERATION___GET_OPERATION_ID;

	/**
	 * The operation id for the '<em>Is Override Of</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_CREATION_APPROACH___IS_OVERRIDE_OF__EOPERATION = EcorePackage.EOPERATION___IS_OVERRIDE_OF__EOPERATION;

	/**
	 * The number of operations of the '<em>Row Creation Approach</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_CREATION_APPROACH_OPERATION_COUNT = EcorePackage.EOPERATION_OPERATION_COUNT + 0;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUSTOM_ROW_CREATION_APPROACH__EANNOTATIONS = ROW_CREATION_APPROACH__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUSTOM_ROW_CREATION_APPROACH__NAME = ROW_CREATION_APPROACH__NAME;

	/**
	 * The feature id for the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUSTOM_ROW_CREATION_APPROACH__ORDERED = ROW_CREATION_APPROACH__ORDERED;

	/**
	 * The feature id for the '<em><b>Unique</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUSTOM_ROW_CREATION_APPROACH__UNIQUE = ROW_CREATION_APPROACH__UNIQUE;

	/**
	 * The feature id for the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUSTOM_ROW_CREATION_APPROACH__LOWER_BOUND = ROW_CREATION_APPROACH__LOWER_BOUND;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUSTOM_ROW_CREATION_APPROACH__UPPER_BOUND = ROW_CREATION_APPROACH__UPPER_BOUND;

	/**
	 * The feature id for the '<em><b>Many</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUSTOM_ROW_CREATION_APPROACH__MANY = ROW_CREATION_APPROACH__MANY;

	/**
	 * The feature id for the '<em><b>Required</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUSTOM_ROW_CREATION_APPROACH__REQUIRED = ROW_CREATION_APPROACH__REQUIRED;

	/**
	 * The feature id for the '<em><b>EType</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUSTOM_ROW_CREATION_APPROACH__ETYPE = ROW_CREATION_APPROACH__ETYPE;

	/**
	 * The feature id for the '<em><b>EGeneric Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUSTOM_ROW_CREATION_APPROACH__EGENERIC_TYPE = ROW_CREATION_APPROACH__EGENERIC_TYPE;

	/**
	 * The feature id for the '<em><b>EContaining Class</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUSTOM_ROW_CREATION_APPROACH__ECONTAINING_CLASS = ROW_CREATION_APPROACH__ECONTAINING_CLASS;

	/**
	 * The feature id for the '<em><b>EType Parameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUSTOM_ROW_CREATION_APPROACH__ETYPE_PARAMETERS = ROW_CREATION_APPROACH__ETYPE_PARAMETERS;

	/**
	 * The feature id for the '<em><b>EParameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUSTOM_ROW_CREATION_APPROACH__EPARAMETERS = ROW_CREATION_APPROACH__EPARAMETERS;

	/**
	 * The feature id for the '<em><b>EExceptions</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUSTOM_ROW_CREATION_APPROACH__EEXCEPTIONS = ROW_CREATION_APPROACH__EEXCEPTIONS;

	/**
	 * The feature id for the '<em><b>EGeneric Exceptions</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUSTOM_ROW_CREATION_APPROACH__EGENERIC_EXCEPTIONS = ROW_CREATION_APPROACH__EGENERIC_EXCEPTIONS;

	/**
	 * The feature id for the '<em><b>Created Cube Columns</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUSTOM_ROW_CREATION_APPROACH__CREATED_CUBE_COLUMNS = ROW_CREATION_APPROACH__CREATED_CUBE_COLUMNS;

	/**
	 * The number of structural features of the '<em>Custom Row Creation Approach</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUSTOM_ROW_CREATION_APPROACH_FEATURE_COUNT = ROW_CREATION_APPROACH_FEATURE_COUNT + 0;

	/**
	 * The operation id for the '<em>Get EAnnotation</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUSTOM_ROW_CREATION_APPROACH___GET_EANNOTATION__STRING = ROW_CREATION_APPROACH___GET_EANNOTATION__STRING;

	/**
	 * The operation id for the '<em>Get Operation ID</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUSTOM_ROW_CREATION_APPROACH___GET_OPERATION_ID = ROW_CREATION_APPROACH___GET_OPERATION_ID;

	/**
	 * The operation id for the '<em>Is Override Of</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUSTOM_ROW_CREATION_APPROACH___IS_OVERRIDE_OF__EOPERATION = ROW_CREATION_APPROACH___IS_OVERRIDE_OF__EOPERATION;

	/**
	 * The number of operations of the '<em>Custom Row Creation Approach</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUSTOM_ROW_CREATION_APPROACH_OPERATION_COUNT = ROW_CREATION_APPROACH_OPERATION_COUNT + 0;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH__EANNOTATIONS = ROW_CREATION_APPROACH__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH__NAME = ROW_CREATION_APPROACH__NAME;

	/**
	 * The feature id for the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH__ORDERED = ROW_CREATION_APPROACH__ORDERED;

	/**
	 * The feature id for the '<em><b>Unique</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH__UNIQUE = ROW_CREATION_APPROACH__UNIQUE;

	/**
	 * The feature id for the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH__LOWER_BOUND = ROW_CREATION_APPROACH__LOWER_BOUND;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH__UPPER_BOUND = ROW_CREATION_APPROACH__UPPER_BOUND;

	/**
	 * The feature id for the '<em><b>Many</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH__MANY = ROW_CREATION_APPROACH__MANY;

	/**
	 * The feature id for the '<em><b>Required</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH__REQUIRED = ROW_CREATION_APPROACH__REQUIRED;

	/**
	 * The feature id for the '<em><b>EType</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH__ETYPE = ROW_CREATION_APPROACH__ETYPE;

	/**
	 * The feature id for the '<em><b>EGeneric Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH__EGENERIC_TYPE = ROW_CREATION_APPROACH__EGENERIC_TYPE;

	/**
	 * The feature id for the '<em><b>EContaining Class</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH__ECONTAINING_CLASS = ROW_CREATION_APPROACH__ECONTAINING_CLASS;

	/**
	 * The feature id for the '<em><b>EType Parameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH__ETYPE_PARAMETERS = ROW_CREATION_APPROACH__ETYPE_PARAMETERS;

	/**
	 * The feature id for the '<em><b>EParameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH__EPARAMETERS = ROW_CREATION_APPROACH__EPARAMETERS;

	/**
	 * The feature id for the '<em><b>EExceptions</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH__EEXCEPTIONS = ROW_CREATION_APPROACH__EEXCEPTIONS;

	/**
	 * The feature id for the '<em><b>EGeneric Exceptions</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH__EGENERIC_EXCEPTIONS = ROW_CREATION_APPROACH__EGENERIC_EXCEPTIONS;

	/**
	 * The feature id for the '<em><b>Created Cube Columns</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH__CREATED_CUBE_COLUMNS = ROW_CREATION_APPROACH__CREATED_CUBE_COLUMNS;

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
	 * The operation id for the '<em>Get EAnnotation</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH___GET_EANNOTATION__STRING = ROW_CREATION_APPROACH___GET_EANNOTATION__STRING;

	/**
	 * The operation id for the '<em>Get Operation ID</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH___GET_OPERATION_ID = ROW_CREATION_APPROACH___GET_OPERATION_ID;

	/**
	 * The operation id for the '<em>Is Override Of</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH___IS_OVERRIDE_OF__EOPERATION = ROW_CREATION_APPROACH___IS_OVERRIDE_OF__EOPERATION;

	/**
	 * The number of operations of the '<em>Filter And Group To One Row Creation Approach</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_AND_GROUP_TO_ONE_ROW_CREATION_APPROACH_OPERATION_COUNT = ROW_CREATION_APPROACH_OPERATION_COUNT + 0;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_ONE_ROW_CREATION_APPROACH__EANNOTATIONS = ROW_CREATION_APPROACH__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_ONE_ROW_CREATION_APPROACH__NAME = ROW_CREATION_APPROACH__NAME;

	/**
	 * The feature id for the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_ONE_ROW_CREATION_APPROACH__ORDERED = ROW_CREATION_APPROACH__ORDERED;

	/**
	 * The feature id for the '<em><b>Unique</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_ONE_ROW_CREATION_APPROACH__UNIQUE = ROW_CREATION_APPROACH__UNIQUE;

	/**
	 * The feature id for the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_ONE_ROW_CREATION_APPROACH__LOWER_BOUND = ROW_CREATION_APPROACH__LOWER_BOUND;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_ONE_ROW_CREATION_APPROACH__UPPER_BOUND = ROW_CREATION_APPROACH__UPPER_BOUND;

	/**
	 * The feature id for the '<em><b>Many</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_ONE_ROW_CREATION_APPROACH__MANY = ROW_CREATION_APPROACH__MANY;

	/**
	 * The feature id for the '<em><b>Required</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_ONE_ROW_CREATION_APPROACH__REQUIRED = ROW_CREATION_APPROACH__REQUIRED;

	/**
	 * The feature id for the '<em><b>EType</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_ONE_ROW_CREATION_APPROACH__ETYPE = ROW_CREATION_APPROACH__ETYPE;

	/**
	 * The feature id for the '<em><b>EGeneric Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_ONE_ROW_CREATION_APPROACH__EGENERIC_TYPE = ROW_CREATION_APPROACH__EGENERIC_TYPE;

	/**
	 * The feature id for the '<em><b>EContaining Class</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_ONE_ROW_CREATION_APPROACH__ECONTAINING_CLASS = ROW_CREATION_APPROACH__ECONTAINING_CLASS;

	/**
	 * The feature id for the '<em><b>EType Parameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_ONE_ROW_CREATION_APPROACH__ETYPE_PARAMETERS = ROW_CREATION_APPROACH__ETYPE_PARAMETERS;

	/**
	 * The feature id for the '<em><b>EParameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_ONE_ROW_CREATION_APPROACH__EPARAMETERS = ROW_CREATION_APPROACH__EPARAMETERS;

	/**
	 * The feature id for the '<em><b>EExceptions</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_ONE_ROW_CREATION_APPROACH__EEXCEPTIONS = ROW_CREATION_APPROACH__EEXCEPTIONS;

	/**
	 * The feature id for the '<em><b>EGeneric Exceptions</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_ONE_ROW_CREATION_APPROACH__EGENERIC_EXCEPTIONS = ROW_CREATION_APPROACH__EGENERIC_EXCEPTIONS;

	/**
	 * The feature id for the '<em><b>Created Cube Columns</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_ONE_ROW_CREATION_APPROACH__CREATED_CUBE_COLUMNS = ROW_CREATION_APPROACH__CREATED_CUBE_COLUMNS;

	/**
	 * The number of structural features of the '<em>One To One Row Creation Approach</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_ONE_ROW_CREATION_APPROACH_FEATURE_COUNT = ROW_CREATION_APPROACH_FEATURE_COUNT + 0;

	/**
	 * The operation id for the '<em>Get EAnnotation</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_ONE_ROW_CREATION_APPROACH___GET_EANNOTATION__STRING = ROW_CREATION_APPROACH___GET_EANNOTATION__STRING;

	/**
	 * The operation id for the '<em>Get Operation ID</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_ONE_ROW_CREATION_APPROACH___GET_OPERATION_ID = ROW_CREATION_APPROACH___GET_OPERATION_ID;

	/**
	 * The operation id for the '<em>Is Override Of</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_ONE_ROW_CREATION_APPROACH___IS_OVERRIDE_OF__EOPERATION = ROW_CREATION_APPROACH___IS_OVERRIDE_OF__EOPERATION;

	/**
	 * The number of operations of the '<em>One To One Row Creation Approach</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_ONE_ROW_CREATION_APPROACH_OPERATION_COUNT = ROW_CREATION_APPROACH_OPERATION_COUNT + 0;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_ROW_CREATION_APPROACH__EANNOTATIONS = ROW_CREATION_APPROACH__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_ROW_CREATION_APPROACH__NAME = ROW_CREATION_APPROACH__NAME;

	/**
	 * The feature id for the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_ROW_CREATION_APPROACH__ORDERED = ROW_CREATION_APPROACH__ORDERED;

	/**
	 * The feature id for the '<em><b>Unique</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_ROW_CREATION_APPROACH__UNIQUE = ROW_CREATION_APPROACH__UNIQUE;

	/**
	 * The feature id for the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_ROW_CREATION_APPROACH__LOWER_BOUND = ROW_CREATION_APPROACH__LOWER_BOUND;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_ROW_CREATION_APPROACH__UPPER_BOUND = ROW_CREATION_APPROACH__UPPER_BOUND;

	/**
	 * The feature id for the '<em><b>Many</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_ROW_CREATION_APPROACH__MANY = ROW_CREATION_APPROACH__MANY;

	/**
	 * The feature id for the '<em><b>Required</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_ROW_CREATION_APPROACH__REQUIRED = ROW_CREATION_APPROACH__REQUIRED;

	/**
	 * The feature id for the '<em><b>EType</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_ROW_CREATION_APPROACH__ETYPE = ROW_CREATION_APPROACH__ETYPE;

	/**
	 * The feature id for the '<em><b>EGeneric Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_ROW_CREATION_APPROACH__EGENERIC_TYPE = ROW_CREATION_APPROACH__EGENERIC_TYPE;

	/**
	 * The feature id for the '<em><b>EContaining Class</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_ROW_CREATION_APPROACH__ECONTAINING_CLASS = ROW_CREATION_APPROACH__ECONTAINING_CLASS;

	/**
	 * The feature id for the '<em><b>EType Parameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_ROW_CREATION_APPROACH__ETYPE_PARAMETERS = ROW_CREATION_APPROACH__ETYPE_PARAMETERS;

	/**
	 * The feature id for the '<em><b>EParameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_ROW_CREATION_APPROACH__EPARAMETERS = ROW_CREATION_APPROACH__EPARAMETERS;

	/**
	 * The feature id for the '<em><b>EExceptions</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_ROW_CREATION_APPROACH__EEXCEPTIONS = ROW_CREATION_APPROACH__EEXCEPTIONS;

	/**
	 * The feature id for the '<em><b>EGeneric Exceptions</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_ROW_CREATION_APPROACH__EGENERIC_EXCEPTIONS = ROW_CREATION_APPROACH__EGENERIC_EXCEPTIONS;

	/**
	 * The feature id for the '<em><b>Created Cube Columns</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_ROW_CREATION_APPROACH__CREATED_CUBE_COLUMNS = ROW_CREATION_APPROACH__CREATED_CUBE_COLUMNS;

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
	 * The operation id for the '<em>Get EAnnotation</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_ROW_CREATION_APPROACH___GET_EANNOTATION__STRING = ROW_CREATION_APPROACH___GET_EANNOTATION__STRING;

	/**
	 * The operation id for the '<em>Get Operation ID</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_ROW_CREATION_APPROACH___GET_OPERATION_ID = ROW_CREATION_APPROACH___GET_OPERATION_ID;

	/**
	 * The operation id for the '<em>Is Override Of</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_ROW_CREATION_APPROACH___IS_OVERRIDE_OF__EOPERATION = ROW_CREATION_APPROACH___IS_OVERRIDE_OF__EOPERATION;

	/**
	 * The number of operations of the '<em>Filter Row Creation Approach</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int FILTER_ROW_CREATION_APPROACH_OPERATION_COUNT = ROW_CREATION_APPROACH_OPERATION_COUNT + 0;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GROUP_BY_ROW_CREATION_APPROACH__EANNOTATIONS = ROW_CREATION_APPROACH__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GROUP_BY_ROW_CREATION_APPROACH__NAME = ROW_CREATION_APPROACH__NAME;

	/**
	 * The feature id for the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GROUP_BY_ROW_CREATION_APPROACH__ORDERED = ROW_CREATION_APPROACH__ORDERED;

	/**
	 * The feature id for the '<em><b>Unique</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GROUP_BY_ROW_CREATION_APPROACH__UNIQUE = ROW_CREATION_APPROACH__UNIQUE;

	/**
	 * The feature id for the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GROUP_BY_ROW_CREATION_APPROACH__LOWER_BOUND = ROW_CREATION_APPROACH__LOWER_BOUND;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GROUP_BY_ROW_CREATION_APPROACH__UPPER_BOUND = ROW_CREATION_APPROACH__UPPER_BOUND;

	/**
	 * The feature id for the '<em><b>Many</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GROUP_BY_ROW_CREATION_APPROACH__MANY = ROW_CREATION_APPROACH__MANY;

	/**
	 * The feature id for the '<em><b>Required</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GROUP_BY_ROW_CREATION_APPROACH__REQUIRED = ROW_CREATION_APPROACH__REQUIRED;

	/**
	 * The feature id for the '<em><b>EType</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GROUP_BY_ROW_CREATION_APPROACH__ETYPE = ROW_CREATION_APPROACH__ETYPE;

	/**
	 * The feature id for the '<em><b>EGeneric Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GROUP_BY_ROW_CREATION_APPROACH__EGENERIC_TYPE = ROW_CREATION_APPROACH__EGENERIC_TYPE;

	/**
	 * The feature id for the '<em><b>EContaining Class</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GROUP_BY_ROW_CREATION_APPROACH__ECONTAINING_CLASS = ROW_CREATION_APPROACH__ECONTAINING_CLASS;

	/**
	 * The feature id for the '<em><b>EType Parameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GROUP_BY_ROW_CREATION_APPROACH__ETYPE_PARAMETERS = ROW_CREATION_APPROACH__ETYPE_PARAMETERS;

	/**
	 * The feature id for the '<em><b>EParameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GROUP_BY_ROW_CREATION_APPROACH__EPARAMETERS = ROW_CREATION_APPROACH__EPARAMETERS;

	/**
	 * The feature id for the '<em><b>EExceptions</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GROUP_BY_ROW_CREATION_APPROACH__EEXCEPTIONS = ROW_CREATION_APPROACH__EEXCEPTIONS;

	/**
	 * The feature id for the '<em><b>EGeneric Exceptions</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GROUP_BY_ROW_CREATION_APPROACH__EGENERIC_EXCEPTIONS = ROW_CREATION_APPROACH__EGENERIC_EXCEPTIONS;

	/**
	 * The feature id for the '<em><b>Created Cube Columns</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GROUP_BY_ROW_CREATION_APPROACH__CREATED_CUBE_COLUMNS = ROW_CREATION_APPROACH__CREATED_CUBE_COLUMNS;

	/**
	 * The feature id for the '<em><b>Group By Columns</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GROUP_BY_ROW_CREATION_APPROACH__GROUP_BY_COLUMNS = ROW_CREATION_APPROACH_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Group By Cube Column Names</b></em>' attribute list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GROUP_BY_ROW_CREATION_APPROACH__GROUP_BY_CUBE_COLUMN_NAMES = ROW_CREATION_APPROACH_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Group By Row Creation Approach</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GROUP_BY_ROW_CREATION_APPROACH_FEATURE_COUNT = ROW_CREATION_APPROACH_FEATURE_COUNT + 2;

	/**
	 * The operation id for the '<em>Get EAnnotation</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GROUP_BY_ROW_CREATION_APPROACH___GET_EANNOTATION__STRING = ROW_CREATION_APPROACH___GET_EANNOTATION__STRING;

	/**
	 * The operation id for the '<em>Get Operation ID</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GROUP_BY_ROW_CREATION_APPROACH___GET_OPERATION_ID = ROW_CREATION_APPROACH___GET_OPERATION_ID;

	/**
	 * The operation id for the '<em>Is Override Of</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GROUP_BY_ROW_CREATION_APPROACH___IS_OVERRIDE_OF__EOPERATION = ROW_CREATION_APPROACH___IS_OVERRIDE_OF__EOPERATION;

	/**
	 * The number of operations of the '<em>Group By Row Creation Approach</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GROUP_BY_ROW_CREATION_APPROACH_OPERATION_COUNT = ROW_CREATION_APPROACH_OPERATION_COUNT + 0;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_JOIN_FUNCTION__EANNOTATIONS = ROW_CREATION_APPROACH__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_JOIN_FUNCTION__NAME = ROW_CREATION_APPROACH__NAME;

	/**
	 * The feature id for the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_JOIN_FUNCTION__ORDERED = ROW_CREATION_APPROACH__ORDERED;

	/**
	 * The feature id for the '<em><b>Unique</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_JOIN_FUNCTION__UNIQUE = ROW_CREATION_APPROACH__UNIQUE;

	/**
	 * The feature id for the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_JOIN_FUNCTION__LOWER_BOUND = ROW_CREATION_APPROACH__LOWER_BOUND;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_JOIN_FUNCTION__UPPER_BOUND = ROW_CREATION_APPROACH__UPPER_BOUND;

	/**
	 * The feature id for the '<em><b>Many</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_JOIN_FUNCTION__MANY = ROW_CREATION_APPROACH__MANY;

	/**
	 * The feature id for the '<em><b>Required</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_JOIN_FUNCTION__REQUIRED = ROW_CREATION_APPROACH__REQUIRED;

	/**
	 * The feature id for the '<em><b>EType</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_JOIN_FUNCTION__ETYPE = ROW_CREATION_APPROACH__ETYPE;

	/**
	 * The feature id for the '<em><b>EGeneric Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_JOIN_FUNCTION__EGENERIC_TYPE = ROW_CREATION_APPROACH__EGENERIC_TYPE;

	/**
	 * The feature id for the '<em><b>EContaining Class</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_JOIN_FUNCTION__ECONTAINING_CLASS = ROW_CREATION_APPROACH__ECONTAINING_CLASS;

	/**
	 * The feature id for the '<em><b>EType Parameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_JOIN_FUNCTION__ETYPE_PARAMETERS = ROW_CREATION_APPROACH__ETYPE_PARAMETERS;

	/**
	 * The feature id for the '<em><b>EParameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_JOIN_FUNCTION__EPARAMETERS = ROW_CREATION_APPROACH__EPARAMETERS;

	/**
	 * The feature id for the '<em><b>EExceptions</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_JOIN_FUNCTION__EEXCEPTIONS = ROW_CREATION_APPROACH__EEXCEPTIONS;

	/**
	 * The feature id for the '<em><b>EGeneric Exceptions</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_JOIN_FUNCTION__EGENERIC_EXCEPTIONS = ROW_CREATION_APPROACH__EGENERIC_EXCEPTIONS;

	/**
	 * The feature id for the '<em><b>Created Cube Columns</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_JOIN_FUNCTION__CREATED_CUBE_COLUMNS = ROW_CREATION_APPROACH__CREATED_CUBE_COLUMNS;

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
	 * The operation id for the '<em>Get EAnnotation</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_JOIN_FUNCTION___GET_EANNOTATION__STRING = ROW_CREATION_APPROACH___GET_EANNOTATION__STRING;

	/**
	 * The operation id for the '<em>Get Operation ID</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_JOIN_FUNCTION___GET_OPERATION_ID = ROW_CREATION_APPROACH___GET_OPERATION_ID;

	/**
	 * The operation id for the '<em>Is Override Of</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_JOIN_FUNCTION___IS_OVERRIDE_OF__EOPERATION = ROW_CREATION_APPROACH___IS_OVERRIDE_OF__EOPERATION;

	/**
	 * The number of operations of the '<em>Row Join Function</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ROW_JOIN_FUNCTION_OPERATION_COUNT = ROW_CREATION_APPROACH_OPERATION_COUNT + 0;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNION_ROW_CREATION_APPROACH__EANNOTATIONS = ROW_CREATION_APPROACH__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNION_ROW_CREATION_APPROACH__NAME = ROW_CREATION_APPROACH__NAME;

	/**
	 * The feature id for the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNION_ROW_CREATION_APPROACH__ORDERED = ROW_CREATION_APPROACH__ORDERED;

	/**
	 * The feature id for the '<em><b>Unique</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNION_ROW_CREATION_APPROACH__UNIQUE = ROW_CREATION_APPROACH__UNIQUE;

	/**
	 * The feature id for the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNION_ROW_CREATION_APPROACH__LOWER_BOUND = ROW_CREATION_APPROACH__LOWER_BOUND;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNION_ROW_CREATION_APPROACH__UPPER_BOUND = ROW_CREATION_APPROACH__UPPER_BOUND;

	/**
	 * The feature id for the '<em><b>Many</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNION_ROW_CREATION_APPROACH__MANY = ROW_CREATION_APPROACH__MANY;

	/**
	 * The feature id for the '<em><b>Required</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNION_ROW_CREATION_APPROACH__REQUIRED = ROW_CREATION_APPROACH__REQUIRED;

	/**
	 * The feature id for the '<em><b>EType</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNION_ROW_CREATION_APPROACH__ETYPE = ROW_CREATION_APPROACH__ETYPE;

	/**
	 * The feature id for the '<em><b>EGeneric Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNION_ROW_CREATION_APPROACH__EGENERIC_TYPE = ROW_CREATION_APPROACH__EGENERIC_TYPE;

	/**
	 * The feature id for the '<em><b>EContaining Class</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNION_ROW_CREATION_APPROACH__ECONTAINING_CLASS = ROW_CREATION_APPROACH__ECONTAINING_CLASS;

	/**
	 * The feature id for the '<em><b>EType Parameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNION_ROW_CREATION_APPROACH__ETYPE_PARAMETERS = ROW_CREATION_APPROACH__ETYPE_PARAMETERS;

	/**
	 * The feature id for the '<em><b>EParameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNION_ROW_CREATION_APPROACH__EPARAMETERS = ROW_CREATION_APPROACH__EPARAMETERS;

	/**
	 * The feature id for the '<em><b>EExceptions</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNION_ROW_CREATION_APPROACH__EEXCEPTIONS = ROW_CREATION_APPROACH__EEXCEPTIONS;

	/**
	 * The feature id for the '<em><b>EGeneric Exceptions</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNION_ROW_CREATION_APPROACH__EGENERIC_EXCEPTIONS = ROW_CREATION_APPROACH__EGENERIC_EXCEPTIONS;

	/**
	 * The feature id for the '<em><b>Created Cube Columns</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNION_ROW_CREATION_APPROACH__CREATED_CUBE_COLUMNS = ROW_CREATION_APPROACH__CREATED_CUBE_COLUMNS;

	/**
	 * The number of structural features of the '<em>Union Row Creation Approach</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNION_ROW_CREATION_APPROACH_FEATURE_COUNT = ROW_CREATION_APPROACH_FEATURE_COUNT + 0;

	/**
	 * The operation id for the '<em>Get EAnnotation</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNION_ROW_CREATION_APPROACH___GET_EANNOTATION__STRING = ROW_CREATION_APPROACH___GET_EANNOTATION__STRING;

	/**
	 * The operation id for the '<em>Get Operation ID</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNION_ROW_CREATION_APPROACH___GET_OPERATION_ID = ROW_CREATION_APPROACH___GET_OPERATION_ID;

	/**
	 * The operation id for the '<em>Is Override Of</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNION_ROW_CREATION_APPROACH___IS_OVERRIDE_OF__EOPERATION = ROW_CREATION_APPROACH___IS_OVERRIDE_OF__EOPERATION;

	/**
	 * The number of operations of the '<em>Union Row Creation Approach</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int UNION_ROW_CREATION_APPROACH_OPERATION_COUNT = ROW_CREATION_APPROACH_OPERATION_COUNT + 0;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION__EANNOTATIONS = ROW_CREATION_APPROACH__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION__NAME = ROW_CREATION_APPROACH__NAME;

	/**
	 * The feature id for the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION__ORDERED = ROW_CREATION_APPROACH__ORDERED;

	/**
	 * The feature id for the '<em><b>Unique</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION__UNIQUE = ROW_CREATION_APPROACH__UNIQUE;

	/**
	 * The feature id for the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION__LOWER_BOUND = ROW_CREATION_APPROACH__LOWER_BOUND;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION__UPPER_BOUND = ROW_CREATION_APPROACH__UPPER_BOUND;

	/**
	 * The feature id for the '<em><b>Many</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION__MANY = ROW_CREATION_APPROACH__MANY;

	/**
	 * The feature id for the '<em><b>Required</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION__REQUIRED = ROW_CREATION_APPROACH__REQUIRED;

	/**
	 * The feature id for the '<em><b>EType</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION__ETYPE = ROW_CREATION_APPROACH__ETYPE;

	/**
	 * The feature id for the '<em><b>EGeneric Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION__EGENERIC_TYPE = ROW_CREATION_APPROACH__EGENERIC_TYPE;

	/**
	 * The feature id for the '<em><b>EContaining Class</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION__ECONTAINING_CLASS = ROW_CREATION_APPROACH__ECONTAINING_CLASS;

	/**
	 * The feature id for the '<em><b>EType Parameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION__ETYPE_PARAMETERS = ROW_CREATION_APPROACH__ETYPE_PARAMETERS;

	/**
	 * The feature id for the '<em><b>EParameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION__EPARAMETERS = ROW_CREATION_APPROACH__EPARAMETERS;

	/**
	 * The feature id for the '<em><b>EExceptions</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION__EEXCEPTIONS = ROW_CREATION_APPROACH__EEXCEPTIONS;

	/**
	 * The feature id for the '<em><b>EGeneric Exceptions</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION__EGENERIC_EXCEPTIONS = ROW_CREATION_APPROACH__EGENERIC_EXCEPTIONS;

	/**
	 * The feature id for the '<em><b>Created Cube Columns</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION__CREATED_CUBE_COLUMNS = ROW_CREATION_APPROACH__CREATED_CUBE_COLUMNS;

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
	 * The operation id for the '<em>Get EAnnotation</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION___GET_EANNOTATION__STRING = ROW_CREATION_APPROACH___GET_EANNOTATION__STRING;

	/**
	 * The operation id for the '<em>Get Operation ID</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION___GET_OPERATION_ID = ROW_CREATION_APPROACH___GET_OPERATION_ID;

	/**
	 * The operation id for the '<em>Is Override Of</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION___IS_OVERRIDE_OF__EOPERATION = ROW_CREATION_APPROACH___IS_OVERRIDE_OF__EOPERATION;

	/**
	 * The number of operations of the '<em>Explode Array Of Structs Row Function</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int EXPLODE_ARRAY_OF_STRUCTS_ROW_FUNCTION_OPERATION_COUNT = ROW_CREATION_APPROACH_OPERATION_COUNT + 0;

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
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TABLE__EANNOTATIONS = Ecore_plus_structurePackage.ENTITY__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TABLE__NAME = Ecore_plus_structurePackage.ENTITY__NAME;

	/**
	 * The feature id for the '<em><b>Instance Class Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TABLE__INSTANCE_CLASS_NAME = Ecore_plus_structurePackage.ENTITY__INSTANCE_CLASS_NAME;

	/**
	 * The feature id for the '<em><b>Instance Class</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TABLE__INSTANCE_CLASS = Ecore_plus_structurePackage.ENTITY__INSTANCE_CLASS;

	/**
	 * The feature id for the '<em><b>Default Value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TABLE__DEFAULT_VALUE = Ecore_plus_structurePackage.ENTITY__DEFAULT_VALUE;

	/**
	 * The feature id for the '<em><b>Instance Type Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TABLE__INSTANCE_TYPE_NAME = Ecore_plus_structurePackage.ENTITY__INSTANCE_TYPE_NAME;

	/**
	 * The feature id for the '<em><b>EPackage</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TABLE__EPACKAGE = Ecore_plus_structurePackage.ENTITY__EPACKAGE;

	/**
	 * The feature id for the '<em><b>EType Parameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TABLE__ETYPE_PARAMETERS = Ecore_plus_structurePackage.ENTITY__ETYPE_PARAMETERS;

	/**
	 * The feature id for the '<em><b>Abstract</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TABLE__ABSTRACT = Ecore_plus_structurePackage.ENTITY__ABSTRACT;

	/**
	 * The feature id for the '<em><b>Interface</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TABLE__INTERFACE = Ecore_plus_structurePackage.ENTITY__INTERFACE;

	/**
	 * The feature id for the '<em><b>ESuper Types</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TABLE__ESUPER_TYPES = Ecore_plus_structurePackage.ENTITY__ESUPER_TYPES;

	/**
	 * The feature id for the '<em><b>EOperations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TABLE__EOPERATIONS = Ecore_plus_structurePackage.ENTITY__EOPERATIONS;

	/**
	 * The feature id for the '<em><b>EAll Attributes</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TABLE__EALL_ATTRIBUTES = Ecore_plus_structurePackage.ENTITY__EALL_ATTRIBUTES;

	/**
	 * The feature id for the '<em><b>EAll References</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TABLE__EALL_REFERENCES = Ecore_plus_structurePackage.ENTITY__EALL_REFERENCES;

	/**
	 * The feature id for the '<em><b>EReferences</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TABLE__EREFERENCES = Ecore_plus_structurePackage.ENTITY__EREFERENCES;

	/**
	 * The feature id for the '<em><b>EAttributes</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TABLE__EATTRIBUTES = Ecore_plus_structurePackage.ENTITY__EATTRIBUTES;

	/**
	 * The feature id for the '<em><b>EAll Containments</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TABLE__EALL_CONTAINMENTS = Ecore_plus_structurePackage.ENTITY__EALL_CONTAINMENTS;

	/**
	 * The feature id for the '<em><b>EAll Operations</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TABLE__EALL_OPERATIONS = Ecore_plus_structurePackage.ENTITY__EALL_OPERATIONS;

	/**
	 * The feature id for the '<em><b>EAll Structural Features</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TABLE__EALL_STRUCTURAL_FEATURES = Ecore_plus_structurePackage.ENTITY__EALL_STRUCTURAL_FEATURES;

	/**
	 * The feature id for the '<em><b>EAll Super Types</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TABLE__EALL_SUPER_TYPES = Ecore_plus_structurePackage.ENTITY__EALL_SUPER_TYPES;

	/**
	 * The feature id for the '<em><b>EID Attribute</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TABLE__EID_ATTRIBUTE = Ecore_plus_structurePackage.ENTITY__EID_ATTRIBUTE;

	/**
	 * The feature id for the '<em><b>EStructural Features</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TABLE__ESTRUCTURAL_FEATURES = Ecore_plus_structurePackage.ENTITY__ESTRUCTURAL_FEATURES;

	/**
	 * The feature id for the '<em><b>EGeneric Super Types</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TABLE__EGENERIC_SUPER_TYPES = Ecore_plus_structurePackage.ENTITY__EGENERIC_SUPER_TYPES;

	/**
	 * The feature id for the '<em><b>EAll Generic Super Types</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TABLE__EALL_GENERIC_SUPER_TYPES = Ecore_plus_structurePackage.ENTITY__EALL_GENERIC_SUPER_TYPES;

	/**
	 * The number of structural features of the '<em>Entity Table</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TABLE_FEATURE_COUNT = Ecore_plus_structurePackage.ENTITY_FEATURE_COUNT + 0;

	/**
	 * The operation id for the '<em>Get EAnnotation</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TABLE___GET_EANNOTATION__STRING = Ecore_plus_structurePackage.ENTITY___GET_EANNOTATION__STRING;

	/**
	 * The operation id for the '<em>Is Instance</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TABLE___IS_INSTANCE__OBJECT = Ecore_plus_structurePackage.ENTITY___IS_INSTANCE__OBJECT;

	/**
	 * The operation id for the '<em>Get Classifier ID</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TABLE___GET_CLASSIFIER_ID = Ecore_plus_structurePackage.ENTITY___GET_CLASSIFIER_ID;

	/**
	 * The operation id for the '<em>Is Super Type Of</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TABLE___IS_SUPER_TYPE_OF__ECLASS = Ecore_plus_structurePackage.ENTITY___IS_SUPER_TYPE_OF__ECLASS;

	/**
	 * The operation id for the '<em>Get Feature Count</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TABLE___GET_FEATURE_COUNT = Ecore_plus_structurePackage.ENTITY___GET_FEATURE_COUNT;

	/**
	 * The operation id for the '<em>Get EStructural Feature</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TABLE___GET_ESTRUCTURAL_FEATURE__INT = Ecore_plus_structurePackage.ENTITY___GET_ESTRUCTURAL_FEATURE__INT;

	/**
	 * The operation id for the '<em>Get Feature ID</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TABLE___GET_FEATURE_ID__ESTRUCTURALFEATURE = Ecore_plus_structurePackage.ENTITY___GET_FEATURE_ID__ESTRUCTURALFEATURE;

	/**
	 * The operation id for the '<em>Get EStructural Feature</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TABLE___GET_ESTRUCTURAL_FEATURE__STRING = Ecore_plus_structurePackage.ENTITY___GET_ESTRUCTURAL_FEATURE__STRING;

	/**
	 * The operation id for the '<em>Get Operation Count</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TABLE___GET_OPERATION_COUNT = Ecore_plus_structurePackage.ENTITY___GET_OPERATION_COUNT;

	/**
	 * The operation id for the '<em>Get EOperation</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TABLE___GET_EOPERATION__INT = Ecore_plus_structurePackage.ENTITY___GET_EOPERATION__INT;

	/**
	 * The operation id for the '<em>Get Operation ID</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TABLE___GET_OPERATION_ID__EOPERATION = Ecore_plus_structurePackage.ENTITY___GET_OPERATION_ID__EOPERATION;

	/**
	 * The operation id for the '<em>Get Override</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TABLE___GET_OVERRIDE__EOPERATION = Ecore_plus_structurePackage.ENTITY___GET_OVERRIDE__EOPERATION;

	/**
	 * The operation id for the '<em>Get Feature Type</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TABLE___GET_FEATURE_TYPE__ESTRUCTURALFEATURE = Ecore_plus_structurePackage.ENTITY___GET_FEATURE_TYPE__ESTRUCTURALFEATURE;

	/**
	 * The number of operations of the '<em>Entity Table</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_TABLE_OPERATION_COUNT = Ecore_plus_structurePackage.ENTITY_OPERATION_COUNT + 0;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_TABLE__EANNOTATIONS = ENTITY_TABLE__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_TABLE__NAME = ENTITY_TABLE__NAME;

	/**
	 * The feature id for the '<em><b>Instance Class Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_TABLE__INSTANCE_CLASS_NAME = ENTITY_TABLE__INSTANCE_CLASS_NAME;

	/**
	 * The feature id for the '<em><b>Instance Class</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_TABLE__INSTANCE_CLASS = ENTITY_TABLE__INSTANCE_CLASS;

	/**
	 * The feature id for the '<em><b>Default Value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_TABLE__DEFAULT_VALUE = ENTITY_TABLE__DEFAULT_VALUE;

	/**
	 * The feature id for the '<em><b>Instance Type Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_TABLE__INSTANCE_TYPE_NAME = ENTITY_TABLE__INSTANCE_TYPE_NAME;

	/**
	 * The feature id for the '<em><b>EPackage</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_TABLE__EPACKAGE = ENTITY_TABLE__EPACKAGE;

	/**
	 * The feature id for the '<em><b>EType Parameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_TABLE__ETYPE_PARAMETERS = ENTITY_TABLE__ETYPE_PARAMETERS;

	/**
	 * The feature id for the '<em><b>Abstract</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_TABLE__ABSTRACT = ENTITY_TABLE__ABSTRACT;

	/**
	 * The feature id for the '<em><b>Interface</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_TABLE__INTERFACE = ENTITY_TABLE__INTERFACE;

	/**
	 * The feature id for the '<em><b>ESuper Types</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_TABLE__ESUPER_TYPES = ENTITY_TABLE__ESUPER_TYPES;

	/**
	 * The feature id for the '<em><b>EOperations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_TABLE__EOPERATIONS = ENTITY_TABLE__EOPERATIONS;

	/**
	 * The feature id for the '<em><b>EAll Attributes</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_TABLE__EALL_ATTRIBUTES = ENTITY_TABLE__EALL_ATTRIBUTES;

	/**
	 * The feature id for the '<em><b>EAll References</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_TABLE__EALL_REFERENCES = ENTITY_TABLE__EALL_REFERENCES;

	/**
	 * The feature id for the '<em><b>EReferences</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_TABLE__EREFERENCES = ENTITY_TABLE__EREFERENCES;

	/**
	 * The feature id for the '<em><b>EAttributes</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_TABLE__EATTRIBUTES = ENTITY_TABLE__EATTRIBUTES;

	/**
	 * The feature id for the '<em><b>EAll Containments</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_TABLE__EALL_CONTAINMENTS = ENTITY_TABLE__EALL_CONTAINMENTS;

	/**
	 * The feature id for the '<em><b>EAll Operations</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_TABLE__EALL_OPERATIONS = ENTITY_TABLE__EALL_OPERATIONS;

	/**
	 * The feature id for the '<em><b>EAll Structural Features</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_TABLE__EALL_STRUCTURAL_FEATURES = ENTITY_TABLE__EALL_STRUCTURAL_FEATURES;

	/**
	 * The feature id for the '<em><b>EAll Super Types</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_TABLE__EALL_SUPER_TYPES = ENTITY_TABLE__EALL_SUPER_TYPES;

	/**
	 * The feature id for the '<em><b>EID Attribute</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_TABLE__EID_ATTRIBUTE = ENTITY_TABLE__EID_ATTRIBUTE;

	/**
	 * The feature id for the '<em><b>EStructural Features</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_TABLE__ESTRUCTURAL_FEATURES = ENTITY_TABLE__ESTRUCTURAL_FEATURES;

	/**
	 * The feature id for the '<em><b>EGeneric Super Types</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_TABLE__EGENERIC_SUPER_TYPES = ENTITY_TABLE__EGENERIC_SUPER_TYPES;

	/**
	 * The feature id for the '<em><b>EAll Generic Super Types</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_TABLE__EALL_GENERIC_SUPER_TYPES = ENTITY_TABLE__EALL_GENERIC_SUPER_TYPES;

	/**
	 * The feature id for the '<em><b>Source Tables</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_TABLE__SOURCE_TABLES = ENTITY_TABLE_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Contained Entity Type</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_TABLE__CONTAINED_ENTITY_TYPE = ENTITY_TABLE_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Derived Entity Table</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_TABLE_FEATURE_COUNT = ENTITY_TABLE_FEATURE_COUNT + 2;

	/**
	 * The operation id for the '<em>Get EAnnotation</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_TABLE___GET_EANNOTATION__STRING = ENTITY_TABLE___GET_EANNOTATION__STRING;

	/**
	 * The operation id for the '<em>Is Instance</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_TABLE___IS_INSTANCE__OBJECT = ENTITY_TABLE___IS_INSTANCE__OBJECT;

	/**
	 * The operation id for the '<em>Get Classifier ID</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_TABLE___GET_CLASSIFIER_ID = ENTITY_TABLE___GET_CLASSIFIER_ID;

	/**
	 * The operation id for the '<em>Is Super Type Of</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_TABLE___IS_SUPER_TYPE_OF__ECLASS = ENTITY_TABLE___IS_SUPER_TYPE_OF__ECLASS;

	/**
	 * The operation id for the '<em>Get Feature Count</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_TABLE___GET_FEATURE_COUNT = ENTITY_TABLE___GET_FEATURE_COUNT;

	/**
	 * The operation id for the '<em>Get EStructural Feature</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_TABLE___GET_ESTRUCTURAL_FEATURE__INT = ENTITY_TABLE___GET_ESTRUCTURAL_FEATURE__INT;

	/**
	 * The operation id for the '<em>Get Feature ID</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_TABLE___GET_FEATURE_ID__ESTRUCTURALFEATURE = ENTITY_TABLE___GET_FEATURE_ID__ESTRUCTURALFEATURE;

	/**
	 * The operation id for the '<em>Get EStructural Feature</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_TABLE___GET_ESTRUCTURAL_FEATURE__STRING = ENTITY_TABLE___GET_ESTRUCTURAL_FEATURE__STRING;

	/**
	 * The operation id for the '<em>Get Operation Count</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_TABLE___GET_OPERATION_COUNT = ENTITY_TABLE___GET_OPERATION_COUNT;

	/**
	 * The operation id for the '<em>Get EOperation</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_TABLE___GET_EOPERATION__INT = ENTITY_TABLE___GET_EOPERATION__INT;

	/**
	 * The operation id for the '<em>Get Operation ID</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_TABLE___GET_OPERATION_ID__EOPERATION = ENTITY_TABLE___GET_OPERATION_ID__EOPERATION;

	/**
	 * The operation id for the '<em>Get Override</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_TABLE___GET_OVERRIDE__EOPERATION = ENTITY_TABLE___GET_OVERRIDE__EOPERATION;

	/**
	 * The operation id for the '<em>Get Feature Type</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_TABLE___GET_FEATURE_TYPE__ESTRUCTURALFEATURE = ENTITY_TABLE___GET_FEATURE_TYPE__ESTRUCTURALFEATURE;

	/**
	 * The number of operations of the '<em>Derived Entity Table</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_TABLE_OPERATION_COUNT = ENTITY_TABLE_OPERATION_COUNT + 0;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY_TABLE__EANNOTATIONS = ENTITY_TABLE__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY_TABLE__NAME = ENTITY_TABLE__NAME;

	/**
	 * The feature id for the '<em><b>Instance Class Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY_TABLE__INSTANCE_CLASS_NAME = ENTITY_TABLE__INSTANCE_CLASS_NAME;

	/**
	 * The feature id for the '<em><b>Instance Class</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY_TABLE__INSTANCE_CLASS = ENTITY_TABLE__INSTANCE_CLASS;

	/**
	 * The feature id for the '<em><b>Default Value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY_TABLE__DEFAULT_VALUE = ENTITY_TABLE__DEFAULT_VALUE;

	/**
	 * The feature id for the '<em><b>Instance Type Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY_TABLE__INSTANCE_TYPE_NAME = ENTITY_TABLE__INSTANCE_TYPE_NAME;

	/**
	 * The feature id for the '<em><b>EPackage</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY_TABLE__EPACKAGE = ENTITY_TABLE__EPACKAGE;

	/**
	 * The feature id for the '<em><b>EType Parameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY_TABLE__ETYPE_PARAMETERS = ENTITY_TABLE__ETYPE_PARAMETERS;

	/**
	 * The feature id for the '<em><b>Abstract</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY_TABLE__ABSTRACT = ENTITY_TABLE__ABSTRACT;

	/**
	 * The feature id for the '<em><b>Interface</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY_TABLE__INTERFACE = ENTITY_TABLE__INTERFACE;

	/**
	 * The feature id for the '<em><b>ESuper Types</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY_TABLE__ESUPER_TYPES = ENTITY_TABLE__ESUPER_TYPES;

	/**
	 * The feature id for the '<em><b>EOperations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY_TABLE__EOPERATIONS = ENTITY_TABLE__EOPERATIONS;

	/**
	 * The feature id for the '<em><b>EAll Attributes</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY_TABLE__EALL_ATTRIBUTES = ENTITY_TABLE__EALL_ATTRIBUTES;

	/**
	 * The feature id for the '<em><b>EAll References</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY_TABLE__EALL_REFERENCES = ENTITY_TABLE__EALL_REFERENCES;

	/**
	 * The feature id for the '<em><b>EReferences</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY_TABLE__EREFERENCES = ENTITY_TABLE__EREFERENCES;

	/**
	 * The feature id for the '<em><b>EAttributes</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY_TABLE__EATTRIBUTES = ENTITY_TABLE__EATTRIBUTES;

	/**
	 * The feature id for the '<em><b>EAll Containments</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY_TABLE__EALL_CONTAINMENTS = ENTITY_TABLE__EALL_CONTAINMENTS;

	/**
	 * The feature id for the '<em><b>EAll Operations</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY_TABLE__EALL_OPERATIONS = ENTITY_TABLE__EALL_OPERATIONS;

	/**
	 * The feature id for the '<em><b>EAll Structural Features</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY_TABLE__EALL_STRUCTURAL_FEATURES = ENTITY_TABLE__EALL_STRUCTURAL_FEATURES;

	/**
	 * The feature id for the '<em><b>EAll Super Types</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY_TABLE__EALL_SUPER_TYPES = ENTITY_TABLE__EALL_SUPER_TYPES;

	/**
	 * The feature id for the '<em><b>EID Attribute</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY_TABLE__EID_ATTRIBUTE = ENTITY_TABLE__EID_ATTRIBUTE;

	/**
	 * The feature id for the '<em><b>EStructural Features</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY_TABLE__ESTRUCTURAL_FEATURES = ENTITY_TABLE__ESTRUCTURAL_FEATURES;

	/**
	 * The feature id for the '<em><b>EGeneric Super Types</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY_TABLE__EGENERIC_SUPER_TYPES = ENTITY_TABLE__EGENERIC_SUPER_TYPES;

	/**
	 * The feature id for the '<em><b>EAll Generic Super Types</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY_TABLE__EALL_GENERIC_SUPER_TYPES = ENTITY_TABLE__EALL_GENERIC_SUPER_TYPES;

	/**
	 * The number of structural features of the '<em>Base Entity Table</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY_TABLE_FEATURE_COUNT = ENTITY_TABLE_FEATURE_COUNT + 0;

	/**
	 * The operation id for the '<em>Get EAnnotation</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY_TABLE___GET_EANNOTATION__STRING = ENTITY_TABLE___GET_EANNOTATION__STRING;

	/**
	 * The operation id for the '<em>Is Instance</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY_TABLE___IS_INSTANCE__OBJECT = ENTITY_TABLE___IS_INSTANCE__OBJECT;

	/**
	 * The operation id for the '<em>Get Classifier ID</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY_TABLE___GET_CLASSIFIER_ID = ENTITY_TABLE___GET_CLASSIFIER_ID;

	/**
	 * The operation id for the '<em>Is Super Type Of</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY_TABLE___IS_SUPER_TYPE_OF__ECLASS = ENTITY_TABLE___IS_SUPER_TYPE_OF__ECLASS;

	/**
	 * The operation id for the '<em>Get Feature Count</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY_TABLE___GET_FEATURE_COUNT = ENTITY_TABLE___GET_FEATURE_COUNT;

	/**
	 * The operation id for the '<em>Get EStructural Feature</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY_TABLE___GET_ESTRUCTURAL_FEATURE__INT = ENTITY_TABLE___GET_ESTRUCTURAL_FEATURE__INT;

	/**
	 * The operation id for the '<em>Get Feature ID</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY_TABLE___GET_FEATURE_ID__ESTRUCTURALFEATURE = ENTITY_TABLE___GET_FEATURE_ID__ESTRUCTURALFEATURE;

	/**
	 * The operation id for the '<em>Get EStructural Feature</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY_TABLE___GET_ESTRUCTURAL_FEATURE__STRING = ENTITY_TABLE___GET_ESTRUCTURAL_FEATURE__STRING;

	/**
	 * The operation id for the '<em>Get Operation Count</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY_TABLE___GET_OPERATION_COUNT = ENTITY_TABLE___GET_OPERATION_COUNT;

	/**
	 * The operation id for the '<em>Get EOperation</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY_TABLE___GET_EOPERATION__INT = ENTITY_TABLE___GET_EOPERATION__INT;

	/**
	 * The operation id for the '<em>Get Operation ID</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY_TABLE___GET_OPERATION_ID__EOPERATION = ENTITY_TABLE___GET_OPERATION_ID__EOPERATION;

	/**
	 * The operation id for the '<em>Get Override</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY_TABLE___GET_OVERRIDE__EOPERATION = ENTITY_TABLE___GET_OVERRIDE__EOPERATION;

	/**
	 * The operation id for the '<em>Get Feature Type</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY_TABLE___GET_FEATURE_TYPE__ESTRUCTURALFEATURE = ENTITY_TABLE___GET_FEATURE_TYPE__ESTRUCTURALFEATURE;

	/**
	 * The number of operations of the '<em>Base Entity Table</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY_TABLE_OPERATION_COUNT = ENTITY_TABLE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.AggregateOperationImpl <em>Aggregate Operation</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.AggregateOperationImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getAggregateOperation()
	 * @generated
	 */
	int AGGREGATE_OPERATION = 34;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_OPERATION__EANNOTATIONS = OPERATION__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_OPERATION__NAME = OPERATION__NAME;

	/**
	 * The feature id for the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_OPERATION__ORDERED = OPERATION__ORDERED;

	/**
	 * The feature id for the '<em><b>Unique</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_OPERATION__UNIQUE = OPERATION__UNIQUE;

	/**
	 * The feature id for the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_OPERATION__LOWER_BOUND = OPERATION__LOWER_BOUND;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_OPERATION__UPPER_BOUND = OPERATION__UPPER_BOUND;

	/**
	 * The feature id for the '<em><b>Many</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_OPERATION__MANY = OPERATION__MANY;

	/**
	 * The feature id for the '<em><b>Required</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_OPERATION__REQUIRED = OPERATION__REQUIRED;

	/**
	 * The feature id for the '<em><b>EType</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_OPERATION__ETYPE = OPERATION__ETYPE;

	/**
	 * The feature id for the '<em><b>EGeneric Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_OPERATION__EGENERIC_TYPE = OPERATION__EGENERIC_TYPE;

	/**
	 * The feature id for the '<em><b>EContaining Class</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_OPERATION__ECONTAINING_CLASS = OPERATION__ECONTAINING_CLASS;

	/**
	 * The feature id for the '<em><b>EType Parameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_OPERATION__ETYPE_PARAMETERS = OPERATION__ETYPE_PARAMETERS;

	/**
	 * The feature id for the '<em><b>EParameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_OPERATION__EPARAMETERS = OPERATION__EPARAMETERS;

	/**
	 * The feature id for the '<em><b>EExceptions</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_OPERATION__EEXCEPTIONS = OPERATION__EEXCEPTIONS;

	/**
	 * The feature id for the '<em><b>EGeneric Exceptions</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_OPERATION__EGENERIC_EXCEPTIONS = OPERATION__EGENERIC_EXCEPTIONS;

	/**
	 * The feature id for the '<em><b>EOperation</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_OPERATION__EOPERATION = OPERATION__EOPERATION;

	/**
	 * The feature id for the '<em><b>Function Spec</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_OPERATION__FUNCTION_SPEC = OPERATION_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Aggregate Operation</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_OPERATION_FEATURE_COUNT = OPERATION_FEATURE_COUNT + 1;

	/**
	 * The operation id for the '<em>Get EAnnotation</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_OPERATION___GET_EANNOTATION__STRING = OPERATION___GET_EANNOTATION__STRING;

	/**
	 * The operation id for the '<em>Get Operation ID</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_OPERATION___GET_OPERATION_ID = OPERATION___GET_OPERATION_ID;

	/**
	 * The operation id for the '<em>Is Override Of</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_OPERATION___IS_OVERRIDE_OF__EOPERATION = OPERATION___IS_OVERRIDE_OF__EOPERATION;

	/**
	 * The number of operations of the '<em>Aggregate Operation</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int AGGREGATE_OPERATION_OPERATION_COUNT = OPERATION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.EntityFromSourceOperationImpl <em>Entity From Source Operation</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.EntityFromSourceOperationImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getEntityFromSourceOperation()
	 * @generated
	 */
	int ENTITY_FROM_SOURCE_OPERATION = 35;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_FROM_SOURCE_OPERATION__EANNOTATIONS = OPERATION__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_FROM_SOURCE_OPERATION__NAME = OPERATION__NAME;

	/**
	 * The feature id for the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_FROM_SOURCE_OPERATION__ORDERED = OPERATION__ORDERED;

	/**
	 * The feature id for the '<em><b>Unique</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_FROM_SOURCE_OPERATION__UNIQUE = OPERATION__UNIQUE;

	/**
	 * The feature id for the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_FROM_SOURCE_OPERATION__LOWER_BOUND = OPERATION__LOWER_BOUND;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_FROM_SOURCE_OPERATION__UPPER_BOUND = OPERATION__UPPER_BOUND;

	/**
	 * The feature id for the '<em><b>Many</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_FROM_SOURCE_OPERATION__MANY = OPERATION__MANY;

	/**
	 * The feature id for the '<em><b>Required</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_FROM_SOURCE_OPERATION__REQUIRED = OPERATION__REQUIRED;

	/**
	 * The feature id for the '<em><b>EType</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_FROM_SOURCE_OPERATION__ETYPE = OPERATION__ETYPE;

	/**
	 * The feature id for the '<em><b>EGeneric Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_FROM_SOURCE_OPERATION__EGENERIC_TYPE = OPERATION__EGENERIC_TYPE;

	/**
	 * The feature id for the '<em><b>EContaining Class</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_FROM_SOURCE_OPERATION__ECONTAINING_CLASS = OPERATION__ECONTAINING_CLASS;

	/**
	 * The feature id for the '<em><b>EType Parameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_FROM_SOURCE_OPERATION__ETYPE_PARAMETERS = OPERATION__ETYPE_PARAMETERS;

	/**
	 * The feature id for the '<em><b>EParameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_FROM_SOURCE_OPERATION__EPARAMETERS = OPERATION__EPARAMETERS;

	/**
	 * The feature id for the '<em><b>EExceptions</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_FROM_SOURCE_OPERATION__EEXCEPTIONS = OPERATION__EEXCEPTIONS;

	/**
	 * The feature id for the '<em><b>EGeneric Exceptions</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_FROM_SOURCE_OPERATION__EGENERIC_EXCEPTIONS = OPERATION__EGENERIC_EXCEPTIONS;

	/**
	 * The feature id for the '<em><b>EOperation</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_FROM_SOURCE_OPERATION__EOPERATION = OPERATION__EOPERATION;

	/**
	 * The number of structural features of the '<em>Entity From Source Operation</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_FROM_SOURCE_OPERATION_FEATURE_COUNT = OPERATION_FEATURE_COUNT + 0;

	/**
	 * The operation id for the '<em>Get EAnnotation</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_FROM_SOURCE_OPERATION___GET_EANNOTATION__STRING = OPERATION___GET_EANNOTATION__STRING;

	/**
	 * The operation id for the '<em>Get Operation ID</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_FROM_SOURCE_OPERATION___GET_OPERATION_ID = OPERATION___GET_OPERATION_ID;

	/**
	 * The operation id for the '<em>Is Override Of</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_FROM_SOURCE_OPERATION___IS_OVERRIDE_OF__EOPERATION = OPERATION___IS_OVERRIDE_OF__EOPERATION;

	/**
	 * The number of operations of the '<em>Entity From Source Operation</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_FROM_SOURCE_OPERATION_OPERATION_COUNT = OPERATION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.AttributeFromEntityOperationImpl <em>Attribute From Entity Operation</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.AttributeFromEntityOperationImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getAttributeFromEntityOperation()
	 * @generated
	 */
	int ATTRIBUTE_FROM_ENTITY_OPERATION = 36;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_FROM_ENTITY_OPERATION__EANNOTATIONS = OPERATION__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_FROM_ENTITY_OPERATION__NAME = OPERATION__NAME;

	/**
	 * The feature id for the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_FROM_ENTITY_OPERATION__ORDERED = OPERATION__ORDERED;

	/**
	 * The feature id for the '<em><b>Unique</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_FROM_ENTITY_OPERATION__UNIQUE = OPERATION__UNIQUE;

	/**
	 * The feature id for the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_FROM_ENTITY_OPERATION__LOWER_BOUND = OPERATION__LOWER_BOUND;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_FROM_ENTITY_OPERATION__UPPER_BOUND = OPERATION__UPPER_BOUND;

	/**
	 * The feature id for the '<em><b>Many</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_FROM_ENTITY_OPERATION__MANY = OPERATION__MANY;

	/**
	 * The feature id for the '<em><b>Required</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_FROM_ENTITY_OPERATION__REQUIRED = OPERATION__REQUIRED;

	/**
	 * The feature id for the '<em><b>EType</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_FROM_ENTITY_OPERATION__ETYPE = OPERATION__ETYPE;

	/**
	 * The feature id for the '<em><b>EGeneric Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_FROM_ENTITY_OPERATION__EGENERIC_TYPE = OPERATION__EGENERIC_TYPE;

	/**
	 * The feature id for the '<em><b>EContaining Class</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_FROM_ENTITY_OPERATION__ECONTAINING_CLASS = OPERATION__ECONTAINING_CLASS;

	/**
	 * The feature id for the '<em><b>EType Parameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_FROM_ENTITY_OPERATION__ETYPE_PARAMETERS = OPERATION__ETYPE_PARAMETERS;

	/**
	 * The feature id for the '<em><b>EParameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_FROM_ENTITY_OPERATION__EPARAMETERS = OPERATION__EPARAMETERS;

	/**
	 * The feature id for the '<em><b>EExceptions</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_FROM_ENTITY_OPERATION__EEXCEPTIONS = OPERATION__EEXCEPTIONS;

	/**
	 * The feature id for the '<em><b>EGeneric Exceptions</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_FROM_ENTITY_OPERATION__EGENERIC_EXCEPTIONS = OPERATION__EGENERIC_EXCEPTIONS;

	/**
	 * The feature id for the '<em><b>EOperation</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_FROM_ENTITY_OPERATION__EOPERATION = OPERATION__EOPERATION;

	/**
	 * The number of structural features of the '<em>Attribute From Entity Operation</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_FROM_ENTITY_OPERATION_FEATURE_COUNT = OPERATION_FEATURE_COUNT + 0;

	/**
	 * The operation id for the '<em>Get EAnnotation</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_FROM_ENTITY_OPERATION___GET_EANNOTATION__STRING = OPERATION___GET_EANNOTATION__STRING;

	/**
	 * The operation id for the '<em>Get Operation ID</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_FROM_ENTITY_OPERATION___GET_OPERATION_ID = OPERATION___GET_OPERATION_ID;

	/**
	 * The operation id for the '<em>Is Override Of</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_FROM_ENTITY_OPERATION___IS_OVERRIDE_OF__EOPERATION = OPERATION___IS_OVERRIDE_OF__EOPERATION;

	/**
	 * The number of operations of the '<em>Attribute From Entity Operation</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_FROM_ENTITY_OPERATION_OPERATION_COUNT = OPERATION_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.CellsParameterImpl <em>Cells Parameter</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.CellsParameterImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getCellsParameter()
	 * @generated
	 */
	int CELLS_PARAMETER = 37;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CELLS_PARAMETER__EANNOTATIONS = PARAMETER__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CELLS_PARAMETER__NAME = PARAMETER__NAME;

	/**
	 * The feature id for the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CELLS_PARAMETER__ORDERED = PARAMETER__ORDERED;

	/**
	 * The feature id for the '<em><b>Unique</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CELLS_PARAMETER__UNIQUE = PARAMETER__UNIQUE;

	/**
	 * The feature id for the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CELLS_PARAMETER__LOWER_BOUND = PARAMETER__LOWER_BOUND;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CELLS_PARAMETER__UPPER_BOUND = PARAMETER__UPPER_BOUND;

	/**
	 * The feature id for the '<em><b>Many</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CELLS_PARAMETER__MANY = PARAMETER__MANY;

	/**
	 * The feature id for the '<em><b>Required</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CELLS_PARAMETER__REQUIRED = PARAMETER__REQUIRED;

	/**
	 * The feature id for the '<em><b>EType</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CELLS_PARAMETER__ETYPE = PARAMETER__ETYPE;

	/**
	 * The feature id for the '<em><b>EGeneric Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CELLS_PARAMETER__EGENERIC_TYPE = PARAMETER__EGENERIC_TYPE;

	/**
	 * The feature id for the '<em><b>EOperation</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CELLS_PARAMETER__EOPERATION = PARAMETER__EOPERATION;

	/**
	 * The number of structural features of the '<em>Cells Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CELLS_PARAMETER_FEATURE_COUNT = PARAMETER_FEATURE_COUNT + 0;

	/**
	 * The operation id for the '<em>Get EAnnotation</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CELLS_PARAMETER___GET_EANNOTATION__STRING = PARAMETER___GET_EANNOTATION__STRING;

	/**
	 * The number of operations of the '<em>Cells Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CELLS_PARAMETER_OPERATION_COUNT = PARAMETER_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BasicOperationScenarioSetImpl <em>Basic Operation Scenario Set</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BasicOperationScenarioSetImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getBasicOperationScenarioSet()
	 * @generated
	 */
	int BASIC_OPERATION_SCENARIO_SET = 38;

	/**
	 * The feature id for the '<em><b>EAnnotations</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_OPERATION_SCENARIO_SET__EANNOTATIONS = OPERATION__EANNOTATIONS;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_OPERATION_SCENARIO_SET__NAME = OPERATION__NAME;

	/**
	 * The feature id for the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_OPERATION_SCENARIO_SET__ORDERED = OPERATION__ORDERED;

	/**
	 * The feature id for the '<em><b>Unique</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_OPERATION_SCENARIO_SET__UNIQUE = OPERATION__UNIQUE;

	/**
	 * The feature id for the '<em><b>Lower Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_OPERATION_SCENARIO_SET__LOWER_BOUND = OPERATION__LOWER_BOUND;

	/**
	 * The feature id for the '<em><b>Upper Bound</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_OPERATION_SCENARIO_SET__UPPER_BOUND = OPERATION__UPPER_BOUND;

	/**
	 * The feature id for the '<em><b>Many</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_OPERATION_SCENARIO_SET__MANY = OPERATION__MANY;

	/**
	 * The feature id for the '<em><b>Required</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_OPERATION_SCENARIO_SET__REQUIRED = OPERATION__REQUIRED;

	/**
	 * The feature id for the '<em><b>EType</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_OPERATION_SCENARIO_SET__ETYPE = OPERATION__ETYPE;

	/**
	 * The feature id for the '<em><b>EGeneric Type</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_OPERATION_SCENARIO_SET__EGENERIC_TYPE = OPERATION__EGENERIC_TYPE;

	/**
	 * The feature id for the '<em><b>EContaining Class</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_OPERATION_SCENARIO_SET__ECONTAINING_CLASS = OPERATION__ECONTAINING_CLASS;

	/**
	 * The feature id for the '<em><b>EType Parameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_OPERATION_SCENARIO_SET__ETYPE_PARAMETERS = OPERATION__ETYPE_PARAMETERS;

	/**
	 * The feature id for the '<em><b>EParameters</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_OPERATION_SCENARIO_SET__EPARAMETERS = OPERATION__EPARAMETERS;

	/**
	 * The feature id for the '<em><b>EExceptions</b></em>' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_OPERATION_SCENARIO_SET__EEXCEPTIONS = OPERATION__EEXCEPTIONS;

	/**
	 * The feature id for the '<em><b>EGeneric Exceptions</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_OPERATION_SCENARIO_SET__EGENERIC_EXCEPTIONS = OPERATION__EGENERIC_EXCEPTIONS;

	/**
	 * The feature id for the '<em><b>EOperation</b></em>' container reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_OPERATION_SCENARIO_SET__EOPERATION = OPERATION__EOPERATION;

	/**
	 * The feature id for the '<em><b>Basic Scenario Logic</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_OPERATION_SCENARIO_SET__BASIC_SCENARIO_LOGIC = OPERATION_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Basic Operation Scenario Set</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_OPERATION_SCENARIO_SET_FEATURE_COUNT = OPERATION_FEATURE_COUNT + 1;

	/**
	 * The operation id for the '<em>Get EAnnotation</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_OPERATION_SCENARIO_SET___GET_EANNOTATION__STRING = OPERATION___GET_EANNOTATION__STRING;

	/**
	 * The operation id for the '<em>Get Operation ID</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_OPERATION_SCENARIO_SET___GET_OPERATION_ID = OPERATION___GET_OPERATION_ID;

	/**
	 * The operation id for the '<em>Is Override Of</em>' operation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_OPERATION_SCENARIO_SET___IS_OVERRIDE_OF__EOPERATION = OPERATION___IS_OVERRIDE_OF__EOPERATION;

	/**
	 * The number of operations of the '<em>Basic Operation Scenario Set</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_OPERATION_SCENARIO_SET_OPERATION_COUNT = OPERATION_OPERATION_COUNT + 0;


	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BaseEntityTableModuleImpl <em>Base Entity Table Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BaseEntityTableModuleImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getBaseEntityTableModule()
	 * @generated
	 */
	int BASE_ENTITY_TABLE_MODULE = 39;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY_TABLE_MODULE__DEPENDENCIES = Module_managementPackage.MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY_TABLE_MODULE__THE_DESCRIPTION = Module_managementPackage.MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY_TABLE_MODULE__LICENSE = Module_managementPackage.MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY_TABLE_MODULE__NAME = Module_managementPackage.MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY_TABLE_MODULE__VERSION = Module_managementPackage.MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY_TABLE_MODULE__LONG_NAME = Module_managementPackage.MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Base Entity Tables</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY_TABLE_MODULE__BASE_ENTITY_TABLES = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Base Entity Table Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY_TABLE_MODULE_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Base Entity Table Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASE_ENTITY_TABLE_MODULE_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.DerivedEntityTableModuleImpl <em>Derived Entity Table Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.DerivedEntityTableModuleImpl
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getDerivedEntityTableModule()
	 * @generated
	 */
	int DERIVED_ENTITY_TABLE_MODULE = 40;

	/**
	 * The feature id for the '<em><b>Dependencies</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_TABLE_MODULE__DEPENDENCIES = Module_managementPackage.MODULE__DEPENDENCIES;

	/**
	 * The feature id for the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_TABLE_MODULE__THE_DESCRIPTION = Module_managementPackage.MODULE__THE_DESCRIPTION;

	/**
	 * The feature id for the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_TABLE_MODULE__LICENSE = Module_managementPackage.MODULE__LICENSE;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_TABLE_MODULE__NAME = Module_managementPackage.MODULE__NAME;

	/**
	 * The feature id for the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_TABLE_MODULE__VERSION = Module_managementPackage.MODULE__VERSION;

	/**
	 * The feature id for the '<em><b>Long Name</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_TABLE_MODULE__LONG_NAME = Module_managementPackage.MODULE__LONG_NAME;

	/**
	 * The feature id for the '<em><b>Derived Entity Tables</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_TABLE_MODULE__DERIVED_ENTITY_TABLES = Module_managementPackage.MODULE_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Derived Entity Table Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_TABLE_MODULE_FEATURE_COUNT = Module_managementPackage.MODULE_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Derived Entity Table Module</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_TABLE_MODULE_OPERATION_COUNT = Module_managementPackage.MODULE_OPERATION_COUNT + 0;


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
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AggregateFunctionSpec <em>Aggregate Function Spec</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Aggregate Function Spec</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AggregateFunctionSpec
	 * @generated
	 */
	EClass getAggregateFunctionSpec();

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
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BooleanOperation <em>Boolean Operation</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Boolean Operation</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BooleanOperation
	 * @generated
	 */
	EClass getBooleanOperation();

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
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.EntityParameter <em>Entity Parameter</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Entity Parameter</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.EntityParameter
	 * @generated
	 */
	EClass getEntityParameter();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.EntityParameter#getEntity <em>Entity</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Entity</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.EntityParameter#getEntity()
	 * @see #getEntityParameter()
	 * @generated
	 */
	EReference getEntityParameter_Entity();

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
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicScenarioOperation <em>Basic Scenario Operation</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Basic Scenario Operation</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicScenarioOperation
	 * @generated
	 */
	EClass getBasicScenarioOperation();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicScenarioOperation#getSceanrio <em>Sceanrio</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Sceanrio</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicScenarioOperation#getSceanrio()
	 * @see #getBasicScenarioOperation()
	 * @generated
	 */
	EReference getBasicScenarioOperation_Sceanrio();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicScenarioOperation#getFunctionSpec <em>Function Spec</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Function Spec</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicScenarioOperation#getFunctionSpec()
	 * @see #getBasicScenarioOperation()
	 * @generated
	 */
	EReference getBasicScenarioOperation_FunctionSpec();

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
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.OperationParameter <em>Operation Parameter</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Operation Parameter</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.OperationParameter
	 * @generated
	 */
	EClass getOperationParameter();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.OperationParameter#getOperation <em>Operation</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Operation</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.OperationParameter#getOperation()
	 * @see #getOperationParameter()
	 * @generated
	 */
	EReference getOperationParameter_Operation();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ProxyOperationParameter <em>Proxy Operation Parameter</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Proxy Operation Parameter</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ProxyOperationParameter
	 * @generated
	 */
	EClass getProxyOperationParameter();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ProxyOperationParameter#getEntity <em>Entity</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Entity</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ProxyOperationParameter#getEntity()
	 * @see #getProxyOperationParameter()
	 * @generated
	 */
	EReference getProxyOperationParameter_Entity();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ProxyOperationParameter#getOperation_name <em>Operation name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Operation name</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ProxyOperationParameter#getOperation_name()
	 * @see #getProxyOperationParameter()
	 * @generated
	 */
	EAttribute getProxyOperationParameter_Operation_name();

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
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ProxyFeatureParameter#getFeature_name <em>Feature name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Feature name</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.ProxyFeatureParameter#getFeature_name()
	 * @see #getProxyFeatureParameter()
	 * @generated
	 */
	EAttribute getProxyFeatureParameter_Feature_name();

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
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.GroupByRowCreationApproach <em>Group By Row Creation Approach</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Group By Row Creation Approach</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.GroupByRowCreationApproach
	 * @generated
	 */
	EClass getGroupByRowCreationApproach();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.GroupByRowCreationApproach#getGroupByColumns <em>Group By Columns</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Group By Columns</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.GroupByRowCreationApproach#getGroupByColumns()
	 * @see #getGroupByRowCreationApproach()
	 * @generated
	 */
	EReference getGroupByRowCreationApproach_GroupByColumns();

	/**
	 * Returns the meta object for the attribute list '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.GroupByRowCreationApproach#getGroupByCubeColumnNames <em>Group By Cube Column Names</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute list '<em>Group By Cube Column Names</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.GroupByRowCreationApproach#getGroupByCubeColumnNames()
	 * @see #getGroupByRowCreationApproach()
	 * @generated
	 */
	EAttribute getGroupByRowCreationApproach_GroupByCubeColumnNames();

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
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.EntityTable <em>Entity Table</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Entity Table</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.EntityTable
	 * @generated
	 */
	EClass getEntityTable();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntityTable <em>Derived Entity Table</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Derived Entity Table</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntityTable
	 * @generated
	 */
	EClass getDerivedEntityTable();

	/**
	 * Returns the meta object for the reference list '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntityTable#getSourceTables <em>Source Tables</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference list '<em>Source Tables</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntityTable#getSourceTables()
	 * @see #getDerivedEntityTable()
	 * @generated
	 */
	EReference getDerivedEntityTable_SourceTables();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntityTable#getContainedEntityType <em>Contained Entity Type</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Contained Entity Type</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntityTable#getContainedEntityType()
	 * @see #getDerivedEntityTable()
	 * @generated
	 */
	EReference getDerivedEntityTable_ContainedEntityType();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BaseEntityTable <em>Base Entity Table</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Base Entity Table</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BaseEntityTable
	 * @generated
	 */
	EClass getBaseEntityTable();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Operation <em>Operation</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Operation</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.Operation
	 * @generated
	 */
	EClass getOperation();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicOperation <em>Basic Operation</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Basic Operation</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicOperation
	 * @generated
	 */
	EClass getBasicOperation();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicOperation#getFunctionSpec <em>Function Spec</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Function Spec</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicOperation#getFunctionSpec()
	 * @see #getBasicOperation()
	 * @generated
	 */
	EReference getBasicOperation_FunctionSpec();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AggregateOperation <em>Aggregate Operation</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Aggregate Operation</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AggregateOperation
	 * @generated
	 */
	EClass getAggregateOperation();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AggregateOperation#getFunctionSpec <em>Function Spec</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Function Spec</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AggregateOperation#getFunctionSpec()
	 * @see #getAggregateOperation()
	 * @generated
	 */
	EReference getAggregateOperation_FunctionSpec();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.EntityFromSourceOperation <em>Entity From Source Operation</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Entity From Source Operation</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.EntityFromSourceOperation
	 * @generated
	 */
	EClass getEntityFromSourceOperation();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AttributeFromEntityOperation <em>Attribute From Entity Operation</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Attribute From Entity Operation</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.AttributeFromEntityOperation
	 * @generated
	 */
	EClass getAttributeFromEntityOperation();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.CellsParameter <em>Cells Parameter</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Cells Parameter</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.CellsParameter
	 * @generated
	 */
	EClass getCellsParameter();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicOperationScenarioSet <em>Basic Operation Scenario Set</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Basic Operation Scenario Set</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicOperationScenarioSet
	 * @generated
	 */
	EClass getBasicOperationScenarioSet();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicOperationScenarioSet#getBasicScenarioLogic <em>Basic Scenario Logic</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Basic Scenario Logic</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BasicOperationScenarioSet#getBasicScenarioLogic()
	 * @see #getBasicOperationScenarioSet()
	 * @generated
	 */
	EReference getBasicOperationScenarioSet_BasicScenarioLogic();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BaseEntityTableModule <em>Base Entity Table Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Base Entity Table Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BaseEntityTableModule
	 * @generated
	 */
	EClass getBaseEntityTableModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BaseEntityTableModule#getBaseEntityTables <em>Base Entity Tables</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Base Entity Tables</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.BaseEntityTableModule#getBaseEntityTables()
	 * @see #getBaseEntityTableModule()
	 * @generated
	 */
	EReference getBaseEntityTableModule_BaseEntityTables();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntityTableModule <em>Derived Entity Table Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Derived Entity Table Module</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntityTableModule
	 * @generated
	 */
	EClass getDerivedEntityTableModule();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntityTableModule#getDerivedEntityTables <em>Derived Entity Tables</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Derived Entity Tables</em>'.
	 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.DerivedEntityTableModule#getDerivedEntityTables()
	 * @see #getDerivedEntityTableModule()
	 * @generated
	 */
	EReference getDerivedEntityTableModule_DerivedEntityTables();

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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.AggregateFunctionSpecImpl <em>Aggregate Function Spec</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.AggregateFunctionSpecImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getAggregateFunctionSpec()
		 * @generated
		 */
		EClass AGGREGATE_FUNCTION_SPEC = eINSTANCE.getAggregateFunctionSpec();

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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BooleanOperationImpl <em>Boolean Operation</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BooleanOperationImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getBooleanOperation()
		 * @generated
		 */
		EClass BOOLEAN_OPERATION = eINSTANCE.getBooleanOperation();

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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.EntityParameterImpl <em>Entity Parameter</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.EntityParameterImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getEntityParameter()
		 * @generated
		 */
		EClass ENTITY_PARAMETER = eINSTANCE.getEntityParameter();

		/**
		 * The meta object literal for the '<em><b>Entity</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ENTITY_PARAMETER__ENTITY = eINSTANCE.getEntityParameter_Entity();

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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BasicScenarioOperationImpl <em>Basic Scenario Operation</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BasicScenarioOperationImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getBasicScenarioOperation()
		 * @generated
		 */
		EClass BASIC_SCENARIO_OPERATION = eINSTANCE.getBasicScenarioOperation();

		/**
		 * The meta object literal for the '<em><b>Sceanrio</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BASIC_SCENARIO_OPERATION__SCEANRIO = eINSTANCE.getBasicScenarioOperation_Sceanrio();

		/**
		 * The meta object literal for the '<em><b>Function Spec</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BASIC_SCENARIO_OPERATION__FUNCTION_SPEC = eINSTANCE.getBasicScenarioOperation_FunctionSpec();

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
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.OperationParameterImpl <em>Operation Parameter</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.OperationParameterImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getOperationParameter()
		 * @generated
		 */
		EClass OPERATION_PARAMETER = eINSTANCE.getOperationParameter();

		/**
		 * The meta object literal for the '<em><b>Operation</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference OPERATION_PARAMETER__OPERATION = eINSTANCE.getOperationParameter_Operation();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.ProxyOperationParameterImpl <em>Proxy Operation Parameter</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.ProxyOperationParameterImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getProxyOperationParameter()
		 * @generated
		 */
		EClass PROXY_OPERATION_PARAMETER = eINSTANCE.getProxyOperationParameter();

		/**
		 * The meta object literal for the '<em><b>Entity</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference PROXY_OPERATION_PARAMETER__ENTITY = eINSTANCE.getProxyOperationParameter_Entity();

		/**
		 * The meta object literal for the '<em><b>Operation name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute PROXY_OPERATION_PARAMETER__OPERATION_NAME = eINSTANCE.getProxyOperationParameter_Operation_name();

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
		 * The meta object literal for the '<em><b>Feature name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute PROXY_FEATURE_PARAMETER__FEATURE_NAME = eINSTANCE.getProxyFeatureParameter_Feature_name();

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
		 * The meta object literal for the '<em><b>Created Cube Columns</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ROW_CREATION_APPROACH__CREATED_CUBE_COLUMNS = eINSTANCE.getRowCreationApproach_CreatedCubeColumns();

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
		 * The meta object literal for the '<em><b>Group By Columns</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference GROUP_BY_ROW_CREATION_APPROACH__GROUP_BY_COLUMNS = eINSTANCE.getGroupByRowCreationApproach_GroupByColumns();

		/**
		 * The meta object literal for the '<em><b>Group By Cube Column Names</b></em>' attribute list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute GROUP_BY_ROW_CREATION_APPROACH__GROUP_BY_CUBE_COLUMN_NAMES = eINSTANCE.getGroupByRowCreationApproach_GroupByCubeColumnNames();

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

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.EntityTableImpl <em>Entity Table</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.EntityTableImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getEntityTable()
		 * @generated
		 */
		EClass ENTITY_TABLE = eINSTANCE.getEntityTable();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.DerivedEntityTableImpl <em>Derived Entity Table</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.DerivedEntityTableImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getDerivedEntityTable()
		 * @generated
		 */
		EClass DERIVED_ENTITY_TABLE = eINSTANCE.getDerivedEntityTable();

		/**
		 * The meta object literal for the '<em><b>Source Tables</b></em>' reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference DERIVED_ENTITY_TABLE__SOURCE_TABLES = eINSTANCE.getDerivedEntityTable_SourceTables();

		/**
		 * The meta object literal for the '<em><b>Contained Entity Type</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference DERIVED_ENTITY_TABLE__CONTAINED_ENTITY_TYPE = eINSTANCE.getDerivedEntityTable_ContainedEntityType();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BaseEntityTableImpl <em>Base Entity Table</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BaseEntityTableImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getBaseEntityTable()
		 * @generated
		 */
		EClass BASE_ENTITY_TABLE = eINSTANCE.getBaseEntityTable();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.OperationImpl <em>Operation</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.OperationImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getOperation()
		 * @generated
		 */
		EClass OPERATION = eINSTANCE.getOperation();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BasicOperationImpl <em>Basic Operation</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BasicOperationImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getBasicOperation()
		 * @generated
		 */
		EClass BASIC_OPERATION = eINSTANCE.getBasicOperation();

		/**
		 * The meta object literal for the '<em><b>Function Spec</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BASIC_OPERATION__FUNCTION_SPEC = eINSTANCE.getBasicOperation_FunctionSpec();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.AggregateOperationImpl <em>Aggregate Operation</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.AggregateOperationImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getAggregateOperation()
		 * @generated
		 */
		EClass AGGREGATE_OPERATION = eINSTANCE.getAggregateOperation();

		/**
		 * The meta object literal for the '<em><b>Function Spec</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference AGGREGATE_OPERATION__FUNCTION_SPEC = eINSTANCE.getAggregateOperation_FunctionSpec();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.EntityFromSourceOperationImpl <em>Entity From Source Operation</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.EntityFromSourceOperationImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getEntityFromSourceOperation()
		 * @generated
		 */
		EClass ENTITY_FROM_SOURCE_OPERATION = eINSTANCE.getEntityFromSourceOperation();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.AttributeFromEntityOperationImpl <em>Attribute From Entity Operation</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.AttributeFromEntityOperationImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getAttributeFromEntityOperation()
		 * @generated
		 */
		EClass ATTRIBUTE_FROM_ENTITY_OPERATION = eINSTANCE.getAttributeFromEntityOperation();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.CellsParameterImpl <em>Cells Parameter</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.CellsParameterImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getCellsParameter()
		 * @generated
		 */
		EClass CELLS_PARAMETER = eINSTANCE.getCellsParameter();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BasicOperationScenarioSetImpl <em>Basic Operation Scenario Set</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BasicOperationScenarioSetImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getBasicOperationScenarioSet()
		 * @generated
		 */
		EClass BASIC_OPERATION_SCENARIO_SET = eINSTANCE.getBasicOperationScenarioSet();

		/**
		 * The meta object literal for the '<em><b>Basic Scenario Logic</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BASIC_OPERATION_SCENARIO_SET__BASIC_SCENARIO_LOGIC = eINSTANCE.getBasicOperationScenarioSet_BasicScenarioLogic();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BaseEntityTableModuleImpl <em>Base Entity Table Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.BaseEntityTableModuleImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getBaseEntityTableModule()
		 * @generated
		 */
		EClass BASE_ENTITY_TABLE_MODULE = eINSTANCE.getBaseEntityTableModule();

		/**
		 * The meta object literal for the '<em><b>Base Entity Tables</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BASE_ENTITY_TABLE_MODULE__BASE_ENTITY_TABLES = eINSTANCE.getBaseEntityTableModule_BaseEntityTables();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.DerivedEntityTableModuleImpl <em>Derived Entity Table Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.DerivedEntityTableModuleImpl
		 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_logic.impl.Ecore_plus_logicPackageImpl#getDerivedEntityTableModule()
		 * @generated
		 */
		EClass DERIVED_ENTITY_TABLE_MODULE = eINSTANCE.getDerivedEntityTableModule();

		/**
		 * The meta object literal for the '<em><b>Derived Entity Tables</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference DERIVED_ENTITY_TABLE_MODULE__DERIVED_ENTITY_TABLES = eINSTANCE.getDerivedEntityTableModule_DerivedEntityTables();

	}

} //Ecore_plus_logicPackage
