/**
 */
package org.eclipse.efbt.openregspecs.model.data_meta_model;

import org.eclipse.efbt.openregspecs.model.module_management.Module_managementPackage;

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
 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.Data_meta_modelFactory
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
	Data_meta_modelPackage eINSTANCE = org.eclipse.efbt.openregspecs.model.data_meta_model.impl.Data_meta_modelPackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.impl.ElementImpl <em>Element</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.ElementImpl
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.Data_meta_modelPackageImpl#getElement()
	 * @generated
	 */
	int ELEMENT = 8;

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
	 * The meta object id for the '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.impl.NamedElementImpl <em>Named Element</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.NamedElementImpl
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.Data_meta_modelPackageImpl#getNamedElement()
	 * @generated
	 */
	int NAMED_ELEMENT = 9;

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
	 * The meta object id for the '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.impl.TypedElementImpl <em>Typed Element</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.TypedElementImpl
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.Data_meta_modelPackageImpl#getTypedElement()
	 * @generated
	 */
	int TYPED_ELEMENT = 12;

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
	 * The meta object id for the '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.impl.StructuralFeatureImpl <em>Structural Feature</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.StructuralFeatureImpl
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.Data_meta_modelPackageImpl#getStructuralFeature()
	 * @generated
	 */
	int STRUCTURAL_FEATURE = 11;

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
	 * The meta object id for the '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.impl.AttributeImpl <em>Attribute</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.AttributeImpl
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.Data_meta_modelPackageImpl#getAttribute()
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
	 * The feature id for the '<em><b>Concept</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE__CONCEPT = STRUCTURAL_FEATURE_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE__ORDERED = STRUCTURAL_FEATURE_FEATURE_COUNT + 2;

	/**
	 * The number of structural features of the '<em>Attribute</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_FEATURE_COUNT = STRUCTURAL_FEATURE_FEATURE_COUNT + 3;

	/**
	 * The number of operations of the '<em>Attribute</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ATTRIBUTE_OPERATION_COUNT = STRUCTURAL_FEATURE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.impl.ConceptImpl <em>Concept</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.ConceptImpl
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.Data_meta_modelPackageImpl#getConcept()
	 * @generated
	 */
	int CONCEPT = 1;

	/**
	 * The feature id for the '<em><b>Concept Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONCEPT__CONCEPT_NAME = 0;

	/**
	 * The number of structural features of the '<em>Concept</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONCEPT_FEATURE_COUNT = 1;

	/**
	 * The number of operations of the '<em>Concept</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CONCEPT_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.impl.RelationshipAttributeImpl <em>Relationship Attribute</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.RelationshipAttributeImpl
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.Data_meta_modelPackageImpl#getRelationshipAttribute()
	 * @generated
	 */
	int RELATIONSHIP_ATTRIBUTE = 2;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RELATIONSHIP_ATTRIBUTE__NAME = ATTRIBUTE__NAME;

	/**
	 * The feature id for the '<em><b>Classifier</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RELATIONSHIP_ATTRIBUTE__CLASSIFIER = ATTRIBUTE__CLASSIFIER;

	/**
	 * The feature id for the '<em><b>Is PK</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RELATIONSHIP_ATTRIBUTE__IS_PK = ATTRIBUTE__IS_PK;

	/**
	 * The feature id for the '<em><b>Concept</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RELATIONSHIP_ATTRIBUTE__CONCEPT = ATTRIBUTE__CONCEPT;

	/**
	 * The feature id for the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RELATIONSHIP_ATTRIBUTE__ORDERED = ATTRIBUTE__ORDERED;

	/**
	 * The feature id for the '<em><b>Entity</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RELATIONSHIP_ATTRIBUTE__ENTITY = ATTRIBUTE_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Containment</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RELATIONSHIP_ATTRIBUTE__CONTAINMENT = ATTRIBUTE_FEATURE_COUNT + 1;

	/**
	 * The feature id for the '<em><b>Mandatory</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RELATIONSHIP_ATTRIBUTE__MANDATORY = ATTRIBUTE_FEATURE_COUNT + 2;

	/**
	 * The feature id for the '<em><b>Dominant</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RELATIONSHIP_ATTRIBUTE__DOMINANT = ATTRIBUTE_FEATURE_COUNT + 3;

	/**
	 * The number of structural features of the '<em>Relationship Attribute</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RELATIONSHIP_ATTRIBUTE_FEATURE_COUNT = ATTRIBUTE_FEATURE_COUNT + 4;

	/**
	 * The number of operations of the '<em>Relationship Attribute</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RELATIONSHIP_ATTRIBUTE_OPERATION_COUNT = ATTRIBUTE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.impl.OneToOneRelationshipAttributeImpl <em>One To One Relationship Attribute</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.OneToOneRelationshipAttributeImpl
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.Data_meta_modelPackageImpl#getOneToOneRelationshipAttribute()
	 * @generated
	 */
	int ONE_TO_ONE_RELATIONSHIP_ATTRIBUTE = 3;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_ONE_RELATIONSHIP_ATTRIBUTE__NAME = RELATIONSHIP_ATTRIBUTE__NAME;

	/**
	 * The feature id for the '<em><b>Classifier</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_ONE_RELATIONSHIP_ATTRIBUTE__CLASSIFIER = RELATIONSHIP_ATTRIBUTE__CLASSIFIER;

	/**
	 * The feature id for the '<em><b>Is PK</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_ONE_RELATIONSHIP_ATTRIBUTE__IS_PK = RELATIONSHIP_ATTRIBUTE__IS_PK;

	/**
	 * The feature id for the '<em><b>Concept</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_ONE_RELATIONSHIP_ATTRIBUTE__CONCEPT = RELATIONSHIP_ATTRIBUTE__CONCEPT;

	/**
	 * The feature id for the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_ONE_RELATIONSHIP_ATTRIBUTE__ORDERED = RELATIONSHIP_ATTRIBUTE__ORDERED;

	/**
	 * The feature id for the '<em><b>Entity</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_ONE_RELATIONSHIP_ATTRIBUTE__ENTITY = RELATIONSHIP_ATTRIBUTE__ENTITY;

	/**
	 * The feature id for the '<em><b>Containment</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_ONE_RELATIONSHIP_ATTRIBUTE__CONTAINMENT = RELATIONSHIP_ATTRIBUTE__CONTAINMENT;

	/**
	 * The feature id for the '<em><b>Mandatory</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_ONE_RELATIONSHIP_ATTRIBUTE__MANDATORY = RELATIONSHIP_ATTRIBUTE__MANDATORY;

	/**
	 * The feature id for the '<em><b>Dominant</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_ONE_RELATIONSHIP_ATTRIBUTE__DOMINANT = RELATIONSHIP_ATTRIBUTE__DOMINANT;

	/**
	 * The number of structural features of the '<em>One To One Relationship Attribute</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_ONE_RELATIONSHIP_ATTRIBUTE_FEATURE_COUNT = RELATIONSHIP_ATTRIBUTE_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>One To One Relationship Attribute</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_ONE_RELATIONSHIP_ATTRIBUTE_OPERATION_COUNT = RELATIONSHIP_ATTRIBUTE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.impl.ManyToOneRelationshipAttributeImpl <em>Many To One Relationship Attribute</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.ManyToOneRelationshipAttributeImpl
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.Data_meta_modelPackageImpl#getManyToOneRelationshipAttribute()
	 * @generated
	 */
	int MANY_TO_ONE_RELATIONSHIP_ATTRIBUTE = 4;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANY_TO_ONE_RELATIONSHIP_ATTRIBUTE__NAME = RELATIONSHIP_ATTRIBUTE__NAME;

	/**
	 * The feature id for the '<em><b>Classifier</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANY_TO_ONE_RELATIONSHIP_ATTRIBUTE__CLASSIFIER = RELATIONSHIP_ATTRIBUTE__CLASSIFIER;

	/**
	 * The feature id for the '<em><b>Is PK</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANY_TO_ONE_RELATIONSHIP_ATTRIBUTE__IS_PK = RELATIONSHIP_ATTRIBUTE__IS_PK;

	/**
	 * The feature id for the '<em><b>Concept</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANY_TO_ONE_RELATIONSHIP_ATTRIBUTE__CONCEPT = RELATIONSHIP_ATTRIBUTE__CONCEPT;

	/**
	 * The feature id for the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANY_TO_ONE_RELATIONSHIP_ATTRIBUTE__ORDERED = RELATIONSHIP_ATTRIBUTE__ORDERED;

	/**
	 * The feature id for the '<em><b>Entity</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANY_TO_ONE_RELATIONSHIP_ATTRIBUTE__ENTITY = RELATIONSHIP_ATTRIBUTE__ENTITY;

	/**
	 * The feature id for the '<em><b>Containment</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANY_TO_ONE_RELATIONSHIP_ATTRIBUTE__CONTAINMENT = RELATIONSHIP_ATTRIBUTE__CONTAINMENT;

	/**
	 * The feature id for the '<em><b>Mandatory</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANY_TO_ONE_RELATIONSHIP_ATTRIBUTE__MANDATORY = RELATIONSHIP_ATTRIBUTE__MANDATORY;

	/**
	 * The feature id for the '<em><b>Dominant</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANY_TO_ONE_RELATIONSHIP_ATTRIBUTE__DOMINANT = RELATIONSHIP_ATTRIBUTE__DOMINANT;

	/**
	 * The number of structural features of the '<em>Many To One Relationship Attribute</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANY_TO_ONE_RELATIONSHIP_ATTRIBUTE_FEATURE_COUNT = RELATIONSHIP_ATTRIBUTE_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Many To One Relationship Attribute</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANY_TO_ONE_RELATIONSHIP_ATTRIBUTE_OPERATION_COUNT = RELATIONSHIP_ATTRIBUTE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.impl.OneToManyRelationshipAttributeImpl <em>One To Many Relationship Attribute</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.OneToManyRelationshipAttributeImpl
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.Data_meta_modelPackageImpl#getOneToManyRelationshipAttribute()
	 * @generated
	 */
	int ONE_TO_MANY_RELATIONSHIP_ATTRIBUTE = 5;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_MANY_RELATIONSHIP_ATTRIBUTE__NAME = RELATIONSHIP_ATTRIBUTE__NAME;

	/**
	 * The feature id for the '<em><b>Classifier</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_MANY_RELATIONSHIP_ATTRIBUTE__CLASSIFIER = RELATIONSHIP_ATTRIBUTE__CLASSIFIER;

	/**
	 * The feature id for the '<em><b>Is PK</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_MANY_RELATIONSHIP_ATTRIBUTE__IS_PK = RELATIONSHIP_ATTRIBUTE__IS_PK;

	/**
	 * The feature id for the '<em><b>Concept</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_MANY_RELATIONSHIP_ATTRIBUTE__CONCEPT = RELATIONSHIP_ATTRIBUTE__CONCEPT;

	/**
	 * The feature id for the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_MANY_RELATIONSHIP_ATTRIBUTE__ORDERED = RELATIONSHIP_ATTRIBUTE__ORDERED;

	/**
	 * The feature id for the '<em><b>Entity</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_MANY_RELATIONSHIP_ATTRIBUTE__ENTITY = RELATIONSHIP_ATTRIBUTE__ENTITY;

	/**
	 * The feature id for the '<em><b>Containment</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_MANY_RELATIONSHIP_ATTRIBUTE__CONTAINMENT = RELATIONSHIP_ATTRIBUTE__CONTAINMENT;

	/**
	 * The feature id for the '<em><b>Mandatory</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_MANY_RELATIONSHIP_ATTRIBUTE__MANDATORY = RELATIONSHIP_ATTRIBUTE__MANDATORY;

	/**
	 * The feature id for the '<em><b>Dominant</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_MANY_RELATIONSHIP_ATTRIBUTE__DOMINANT = RELATIONSHIP_ATTRIBUTE__DOMINANT;

	/**
	 * The number of structural features of the '<em>One To Many Relationship Attribute</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_MANY_RELATIONSHIP_ATTRIBUTE_FEATURE_COUNT = RELATIONSHIP_ATTRIBUTE_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>One To Many Relationship Attribute</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ONE_TO_MANY_RELATIONSHIP_ATTRIBUTE_OPERATION_COUNT = RELATIONSHIP_ATTRIBUTE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.impl.ManyToManyRelationshipAttributeImpl <em>Many To Many Relationship Attribute</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.ManyToManyRelationshipAttributeImpl
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.Data_meta_modelPackageImpl#getManyToManyRelationshipAttribute()
	 * @generated
	 */
	int MANY_TO_MANY_RELATIONSHIP_ATTRIBUTE = 6;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANY_TO_MANY_RELATIONSHIP_ATTRIBUTE__NAME = RELATIONSHIP_ATTRIBUTE__NAME;

	/**
	 * The feature id for the '<em><b>Classifier</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANY_TO_MANY_RELATIONSHIP_ATTRIBUTE__CLASSIFIER = RELATIONSHIP_ATTRIBUTE__CLASSIFIER;

	/**
	 * The feature id for the '<em><b>Is PK</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANY_TO_MANY_RELATIONSHIP_ATTRIBUTE__IS_PK = RELATIONSHIP_ATTRIBUTE__IS_PK;

	/**
	 * The feature id for the '<em><b>Concept</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANY_TO_MANY_RELATIONSHIP_ATTRIBUTE__CONCEPT = RELATIONSHIP_ATTRIBUTE__CONCEPT;

	/**
	 * The feature id for the '<em><b>Ordered</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANY_TO_MANY_RELATIONSHIP_ATTRIBUTE__ORDERED = RELATIONSHIP_ATTRIBUTE__ORDERED;

	/**
	 * The feature id for the '<em><b>Entity</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANY_TO_MANY_RELATIONSHIP_ATTRIBUTE__ENTITY = RELATIONSHIP_ATTRIBUTE__ENTITY;

	/**
	 * The feature id for the '<em><b>Containment</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANY_TO_MANY_RELATIONSHIP_ATTRIBUTE__CONTAINMENT = RELATIONSHIP_ATTRIBUTE__CONTAINMENT;

	/**
	 * The feature id for the '<em><b>Mandatory</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANY_TO_MANY_RELATIONSHIP_ATTRIBUTE__MANDATORY = RELATIONSHIP_ATTRIBUTE__MANDATORY;

	/**
	 * The feature id for the '<em><b>Dominant</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANY_TO_MANY_RELATIONSHIP_ATTRIBUTE__DOMINANT = RELATIONSHIP_ATTRIBUTE__DOMINANT;

	/**
	 * The number of structural features of the '<em>Many To Many Relationship Attribute</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANY_TO_MANY_RELATIONSHIP_ATTRIBUTE_FEATURE_COUNT = RELATIONSHIP_ATTRIBUTE_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Many To Many Relationship Attribute</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MANY_TO_MANY_RELATIONSHIP_ATTRIBUTE_OPERATION_COUNT = RELATIONSHIP_ATTRIBUTE_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.impl.ClassifierImpl <em>Classifier</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.ClassifierImpl
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.Data_meta_modelPackageImpl#getClassifier()
	 * @generated
	 */
	int CLASSIFIER = 7;

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
	 * The meta object id for the '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.impl.EntityModuleImpl <em>Entity Module</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.EntityModuleImpl
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.Data_meta_modelPackageImpl#getEntityModule()
	 * @generated
	 */
	int ENTITY_MODULE = 10;

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
	 * The meta object id for the '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.impl.EntityImpl <em>Entity</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.EntityImpl
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.Data_meta_modelPackageImpl#getEntity()
	 * @generated
	 */
	int ENTITY = 14;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY__NAME = NAMED_ELEMENT__NAME;

	/**
	 * The number of structural features of the '<em>Entity</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_FEATURE_COUNT = NAMED_ELEMENT_FEATURE_COUNT + 0;

	/**
	 * The number of operations of the '<em>Entity</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENTITY_OPERATION_COUNT = NAMED_ELEMENT_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.impl.GeneratedEntityImpl <em>Generated Entity</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.GeneratedEntityImpl
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.Data_meta_modelPackageImpl#getGeneratedEntity()
	 * @generated
	 */
	int GENERATED_ENTITY = 13;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY__NAME = ENTITY__NAME;

	/**
	 * The feature id for the '<em><b>Attributes</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int GENERATED_ENTITY__ATTRIBUTES = ENTITY_FEATURE_COUNT + 0;

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
	 * The meta object id for the '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.impl.DerivedEntityImpl <em>Derived Entity</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.DerivedEntityImpl
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.Data_meta_modelPackageImpl#getDerivedEntity()
	 * @generated
	 */
	int DERIVED_ENTITY = 15;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY__NAME = ENTITY__NAME;

	/**
	 * The feature id for the '<em><b>Attributes</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY__ATTRIBUTES = ENTITY_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Derived Entity</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_FEATURE_COUNT = ENTITY_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Derived Entity</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int DERIVED_ENTITY_OPERATION_COUNT = ENTITY_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.impl.BasicEntityImpl <em>Basic Entity</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.BasicEntityImpl
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.Data_meta_modelPackageImpl#getBasicEntity()
	 * @generated
	 */
	int BASIC_ENTITY = 16;

	/**
	 * The feature id for the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_ENTITY__NAME = ENTITY__NAME;

	/**
	 * The feature id for the '<em><b>Attributes</b></em>' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_ENTITY__ATTRIBUTES = ENTITY_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Super Class</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_ENTITY__SUPER_CLASS = ENTITY_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Basic Entity</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_ENTITY_FEATURE_COUNT = ENTITY_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Basic Entity</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int BASIC_ENTITY_OPERATION_COUNT = ENTITY_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.impl.EnumMemberImpl <em>Enum Member</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.EnumMemberImpl
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.Data_meta_modelPackageImpl#getEnumMember()
	 * @generated
	 */
	int ENUM_MEMBER = 17;

	/**
	 * The number of structural features of the '<em>Enum Member</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENUM_MEMBER_FEATURE_COUNT = 0;

	/**
	 * The number of operations of the '<em>Enum Member</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int ENUM_MEMBER_OPERATION_COUNT = 0;


	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.Attribute <em>Attribute</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Attribute</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.Attribute
	 * @generated
	 */
	EClass getAttribute();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.Attribute#isIsPK <em>Is PK</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Is PK</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.Attribute#isIsPK()
	 * @see #getAttribute()
	 * @generated
	 */
	EAttribute getAttribute_IsPK();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.Attribute#getConcept <em>Concept</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Concept</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.Attribute#getConcept()
	 * @see #getAttribute()
	 * @generated
	 */
	EReference getAttribute_Concept();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.Attribute#isOrdered <em>Ordered</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Ordered</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.Attribute#isOrdered()
	 * @see #getAttribute()
	 * @generated
	 */
	EAttribute getAttribute_Ordered();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.Concept <em>Concept</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Concept</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.Concept
	 * @generated
	 */
	EClass getConcept();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.Concept#getConceptName <em>Concept Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Concept Name</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.Concept#getConceptName()
	 * @see #getConcept()
	 * @generated
	 */
	EAttribute getConcept_ConceptName();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.RelationshipAttribute <em>Relationship Attribute</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Relationship Attribute</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.RelationshipAttribute
	 * @generated
	 */
	EClass getRelationshipAttribute();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.RelationshipAttribute#getEntity <em>Entity</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Entity</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.RelationshipAttribute#getEntity()
	 * @see #getRelationshipAttribute()
	 * @generated
	 */
	EReference getRelationshipAttribute_Entity();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.RelationshipAttribute#isContainment <em>Containment</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Containment</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.RelationshipAttribute#isContainment()
	 * @see #getRelationshipAttribute()
	 * @generated
	 */
	EAttribute getRelationshipAttribute_Containment();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.RelationshipAttribute#isMandatory <em>Mandatory</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Mandatory</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.RelationshipAttribute#isMandatory()
	 * @see #getRelationshipAttribute()
	 * @generated
	 */
	EAttribute getRelationshipAttribute_Mandatory();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.RelationshipAttribute#isDominant <em>Dominant</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Dominant</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.RelationshipAttribute#isDominant()
	 * @see #getRelationshipAttribute()
	 * @generated
	 */
	EAttribute getRelationshipAttribute_Dominant();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.OneToOneRelationshipAttribute <em>One To One Relationship Attribute</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>One To One Relationship Attribute</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.OneToOneRelationshipAttribute
	 * @generated
	 */
	EClass getOneToOneRelationshipAttribute();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.ManyToOneRelationshipAttribute <em>Many To One Relationship Attribute</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Many To One Relationship Attribute</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.ManyToOneRelationshipAttribute
	 * @generated
	 */
	EClass getManyToOneRelationshipAttribute();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.OneToManyRelationshipAttribute <em>One To Many Relationship Attribute</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>One To Many Relationship Attribute</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.OneToManyRelationshipAttribute
	 * @generated
	 */
	EClass getOneToManyRelationshipAttribute();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.ManyToManyRelationshipAttribute <em>Many To Many Relationship Attribute</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Many To Many Relationship Attribute</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.ManyToManyRelationshipAttribute
	 * @generated
	 */
	EClass getManyToManyRelationshipAttribute();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.Classifier <em>Classifier</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Classifier</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.Classifier
	 * @generated
	 */
	EClass getClassifier();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.Element <em>Element</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Element</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.Element
	 * @generated
	 */
	EClass getElement();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.NamedElement <em>Named Element</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Named Element</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.NamedElement
	 * @generated
	 */
	EClass getNamedElement();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.NamedElement#getName <em>Name</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Name</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.NamedElement#getName()
	 * @see #getNamedElement()
	 * @generated
	 */
	EAttribute getNamedElement_Name();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.EntityModule <em>Entity Module</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Entity Module</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.EntityModule
	 * @generated
	 */
	EClass getEntityModule();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.EntityModule#getNsURI <em>Ns URI</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Ns URI</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.EntityModule#getNsURI()
	 * @see #getEntityModule()
	 * @generated
	 */
	EAttribute getEntityModule_NsURI();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.EntityModule#getNsPrefix <em>Ns Prefix</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Ns Prefix</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.EntityModule#getNsPrefix()
	 * @see #getEntityModule()
	 * @generated
	 */
	EAttribute getEntityModule_NsPrefix();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.EntityModule#getEntities <em>Entities</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Entities</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.EntityModule#getEntities()
	 * @see #getEntityModule()
	 * @generated
	 */
	EReference getEntityModule_Entities();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.StructuralFeature <em>Structural Feature</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Structural Feature</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.StructuralFeature
	 * @generated
	 */
	EClass getStructuralFeature();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.TypedElement <em>Typed Element</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Typed Element</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.TypedElement
	 * @generated
	 */
	EClass getTypedElement();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.TypedElement#getClassifier <em>Classifier</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Classifier</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.TypedElement#getClassifier()
	 * @see #getTypedElement()
	 * @generated
	 */
	EReference getTypedElement_Classifier();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.GeneratedEntity <em>Generated Entity</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Generated Entity</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.GeneratedEntity
	 * @generated
	 */
	EClass getGeneratedEntity();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.GeneratedEntity#getAttributes <em>Attributes</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Attributes</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.GeneratedEntity#getAttributes()
	 * @see #getGeneratedEntity()
	 * @generated
	 */
	EReference getGeneratedEntity_Attributes();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.Entity <em>Entity</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Entity</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.Entity
	 * @generated
	 */
	EClass getEntity();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.DerivedEntity <em>Derived Entity</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Derived Entity</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.DerivedEntity
	 * @generated
	 */
	EClass getDerivedEntity();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.DerivedEntity#getAttributes <em>Attributes</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Attributes</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.DerivedEntity#getAttributes()
	 * @see #getDerivedEntity()
	 * @generated
	 */
	EReference getDerivedEntity_Attributes();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.BasicEntity <em>Basic Entity</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Basic Entity</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.BasicEntity
	 * @generated
	 */
	EClass getBasicEntity();

	/**
	 * Returns the meta object for the containment reference list '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.BasicEntity#getAttributes <em>Attributes</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the containment reference list '<em>Attributes</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.BasicEntity#getAttributes()
	 * @see #getBasicEntity()
	 * @generated
	 */
	EReference getBasicEntity_Attributes();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.BasicEntity#getSuperClass <em>Super Class</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Super Class</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.BasicEntity#getSuperClass()
	 * @see #getBasicEntity()
	 * @generated
	 */
	EReference getBasicEntity_SuperClass();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.EnumMember <em>Enum Member</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Enum Member</em>'.
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.EnumMember
	 * @generated
	 */
	EClass getEnumMember();

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
		 * The meta object literal for the '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.impl.AttributeImpl <em>Attribute</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.AttributeImpl
		 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.Data_meta_modelPackageImpl#getAttribute()
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
		 * The meta object literal for the '<em><b>Concept</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference ATTRIBUTE__CONCEPT = eINSTANCE.getAttribute_Concept();

		/**
		 * The meta object literal for the '<em><b>Ordered</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute ATTRIBUTE__ORDERED = eINSTANCE.getAttribute_Ordered();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.impl.ConceptImpl <em>Concept</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.ConceptImpl
		 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.Data_meta_modelPackageImpl#getConcept()
		 * @generated
		 */
		EClass CONCEPT = eINSTANCE.getConcept();

		/**
		 * The meta object literal for the '<em><b>Concept Name</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CONCEPT__CONCEPT_NAME = eINSTANCE.getConcept_ConceptName();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.impl.RelationshipAttributeImpl <em>Relationship Attribute</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.RelationshipAttributeImpl
		 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.Data_meta_modelPackageImpl#getRelationshipAttribute()
		 * @generated
		 */
		EClass RELATIONSHIP_ATTRIBUTE = eINSTANCE.getRelationshipAttribute();

		/**
		 * The meta object literal for the '<em><b>Entity</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference RELATIONSHIP_ATTRIBUTE__ENTITY = eINSTANCE.getRelationshipAttribute_Entity();

		/**
		 * The meta object literal for the '<em><b>Containment</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute RELATIONSHIP_ATTRIBUTE__CONTAINMENT = eINSTANCE.getRelationshipAttribute_Containment();

		/**
		 * The meta object literal for the '<em><b>Mandatory</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute RELATIONSHIP_ATTRIBUTE__MANDATORY = eINSTANCE.getRelationshipAttribute_Mandatory();

		/**
		 * The meta object literal for the '<em><b>Dominant</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute RELATIONSHIP_ATTRIBUTE__DOMINANT = eINSTANCE.getRelationshipAttribute_Dominant();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.impl.OneToOneRelationshipAttributeImpl <em>One To One Relationship Attribute</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.OneToOneRelationshipAttributeImpl
		 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.Data_meta_modelPackageImpl#getOneToOneRelationshipAttribute()
		 * @generated
		 */
		EClass ONE_TO_ONE_RELATIONSHIP_ATTRIBUTE = eINSTANCE.getOneToOneRelationshipAttribute();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.impl.ManyToOneRelationshipAttributeImpl <em>Many To One Relationship Attribute</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.ManyToOneRelationshipAttributeImpl
		 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.Data_meta_modelPackageImpl#getManyToOneRelationshipAttribute()
		 * @generated
		 */
		EClass MANY_TO_ONE_RELATIONSHIP_ATTRIBUTE = eINSTANCE.getManyToOneRelationshipAttribute();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.impl.OneToManyRelationshipAttributeImpl <em>One To Many Relationship Attribute</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.OneToManyRelationshipAttributeImpl
		 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.Data_meta_modelPackageImpl#getOneToManyRelationshipAttribute()
		 * @generated
		 */
		EClass ONE_TO_MANY_RELATIONSHIP_ATTRIBUTE = eINSTANCE.getOneToManyRelationshipAttribute();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.impl.ManyToManyRelationshipAttributeImpl <em>Many To Many Relationship Attribute</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.ManyToManyRelationshipAttributeImpl
		 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.Data_meta_modelPackageImpl#getManyToManyRelationshipAttribute()
		 * @generated
		 */
		EClass MANY_TO_MANY_RELATIONSHIP_ATTRIBUTE = eINSTANCE.getManyToManyRelationshipAttribute();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.impl.ClassifierImpl <em>Classifier</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.ClassifierImpl
		 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.Data_meta_modelPackageImpl#getClassifier()
		 * @generated
		 */
		EClass CLASSIFIER = eINSTANCE.getClassifier();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.impl.ElementImpl <em>Element</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.ElementImpl
		 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.Data_meta_modelPackageImpl#getElement()
		 * @generated
		 */
		EClass ELEMENT = eINSTANCE.getElement();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.impl.NamedElementImpl <em>Named Element</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.NamedElementImpl
		 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.Data_meta_modelPackageImpl#getNamedElement()
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
		 * The meta object literal for the '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.impl.EntityModuleImpl <em>Entity Module</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.EntityModuleImpl
		 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.Data_meta_modelPackageImpl#getEntityModule()
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
		 * The meta object literal for the '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.impl.StructuralFeatureImpl <em>Structural Feature</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.StructuralFeatureImpl
		 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.Data_meta_modelPackageImpl#getStructuralFeature()
		 * @generated
		 */
		EClass STRUCTURAL_FEATURE = eINSTANCE.getStructuralFeature();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.impl.TypedElementImpl <em>Typed Element</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.TypedElementImpl
		 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.Data_meta_modelPackageImpl#getTypedElement()
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
		 * The meta object literal for the '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.impl.GeneratedEntityImpl <em>Generated Entity</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.GeneratedEntityImpl
		 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.Data_meta_modelPackageImpl#getGeneratedEntity()
		 * @generated
		 */
		EClass GENERATED_ENTITY = eINSTANCE.getGeneratedEntity();

		/**
		 * The meta object literal for the '<em><b>Attributes</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference GENERATED_ENTITY__ATTRIBUTES = eINSTANCE.getGeneratedEntity_Attributes();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.impl.EntityImpl <em>Entity</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.EntityImpl
		 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.Data_meta_modelPackageImpl#getEntity()
		 * @generated
		 */
		EClass ENTITY = eINSTANCE.getEntity();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.impl.DerivedEntityImpl <em>Derived Entity</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.DerivedEntityImpl
		 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.Data_meta_modelPackageImpl#getDerivedEntity()
		 * @generated
		 */
		EClass DERIVED_ENTITY = eINSTANCE.getDerivedEntity();

		/**
		 * The meta object literal for the '<em><b>Attributes</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference DERIVED_ENTITY__ATTRIBUTES = eINSTANCE.getDerivedEntity_Attributes();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.impl.BasicEntityImpl <em>Basic Entity</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.BasicEntityImpl
		 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.Data_meta_modelPackageImpl#getBasicEntity()
		 * @generated
		 */
		EClass BASIC_ENTITY = eINSTANCE.getBasicEntity();

		/**
		 * The meta object literal for the '<em><b>Attributes</b></em>' containment reference list feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BASIC_ENTITY__ATTRIBUTES = eINSTANCE.getBasicEntity_Attributes();

		/**
		 * The meta object literal for the '<em><b>Super Class</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference BASIC_ENTITY__SUPER_CLASS = eINSTANCE.getBasicEntity_SuperClass();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.impl.EnumMemberImpl <em>Enum Member</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.EnumMemberImpl
		 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.impl.Data_meta_modelPackageImpl#getEnumMember()
		 * @generated
		 */
		EClass ENUM_MEMBER = eINSTANCE.getEnumMember();

	}

} //Data_meta_modelPackage