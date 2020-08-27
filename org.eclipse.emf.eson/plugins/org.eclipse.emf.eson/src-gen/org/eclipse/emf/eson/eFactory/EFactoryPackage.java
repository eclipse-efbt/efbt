/**
 */
package org.eclipse.emf.eson.eFactory;

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
 *   <li>each enum,</li>
 *   <li>and each data type</li>
 * </ul>
 * <!-- end-user-doc -->
 * @see org.eclipse.emf.eson.eFactory.EFactoryFactory
 * @model kind="package"
 * @generated
 */
public interface EFactoryPackage extends EPackage
{
  /**
   * The package name.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  String eNAME = "eFactory";

  /**
   * The package namespace URI.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  String eNS_URI = "http://www.eclipse.org/emf/eson/EFactory";

  /**
   * The package namespace name.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  String eNS_PREFIX = "eFactory";

  /**
   * The singleton instance of the package.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  EFactoryPackage eINSTANCE = org.eclipse.emf.eson.eFactory.impl.EFactoryPackageImpl.init();

  /**
   * The meta object id for the '{@link org.eclipse.emf.eson.eFactory.impl.FactoryImpl <em>Factory</em>}' class.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @see org.eclipse.emf.eson.eFactory.impl.FactoryImpl
   * @see org.eclipse.emf.eson.eFactory.impl.EFactoryPackageImpl#getFactory()
   * @generated
   */
  int FACTORY = 0;

  /**
   * The feature id for the '<em><b>Imports</b></em>' containment reference list.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   * @ordered
   */
  int FACTORY__IMPORTS = 0;

  /**
   * The feature id for the '<em><b>EPackages</b></em>' containment reference list.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   * @ordered
   */
  int FACTORY__EPACKAGES = 1;

  /**
   * The feature id for the '<em><b>Annotations</b></em>' containment reference list.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   * @ordered
   */
  int FACTORY__ANNOTATIONS = 2;

  /**
   * The feature id for the '<em><b>Root</b></em>' containment reference.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   * @ordered
   */
  int FACTORY__ROOT = 3;

  /**
   * The number of structural features of the '<em>Factory</em>' class.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   * @ordered
   */
  int FACTORY_FEATURE_COUNT = 4;

  /**
   * The meta object id for the '{@link org.eclipse.emf.eson.eFactory.impl.PackageImportImpl <em>Package Import</em>}' class.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @see org.eclipse.emf.eson.eFactory.impl.PackageImportImpl
   * @see org.eclipse.emf.eson.eFactory.impl.EFactoryPackageImpl#getPackageImport()
   * @generated
   */
  int PACKAGE_IMPORT = 1;

  /**
   * The feature id for the '<em><b>EPackage</b></em>' reference.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   * @ordered
   */
  int PACKAGE_IMPORT__EPACKAGE = 0;

  /**
   * The number of structural features of the '<em>Package Import</em>' class.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   * @ordered
   */
  int PACKAGE_IMPORT_FEATURE_COUNT = 1;

  /**
   * The meta object id for the '{@link org.eclipse.emf.eson.eFactory.impl.NamespaceImportImpl <em>Namespace Import</em>}' class.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @see org.eclipse.emf.eson.eFactory.impl.NamespaceImportImpl
   * @see org.eclipse.emf.eson.eFactory.impl.EFactoryPackageImpl#getNamespaceImport()
   * @generated
   */
  int NAMESPACE_IMPORT = 2;

  /**
   * The feature id for the '<em><b>Imported Namespace</b></em>' attribute.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   * @ordered
   */
  int NAMESPACE_IMPORT__IMPORTED_NAMESPACE = 0;

  /**
   * The number of structural features of the '<em>Namespace Import</em>' class.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   * @ordered
   */
  int NAMESPACE_IMPORT_FEATURE_COUNT = 1;

  /**
   * The meta object id for the '{@link org.eclipse.emf.eson.eFactory.impl.AnnotationImpl <em>Annotation</em>}' class.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @see org.eclipse.emf.eson.eFactory.impl.AnnotationImpl
   * @see org.eclipse.emf.eson.eFactory.impl.EFactoryPackageImpl#getAnnotation()
   * @generated
   */
  int ANNOTATION = 3;

  /**
   * The number of structural features of the '<em>Annotation</em>' class.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   * @ordered
   */
  int ANNOTATION_FEATURE_COUNT = 0;

  /**
   * The meta object id for the '{@link org.eclipse.emf.eson.eFactory.impl.CustomNameMappingImpl <em>Custom Name Mapping</em>}' class.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @see org.eclipse.emf.eson.eFactory.impl.CustomNameMappingImpl
   * @see org.eclipse.emf.eson.eFactory.impl.EFactoryPackageImpl#getCustomNameMapping()
   * @generated
   */
  int CUSTOM_NAME_MAPPING = 4;

  /**
   * The feature id for the '<em><b>EClass</b></em>' reference.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   * @ordered
   */
  int CUSTOM_NAME_MAPPING__ECLASS = ANNOTATION_FEATURE_COUNT + 0;

  /**
   * The feature id for the '<em><b>Name Feature</b></em>' reference.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   * @ordered
   */
  int CUSTOM_NAME_MAPPING__NAME_FEATURE = ANNOTATION_FEATURE_COUNT + 1;

  /**
   * The number of structural features of the '<em>Custom Name Mapping</em>' class.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   * @ordered
   */
  int CUSTOM_NAME_MAPPING_FEATURE_COUNT = ANNOTATION_FEATURE_COUNT + 2;

  /**
   * The meta object id for the '{@link org.eclipse.emf.eson.eFactory.impl.FeatureImpl <em>Feature</em>}' class.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @see org.eclipse.emf.eson.eFactory.impl.FeatureImpl
   * @see org.eclipse.emf.eson.eFactory.impl.EFactoryPackageImpl#getFeature()
   * @generated
   */
  int FEATURE = 5;

  /**
   * The feature id for the '<em><b>EFeature</b></em>' reference.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   * @ordered
   */
  int FEATURE__EFEATURE = 0;

  /**
   * The feature id for the '<em><b>Value</b></em>' containment reference.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   * @ordered
   */
  int FEATURE__VALUE = 1;

  /**
   * The number of structural features of the '<em>Feature</em>' class.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   * @ordered
   */
  int FEATURE_FEATURE_COUNT = 2;

  /**
   * The meta object id for the '{@link org.eclipse.emf.eson.eFactory.impl.ValueImpl <em>Value</em>}' class.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @see org.eclipse.emf.eson.eFactory.impl.ValueImpl
   * @see org.eclipse.emf.eson.eFactory.impl.EFactoryPackageImpl#getValue()
   * @generated
   */
  int VALUE = 7;

  /**
   * The number of structural features of the '<em>Value</em>' class.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   * @ordered
   */
  int VALUE_FEATURE_COUNT = 0;

  /**
   * The meta object id for the '{@link org.eclipse.emf.eson.eFactory.impl.NewObjectImpl <em>New Object</em>}' class.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @see org.eclipse.emf.eson.eFactory.impl.NewObjectImpl
   * @see org.eclipse.emf.eson.eFactory.impl.EFactoryPackageImpl#getNewObject()
   * @generated
   */
  int NEW_OBJECT = 6;

  /**
   * The feature id for the '<em><b>EClass</b></em>' reference.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   * @ordered
   */
  int NEW_OBJECT__ECLASS = VALUE_FEATURE_COUNT + 0;

  /**
   * The feature id for the '<em><b>Name</b></em>' attribute.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   * @ordered
   */
  int NEW_OBJECT__NAME = VALUE_FEATURE_COUNT + 1;

  /**
   * The feature id for the '<em><b>Features</b></em>' containment reference list.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   * @ordered
   */
  int NEW_OBJECT__FEATURES = VALUE_FEATURE_COUNT + 2;

  /**
   * The number of structural features of the '<em>New Object</em>' class.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   * @ordered
   */
  int NEW_OBJECT_FEATURE_COUNT = VALUE_FEATURE_COUNT + 3;

  /**
   * The meta object id for the '{@link org.eclipse.emf.eson.eFactory.impl.MultiValueImpl <em>Multi Value</em>}' class.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @see org.eclipse.emf.eson.eFactory.impl.MultiValueImpl
   * @see org.eclipse.emf.eson.eFactory.impl.EFactoryPackageImpl#getMultiValue()
   * @generated
   */
  int MULTI_VALUE = 8;

  /**
   * The feature id for the '<em><b>Values</b></em>' containment reference list.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   * @ordered
   */
  int MULTI_VALUE__VALUES = VALUE_FEATURE_COUNT + 0;

  /**
   * The number of structural features of the '<em>Multi Value</em>' class.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   * @ordered
   */
  int MULTI_VALUE_FEATURE_COUNT = VALUE_FEATURE_COUNT + 1;

  /**
   * The meta object id for the '{@link org.eclipse.emf.eson.eFactory.impl.ReferenceImpl <em>Reference</em>}' class.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @see org.eclipse.emf.eson.eFactory.impl.ReferenceImpl
   * @see org.eclipse.emf.eson.eFactory.impl.EFactoryPackageImpl#getReference()
   * @generated
   */
  int REFERENCE = 9;

  /**
   * The feature id for the '<em><b>Value</b></em>' reference.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   * @ordered
   */
  int REFERENCE__VALUE = VALUE_FEATURE_COUNT + 0;

  /**
   * The number of structural features of the '<em>Reference</em>' class.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   * @ordered
   */
  int REFERENCE_FEATURE_COUNT = VALUE_FEATURE_COUNT + 1;

  /**
   * The meta object id for the '{@link org.eclipse.emf.eson.eFactory.impl.AttributeImpl <em>Attribute</em>}' class.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @see org.eclipse.emf.eson.eFactory.impl.AttributeImpl
   * @see org.eclipse.emf.eson.eFactory.impl.EFactoryPackageImpl#getAttribute()
   * @generated
   */
  int ATTRIBUTE = 10;

  /**
   * The number of structural features of the '<em>Attribute</em>' class.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   * @ordered
   */
  int ATTRIBUTE_FEATURE_COUNT = VALUE_FEATURE_COUNT + 0;

  /**
   * The meta object id for the '{@link org.eclipse.emf.eson.eFactory.impl.EnumAttributeImpl <em>Enum Attribute</em>}' class.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @see org.eclipse.emf.eson.eFactory.impl.EnumAttributeImpl
   * @see org.eclipse.emf.eson.eFactory.impl.EFactoryPackageImpl#getEnumAttribute()
   * @generated
   */
  int ENUM_ATTRIBUTE = 11;

  /**
   * The feature id for the '<em><b>Value</b></em>' reference.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   * @ordered
   */
  int ENUM_ATTRIBUTE__VALUE = ATTRIBUTE_FEATURE_COUNT + 0;

  /**
   * The number of structural features of the '<em>Enum Attribute</em>' class.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   * @ordered
   */
  int ENUM_ATTRIBUTE_FEATURE_COUNT = ATTRIBUTE_FEATURE_COUNT + 1;

  /**
   * The meta object id for the '{@link org.eclipse.emf.eson.eFactory.impl.StringAttributeImpl <em>String Attribute</em>}' class.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @see org.eclipse.emf.eson.eFactory.impl.StringAttributeImpl
   * @see org.eclipse.emf.eson.eFactory.impl.EFactoryPackageImpl#getStringAttribute()
   * @generated
   */
  int STRING_ATTRIBUTE = 12;

  /**
   * The feature id for the '<em><b>Value</b></em>' attribute.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   * @ordered
   */
  int STRING_ATTRIBUTE__VALUE = ATTRIBUTE_FEATURE_COUNT + 0;

  /**
   * The number of structural features of the '<em>String Attribute</em>' class.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   * @ordered
   */
  int STRING_ATTRIBUTE_FEATURE_COUNT = ATTRIBUTE_FEATURE_COUNT + 1;

  /**
   * The meta object id for the '{@link org.eclipse.emf.eson.eFactory.impl.IntegerAttributeImpl <em>Integer Attribute</em>}' class.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @see org.eclipse.emf.eson.eFactory.impl.IntegerAttributeImpl
   * @see org.eclipse.emf.eson.eFactory.impl.EFactoryPackageImpl#getIntegerAttribute()
   * @generated
   */
  int INTEGER_ATTRIBUTE = 13;

  /**
   * The feature id for the '<em><b>Value</b></em>' attribute.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   * @ordered
   */
  int INTEGER_ATTRIBUTE__VALUE = ATTRIBUTE_FEATURE_COUNT + 0;

  /**
   * The number of structural features of the '<em>Integer Attribute</em>' class.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   * @ordered
   */
  int INTEGER_ATTRIBUTE_FEATURE_COUNT = ATTRIBUTE_FEATURE_COUNT + 1;

  /**
   * The meta object id for the '{@link org.eclipse.emf.eson.eFactory.impl.DoubleAttributeImpl <em>Double Attribute</em>}' class.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @see org.eclipse.emf.eson.eFactory.impl.DoubleAttributeImpl
   * @see org.eclipse.emf.eson.eFactory.impl.EFactoryPackageImpl#getDoubleAttribute()
   * @generated
   */
  int DOUBLE_ATTRIBUTE = 14;

  /**
   * The feature id for the '<em><b>Value</b></em>' attribute.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   * @ordered
   */
  int DOUBLE_ATTRIBUTE__VALUE = ATTRIBUTE_FEATURE_COUNT + 0;

  /**
   * The number of structural features of the '<em>Double Attribute</em>' class.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   * @ordered
   */
  int DOUBLE_ATTRIBUTE_FEATURE_COUNT = ATTRIBUTE_FEATURE_COUNT + 1;

  /**
   * The meta object id for the '{@link org.eclipse.emf.eson.eFactory.impl.DateAttributeImpl <em>Date Attribute</em>}' class.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @see org.eclipse.emf.eson.eFactory.impl.DateAttributeImpl
   * @see org.eclipse.emf.eson.eFactory.impl.EFactoryPackageImpl#getDateAttribute()
   * @generated
   */
  int DATE_ATTRIBUTE = 15;

  /**
   * The feature id for the '<em><b>Value</b></em>' attribute.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   * @ordered
   */
  int DATE_ATTRIBUTE__VALUE = ATTRIBUTE_FEATURE_COUNT + 0;

  /**
   * The number of structural features of the '<em>Date Attribute</em>' class.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   * @ordered
   */
  int DATE_ATTRIBUTE_FEATURE_COUNT = ATTRIBUTE_FEATURE_COUNT + 1;

  /**
   * The meta object id for the '{@link org.eclipse.emf.eson.eFactory.impl.NullAttributeImpl <em>Null Attribute</em>}' class.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @see org.eclipse.emf.eson.eFactory.impl.NullAttributeImpl
   * @see org.eclipse.emf.eson.eFactory.impl.EFactoryPackageImpl#getNullAttribute()
   * @generated
   */
  int NULL_ATTRIBUTE = 16;

  /**
   * The feature id for the '<em><b>Value</b></em>' attribute.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   * @ordered
   */
  int NULL_ATTRIBUTE__VALUE = ATTRIBUTE_FEATURE_COUNT + 0;

  /**
   * The number of structural features of the '<em>Null Attribute</em>' class.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   * @ordered
   */
  int NULL_ATTRIBUTE_FEATURE_COUNT = ATTRIBUTE_FEATURE_COUNT + 1;

  /**
   * The meta object id for the '{@link org.eclipse.emf.eson.eFactory.impl.BooleanAttributeImpl <em>Boolean Attribute</em>}' class.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @see org.eclipse.emf.eson.eFactory.impl.BooleanAttributeImpl
   * @see org.eclipse.emf.eson.eFactory.impl.EFactoryPackageImpl#getBooleanAttribute()
   * @generated
   */
  int BOOLEAN_ATTRIBUTE = 17;

  /**
   * The feature id for the '<em><b>Value</b></em>' attribute.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   * @ordered
   */
  int BOOLEAN_ATTRIBUTE__VALUE = ATTRIBUTE_FEATURE_COUNT + 0;

  /**
   * The number of structural features of the '<em>Boolean Attribute</em>' class.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   * @ordered
   */
  int BOOLEAN_ATTRIBUTE_FEATURE_COUNT = ATTRIBUTE_FEATURE_COUNT + 1;

  /**
   * The meta object id for the '{@link org.eclipse.emf.eson.eFactory.impl.ContainmentImpl <em>Containment</em>}' class.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @see org.eclipse.emf.eson.eFactory.impl.ContainmentImpl
   * @see org.eclipse.emf.eson.eFactory.impl.EFactoryPackageImpl#getContainment()
   * @generated
   */
  int CONTAINMENT = 18;

  /**
   * The feature id for the '<em><b>Value</b></em>' containment reference.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   * @ordered
   */
  int CONTAINMENT__VALUE = VALUE_FEATURE_COUNT + 0;

  /**
   * The number of structural features of the '<em>Containment</em>' class.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   * @ordered
   */
  int CONTAINMENT_FEATURE_COUNT = VALUE_FEATURE_COUNT + 1;


  /**
   * Returns the meta object for class '{@link org.eclipse.emf.eson.eFactory.Factory <em>Factory</em>}'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return the meta object for class '<em>Factory</em>'.
   * @see org.eclipse.emf.eson.eFactory.Factory
   * @generated
   */
  EClass getFactory();

  /**
   * Returns the meta object for the containment reference list '{@link org.eclipse.emf.eson.eFactory.Factory#getImports <em>Imports</em>}'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return the meta object for the containment reference list '<em>Imports</em>'.
   * @see org.eclipse.emf.eson.eFactory.Factory#getImports()
   * @see #getFactory()
   * @generated
   */
  EReference getFactory_Imports();

  /**
   * Returns the meta object for the containment reference list '{@link org.eclipse.emf.eson.eFactory.Factory#getEPackages <em>EPackages</em>}'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return the meta object for the containment reference list '<em>EPackages</em>'.
   * @see org.eclipse.emf.eson.eFactory.Factory#getEPackages()
   * @see #getFactory()
   * @generated
   */
  EReference getFactory_EPackages();

  /**
   * Returns the meta object for the containment reference list '{@link org.eclipse.emf.eson.eFactory.Factory#getAnnotations <em>Annotations</em>}'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return the meta object for the containment reference list '<em>Annotations</em>'.
   * @see org.eclipse.emf.eson.eFactory.Factory#getAnnotations()
   * @see #getFactory()
   * @generated
   */
  EReference getFactory_Annotations();

  /**
   * Returns the meta object for the containment reference '{@link org.eclipse.emf.eson.eFactory.Factory#getRoot <em>Root</em>}'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return the meta object for the containment reference '<em>Root</em>'.
   * @see org.eclipse.emf.eson.eFactory.Factory#getRoot()
   * @see #getFactory()
   * @generated
   */
  EReference getFactory_Root();

  /**
   * Returns the meta object for class '{@link org.eclipse.emf.eson.eFactory.PackageImport <em>Package Import</em>}'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return the meta object for class '<em>Package Import</em>'.
   * @see org.eclipse.emf.eson.eFactory.PackageImport
   * @generated
   */
  EClass getPackageImport();

  /**
   * Returns the meta object for the reference '{@link org.eclipse.emf.eson.eFactory.PackageImport#getEPackage <em>EPackage</em>}'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return the meta object for the reference '<em>EPackage</em>'.
   * @see org.eclipse.emf.eson.eFactory.PackageImport#getEPackage()
   * @see #getPackageImport()
   * @generated
   */
  EReference getPackageImport_EPackage();

  /**
   * Returns the meta object for class '{@link org.eclipse.emf.eson.eFactory.NamespaceImport <em>Namespace Import</em>}'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return the meta object for class '<em>Namespace Import</em>'.
   * @see org.eclipse.emf.eson.eFactory.NamespaceImport
   * @generated
   */
  EClass getNamespaceImport();

  /**
   * Returns the meta object for the attribute '{@link org.eclipse.emf.eson.eFactory.NamespaceImport#getImportedNamespace <em>Imported Namespace</em>}'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return the meta object for the attribute '<em>Imported Namespace</em>'.
   * @see org.eclipse.emf.eson.eFactory.NamespaceImport#getImportedNamespace()
   * @see #getNamespaceImport()
   * @generated
   */
  EAttribute getNamespaceImport_ImportedNamespace();

  /**
   * Returns the meta object for class '{@link org.eclipse.emf.eson.eFactory.Annotation <em>Annotation</em>}'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return the meta object for class '<em>Annotation</em>'.
   * @see org.eclipse.emf.eson.eFactory.Annotation
   * @generated
   */
  EClass getAnnotation();

  /**
   * Returns the meta object for class '{@link org.eclipse.emf.eson.eFactory.CustomNameMapping <em>Custom Name Mapping</em>}'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return the meta object for class '<em>Custom Name Mapping</em>'.
   * @see org.eclipse.emf.eson.eFactory.CustomNameMapping
   * @generated
   */
  EClass getCustomNameMapping();

  /**
   * Returns the meta object for the reference '{@link org.eclipse.emf.eson.eFactory.CustomNameMapping#getEClass <em>EClass</em>}'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return the meta object for the reference '<em>EClass</em>'.
   * @see org.eclipse.emf.eson.eFactory.CustomNameMapping#getEClass()
   * @see #getCustomNameMapping()
   * @generated
   */
  EReference getCustomNameMapping_EClass();

  /**
   * Returns the meta object for the reference '{@link org.eclipse.emf.eson.eFactory.CustomNameMapping#getNameFeature <em>Name Feature</em>}'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return the meta object for the reference '<em>Name Feature</em>'.
   * @see org.eclipse.emf.eson.eFactory.CustomNameMapping#getNameFeature()
   * @see #getCustomNameMapping()
   * @generated
   */
  EReference getCustomNameMapping_NameFeature();

  /**
   * Returns the meta object for class '{@link org.eclipse.emf.eson.eFactory.Feature <em>Feature</em>}'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return the meta object for class '<em>Feature</em>'.
   * @see org.eclipse.emf.eson.eFactory.Feature
   * @generated
   */
  EClass getFeature();

  /**
   * Returns the meta object for the reference '{@link org.eclipse.emf.eson.eFactory.Feature#getEFeature <em>EFeature</em>}'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return the meta object for the reference '<em>EFeature</em>'.
   * @see org.eclipse.emf.eson.eFactory.Feature#getEFeature()
   * @see #getFeature()
   * @generated
   */
  EReference getFeature_EFeature();

  /**
   * Returns the meta object for the containment reference '{@link org.eclipse.emf.eson.eFactory.Feature#getValue <em>Value</em>}'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return the meta object for the containment reference '<em>Value</em>'.
   * @see org.eclipse.emf.eson.eFactory.Feature#getValue()
   * @see #getFeature()
   * @generated
   */
  EReference getFeature_Value();

  /**
   * Returns the meta object for class '{@link org.eclipse.emf.eson.eFactory.NewObject <em>New Object</em>}'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return the meta object for class '<em>New Object</em>'.
   * @see org.eclipse.emf.eson.eFactory.NewObject
   * @generated
   */
  EClass getNewObject();

  /**
   * Returns the meta object for the reference '{@link org.eclipse.emf.eson.eFactory.NewObject#getEClass <em>EClass</em>}'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return the meta object for the reference '<em>EClass</em>'.
   * @see org.eclipse.emf.eson.eFactory.NewObject#getEClass()
   * @see #getNewObject()
   * @generated
   */
  EReference getNewObject_EClass();

  /**
   * Returns the meta object for the attribute '{@link org.eclipse.emf.eson.eFactory.NewObject#getName <em>Name</em>}'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return the meta object for the attribute '<em>Name</em>'.
   * @see org.eclipse.emf.eson.eFactory.NewObject#getName()
   * @see #getNewObject()
   * @generated
   */
  EAttribute getNewObject_Name();

  /**
   * Returns the meta object for the containment reference list '{@link org.eclipse.emf.eson.eFactory.NewObject#getFeatures <em>Features</em>}'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return the meta object for the containment reference list '<em>Features</em>'.
   * @see org.eclipse.emf.eson.eFactory.NewObject#getFeatures()
   * @see #getNewObject()
   * @generated
   */
  EReference getNewObject_Features();

  /**
   * Returns the meta object for class '{@link org.eclipse.emf.eson.eFactory.Value <em>Value</em>}'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return the meta object for class '<em>Value</em>'.
   * @see org.eclipse.emf.eson.eFactory.Value
   * @generated
   */
  EClass getValue();

  /**
   * Returns the meta object for class '{@link org.eclipse.emf.eson.eFactory.MultiValue <em>Multi Value</em>}'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return the meta object for class '<em>Multi Value</em>'.
   * @see org.eclipse.emf.eson.eFactory.MultiValue
   * @generated
   */
  EClass getMultiValue();

  /**
   * Returns the meta object for the containment reference list '{@link org.eclipse.emf.eson.eFactory.MultiValue#getValues <em>Values</em>}'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return the meta object for the containment reference list '<em>Values</em>'.
   * @see org.eclipse.emf.eson.eFactory.MultiValue#getValues()
   * @see #getMultiValue()
   * @generated
   */
  EReference getMultiValue_Values();

  /**
   * Returns the meta object for class '{@link org.eclipse.emf.eson.eFactory.Reference <em>Reference</em>}'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return the meta object for class '<em>Reference</em>'.
   * @see org.eclipse.emf.eson.eFactory.Reference
   * @generated
   */
  EClass getReference();

  /**
   * Returns the meta object for the reference '{@link org.eclipse.emf.eson.eFactory.Reference#getValue <em>Value</em>}'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return the meta object for the reference '<em>Value</em>'.
   * @see org.eclipse.emf.eson.eFactory.Reference#getValue()
   * @see #getReference()
   * @generated
   */
  EReference getReference_Value();

  /**
   * Returns the meta object for class '{@link org.eclipse.emf.eson.eFactory.Attribute <em>Attribute</em>}'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return the meta object for class '<em>Attribute</em>'.
   * @see org.eclipse.emf.eson.eFactory.Attribute
   * @generated
   */
  EClass getAttribute();

  /**
   * Returns the meta object for class '{@link org.eclipse.emf.eson.eFactory.EnumAttribute <em>Enum Attribute</em>}'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return the meta object for class '<em>Enum Attribute</em>'.
   * @see org.eclipse.emf.eson.eFactory.EnumAttribute
   * @generated
   */
  EClass getEnumAttribute();

  /**
   * Returns the meta object for the reference '{@link org.eclipse.emf.eson.eFactory.EnumAttribute#getValue <em>Value</em>}'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return the meta object for the reference '<em>Value</em>'.
   * @see org.eclipse.emf.eson.eFactory.EnumAttribute#getValue()
   * @see #getEnumAttribute()
   * @generated
   */
  EReference getEnumAttribute_Value();

  /**
   * Returns the meta object for class '{@link org.eclipse.emf.eson.eFactory.StringAttribute <em>String Attribute</em>}'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return the meta object for class '<em>String Attribute</em>'.
   * @see org.eclipse.emf.eson.eFactory.StringAttribute
   * @generated
   */
  EClass getStringAttribute();

  /**
   * Returns the meta object for the attribute '{@link org.eclipse.emf.eson.eFactory.StringAttribute#getValue <em>Value</em>}'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return the meta object for the attribute '<em>Value</em>'.
   * @see org.eclipse.emf.eson.eFactory.StringAttribute#getValue()
   * @see #getStringAttribute()
   * @generated
   */
  EAttribute getStringAttribute_Value();

  /**
   * Returns the meta object for class '{@link org.eclipse.emf.eson.eFactory.IntegerAttribute <em>Integer Attribute</em>}'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return the meta object for class '<em>Integer Attribute</em>'.
   * @see org.eclipse.emf.eson.eFactory.IntegerAttribute
   * @generated
   */
  EClass getIntegerAttribute();

  /**
   * Returns the meta object for the attribute '{@link org.eclipse.emf.eson.eFactory.IntegerAttribute#getValue <em>Value</em>}'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return the meta object for the attribute '<em>Value</em>'.
   * @see org.eclipse.emf.eson.eFactory.IntegerAttribute#getValue()
   * @see #getIntegerAttribute()
   * @generated
   */
  EAttribute getIntegerAttribute_Value();

  /**
   * Returns the meta object for class '{@link org.eclipse.emf.eson.eFactory.DoubleAttribute <em>Double Attribute</em>}'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return the meta object for class '<em>Double Attribute</em>'.
   * @see org.eclipse.emf.eson.eFactory.DoubleAttribute
   * @generated
   */
  EClass getDoubleAttribute();

  /**
   * Returns the meta object for the attribute '{@link org.eclipse.emf.eson.eFactory.DoubleAttribute#getValue <em>Value</em>}'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return the meta object for the attribute '<em>Value</em>'.
   * @see org.eclipse.emf.eson.eFactory.DoubleAttribute#getValue()
   * @see #getDoubleAttribute()
   * @generated
   */
  EAttribute getDoubleAttribute_Value();

  /**
   * Returns the meta object for class '{@link org.eclipse.emf.eson.eFactory.DateAttribute <em>Date Attribute</em>}'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return the meta object for class '<em>Date Attribute</em>'.
   * @see org.eclipse.emf.eson.eFactory.DateAttribute
   * @generated
   */
  EClass getDateAttribute();

  /**
   * Returns the meta object for the attribute '{@link org.eclipse.emf.eson.eFactory.DateAttribute#getValue <em>Value</em>}'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return the meta object for the attribute '<em>Value</em>'.
   * @see org.eclipse.emf.eson.eFactory.DateAttribute#getValue()
   * @see #getDateAttribute()
   * @generated
   */
  EAttribute getDateAttribute_Value();

  /**
   * Returns the meta object for class '{@link org.eclipse.emf.eson.eFactory.NullAttribute <em>Null Attribute</em>}'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return the meta object for class '<em>Null Attribute</em>'.
   * @see org.eclipse.emf.eson.eFactory.NullAttribute
   * @generated
   */
  EClass getNullAttribute();

  /**
   * Returns the meta object for the attribute '{@link org.eclipse.emf.eson.eFactory.NullAttribute#getValue <em>Value</em>}'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return the meta object for the attribute '<em>Value</em>'.
   * @see org.eclipse.emf.eson.eFactory.NullAttribute#getValue()
   * @see #getNullAttribute()
   * @generated
   */
  EAttribute getNullAttribute_Value();

  /**
   * Returns the meta object for class '{@link org.eclipse.emf.eson.eFactory.BooleanAttribute <em>Boolean Attribute</em>}'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return the meta object for class '<em>Boolean Attribute</em>'.
   * @see org.eclipse.emf.eson.eFactory.BooleanAttribute
   * @generated
   */
  EClass getBooleanAttribute();

  /**
   * Returns the meta object for the attribute '{@link org.eclipse.emf.eson.eFactory.BooleanAttribute#isValue <em>Value</em>}'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return the meta object for the attribute '<em>Value</em>'.
   * @see org.eclipse.emf.eson.eFactory.BooleanAttribute#isValue()
   * @see #getBooleanAttribute()
   * @generated
   */
  EAttribute getBooleanAttribute_Value();

  /**
   * Returns the meta object for class '{@link org.eclipse.emf.eson.eFactory.Containment <em>Containment</em>}'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return the meta object for class '<em>Containment</em>'.
   * @see org.eclipse.emf.eson.eFactory.Containment
   * @generated
   */
  EClass getContainment();

  /**
   * Returns the meta object for the containment reference '{@link org.eclipse.emf.eson.eFactory.Containment#getValue <em>Value</em>}'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return the meta object for the containment reference '<em>Value</em>'.
   * @see org.eclipse.emf.eson.eFactory.Containment#getValue()
   * @see #getContainment()
   * @generated
   */
  EReference getContainment_Value();

  /**
   * Returns the factory that creates the instances of the model.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return the factory that creates the instances of the model.
   * @generated
   */
  EFactoryFactory getEFactoryFactory();

  /**
   * <!-- begin-user-doc -->
   * Defines literals for the meta objects that represent
   * <ul>
   *   <li>each class,</li>
   *   <li>each feature of each class,</li>
   *   <li>each enum,</li>
   *   <li>and each data type</li>
   * </ul>
   * <!-- end-user-doc -->
   * @generated
   */
  interface Literals
  {
    /**
     * The meta object literal for the '{@link org.eclipse.emf.eson.eFactory.impl.FactoryImpl <em>Factory</em>}' class.
     * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
     * @see org.eclipse.emf.eson.eFactory.impl.FactoryImpl
     * @see org.eclipse.emf.eson.eFactory.impl.EFactoryPackageImpl#getFactory()
     * @generated
     */
    EClass FACTORY = eINSTANCE.getFactory();

    /**
     * The meta object literal for the '<em><b>Imports</b></em>' containment reference list feature.
     * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
     * @generated
     */
    EReference FACTORY__IMPORTS = eINSTANCE.getFactory_Imports();

    /**
     * The meta object literal for the '<em><b>EPackages</b></em>' containment reference list feature.
     * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
     * @generated
     */
    EReference FACTORY__EPACKAGES = eINSTANCE.getFactory_EPackages();

    /**
     * The meta object literal for the '<em><b>Annotations</b></em>' containment reference list feature.
     * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
     * @generated
     */
    EReference FACTORY__ANNOTATIONS = eINSTANCE.getFactory_Annotations();

    /**
     * The meta object literal for the '<em><b>Root</b></em>' containment reference feature.
     * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
     * @generated
     */
    EReference FACTORY__ROOT = eINSTANCE.getFactory_Root();

    /**
     * The meta object literal for the '{@link org.eclipse.emf.eson.eFactory.impl.PackageImportImpl <em>Package Import</em>}' class.
     * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
     * @see org.eclipse.emf.eson.eFactory.impl.PackageImportImpl
     * @see org.eclipse.emf.eson.eFactory.impl.EFactoryPackageImpl#getPackageImport()
     * @generated
     */
    EClass PACKAGE_IMPORT = eINSTANCE.getPackageImport();

    /**
     * The meta object literal for the '<em><b>EPackage</b></em>' reference feature.
     * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
     * @generated
     */
    EReference PACKAGE_IMPORT__EPACKAGE = eINSTANCE.getPackageImport_EPackage();

    /**
     * The meta object literal for the '{@link org.eclipse.emf.eson.eFactory.impl.NamespaceImportImpl <em>Namespace Import</em>}' class.
     * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
     * @see org.eclipse.emf.eson.eFactory.impl.NamespaceImportImpl
     * @see org.eclipse.emf.eson.eFactory.impl.EFactoryPackageImpl#getNamespaceImport()
     * @generated
     */
    EClass NAMESPACE_IMPORT = eINSTANCE.getNamespaceImport();

    /**
     * The meta object literal for the '<em><b>Imported Namespace</b></em>' attribute feature.
     * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
     * @generated
     */
    EAttribute NAMESPACE_IMPORT__IMPORTED_NAMESPACE = eINSTANCE.getNamespaceImport_ImportedNamespace();

    /**
     * The meta object literal for the '{@link org.eclipse.emf.eson.eFactory.impl.AnnotationImpl <em>Annotation</em>}' class.
     * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
     * @see org.eclipse.emf.eson.eFactory.impl.AnnotationImpl
     * @see org.eclipse.emf.eson.eFactory.impl.EFactoryPackageImpl#getAnnotation()
     * @generated
     */
    EClass ANNOTATION = eINSTANCE.getAnnotation();

    /**
     * The meta object literal for the '{@link org.eclipse.emf.eson.eFactory.impl.CustomNameMappingImpl <em>Custom Name Mapping</em>}' class.
     * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
     * @see org.eclipse.emf.eson.eFactory.impl.CustomNameMappingImpl
     * @see org.eclipse.emf.eson.eFactory.impl.EFactoryPackageImpl#getCustomNameMapping()
     * @generated
     */
    EClass CUSTOM_NAME_MAPPING = eINSTANCE.getCustomNameMapping();

    /**
     * The meta object literal for the '<em><b>EClass</b></em>' reference feature.
     * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
     * @generated
     */
    EReference CUSTOM_NAME_MAPPING__ECLASS = eINSTANCE.getCustomNameMapping_EClass();

    /**
     * The meta object literal for the '<em><b>Name Feature</b></em>' reference feature.
     * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
     * @generated
     */
    EReference CUSTOM_NAME_MAPPING__NAME_FEATURE = eINSTANCE.getCustomNameMapping_NameFeature();

    /**
     * The meta object literal for the '{@link org.eclipse.emf.eson.eFactory.impl.FeatureImpl <em>Feature</em>}' class.
     * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
     * @see org.eclipse.emf.eson.eFactory.impl.FeatureImpl
     * @see org.eclipse.emf.eson.eFactory.impl.EFactoryPackageImpl#getFeature()
     * @generated
     */
    EClass FEATURE = eINSTANCE.getFeature();

    /**
     * The meta object literal for the '<em><b>EFeature</b></em>' reference feature.
     * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
     * @generated
     */
    EReference FEATURE__EFEATURE = eINSTANCE.getFeature_EFeature();

    /**
     * The meta object literal for the '<em><b>Value</b></em>' containment reference feature.
     * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
     * @generated
     */
    EReference FEATURE__VALUE = eINSTANCE.getFeature_Value();

    /**
     * The meta object literal for the '{@link org.eclipse.emf.eson.eFactory.impl.NewObjectImpl <em>New Object</em>}' class.
     * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
     * @see org.eclipse.emf.eson.eFactory.impl.NewObjectImpl
     * @see org.eclipse.emf.eson.eFactory.impl.EFactoryPackageImpl#getNewObject()
     * @generated
     */
    EClass NEW_OBJECT = eINSTANCE.getNewObject();

    /**
     * The meta object literal for the '<em><b>EClass</b></em>' reference feature.
     * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
     * @generated
     */
    EReference NEW_OBJECT__ECLASS = eINSTANCE.getNewObject_EClass();

    /**
     * The meta object literal for the '<em><b>Name</b></em>' attribute feature.
     * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
     * @generated
     */
    EAttribute NEW_OBJECT__NAME = eINSTANCE.getNewObject_Name();

    /**
     * The meta object literal for the '<em><b>Features</b></em>' containment reference list feature.
     * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
     * @generated
     */
    EReference NEW_OBJECT__FEATURES = eINSTANCE.getNewObject_Features();

    /**
     * The meta object literal for the '{@link org.eclipse.emf.eson.eFactory.impl.ValueImpl <em>Value</em>}' class.
     * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
     * @see org.eclipse.emf.eson.eFactory.impl.ValueImpl
     * @see org.eclipse.emf.eson.eFactory.impl.EFactoryPackageImpl#getValue()
     * @generated
     */
    EClass VALUE = eINSTANCE.getValue();

    /**
     * The meta object literal for the '{@link org.eclipse.emf.eson.eFactory.impl.MultiValueImpl <em>Multi Value</em>}' class.
     * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
     * @see org.eclipse.emf.eson.eFactory.impl.MultiValueImpl
     * @see org.eclipse.emf.eson.eFactory.impl.EFactoryPackageImpl#getMultiValue()
     * @generated
     */
    EClass MULTI_VALUE = eINSTANCE.getMultiValue();

    /**
     * The meta object literal for the '<em><b>Values</b></em>' containment reference list feature.
     * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
     * @generated
     */
    EReference MULTI_VALUE__VALUES = eINSTANCE.getMultiValue_Values();

    /**
     * The meta object literal for the '{@link org.eclipse.emf.eson.eFactory.impl.ReferenceImpl <em>Reference</em>}' class.
     * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
     * @see org.eclipse.emf.eson.eFactory.impl.ReferenceImpl
     * @see org.eclipse.emf.eson.eFactory.impl.EFactoryPackageImpl#getReference()
     * @generated
     */
    EClass REFERENCE = eINSTANCE.getReference();

    /**
     * The meta object literal for the '<em><b>Value</b></em>' reference feature.
     * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
     * @generated
     */
    EReference REFERENCE__VALUE = eINSTANCE.getReference_Value();

    /**
     * The meta object literal for the '{@link org.eclipse.emf.eson.eFactory.impl.AttributeImpl <em>Attribute</em>}' class.
     * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
     * @see org.eclipse.emf.eson.eFactory.impl.AttributeImpl
     * @see org.eclipse.emf.eson.eFactory.impl.EFactoryPackageImpl#getAttribute()
     * @generated
     */
    EClass ATTRIBUTE = eINSTANCE.getAttribute();

    /**
     * The meta object literal for the '{@link org.eclipse.emf.eson.eFactory.impl.EnumAttributeImpl <em>Enum Attribute</em>}' class.
     * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
     * @see org.eclipse.emf.eson.eFactory.impl.EnumAttributeImpl
     * @see org.eclipse.emf.eson.eFactory.impl.EFactoryPackageImpl#getEnumAttribute()
     * @generated
     */
    EClass ENUM_ATTRIBUTE = eINSTANCE.getEnumAttribute();

    /**
     * The meta object literal for the '<em><b>Value</b></em>' reference feature.
     * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
     * @generated
     */
    EReference ENUM_ATTRIBUTE__VALUE = eINSTANCE.getEnumAttribute_Value();

    /**
     * The meta object literal for the '{@link org.eclipse.emf.eson.eFactory.impl.StringAttributeImpl <em>String Attribute</em>}' class.
     * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
     * @see org.eclipse.emf.eson.eFactory.impl.StringAttributeImpl
     * @see org.eclipse.emf.eson.eFactory.impl.EFactoryPackageImpl#getStringAttribute()
     * @generated
     */
    EClass STRING_ATTRIBUTE = eINSTANCE.getStringAttribute();

    /**
     * The meta object literal for the '<em><b>Value</b></em>' attribute feature.
     * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
     * @generated
     */
    EAttribute STRING_ATTRIBUTE__VALUE = eINSTANCE.getStringAttribute_Value();

    /**
     * The meta object literal for the '{@link org.eclipse.emf.eson.eFactory.impl.IntegerAttributeImpl <em>Integer Attribute</em>}' class.
     * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
     * @see org.eclipse.emf.eson.eFactory.impl.IntegerAttributeImpl
     * @see org.eclipse.emf.eson.eFactory.impl.EFactoryPackageImpl#getIntegerAttribute()
     * @generated
     */
    EClass INTEGER_ATTRIBUTE = eINSTANCE.getIntegerAttribute();

    /**
     * The meta object literal for the '<em><b>Value</b></em>' attribute feature.
     * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
     * @generated
     */
    EAttribute INTEGER_ATTRIBUTE__VALUE = eINSTANCE.getIntegerAttribute_Value();

    /**
     * The meta object literal for the '{@link org.eclipse.emf.eson.eFactory.impl.DoubleAttributeImpl <em>Double Attribute</em>}' class.
     * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
     * @see org.eclipse.emf.eson.eFactory.impl.DoubleAttributeImpl
     * @see org.eclipse.emf.eson.eFactory.impl.EFactoryPackageImpl#getDoubleAttribute()
     * @generated
     */
    EClass DOUBLE_ATTRIBUTE = eINSTANCE.getDoubleAttribute();

    /**
     * The meta object literal for the '<em><b>Value</b></em>' attribute feature.
     * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
     * @generated
     */
    EAttribute DOUBLE_ATTRIBUTE__VALUE = eINSTANCE.getDoubleAttribute_Value();

    /**
     * The meta object literal for the '{@link org.eclipse.emf.eson.eFactory.impl.DateAttributeImpl <em>Date Attribute</em>}' class.
     * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
     * @see org.eclipse.emf.eson.eFactory.impl.DateAttributeImpl
     * @see org.eclipse.emf.eson.eFactory.impl.EFactoryPackageImpl#getDateAttribute()
     * @generated
     */
    EClass DATE_ATTRIBUTE = eINSTANCE.getDateAttribute();

    /**
     * The meta object literal for the '<em><b>Value</b></em>' attribute feature.
     * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
     * @generated
     */
    EAttribute DATE_ATTRIBUTE__VALUE = eINSTANCE.getDateAttribute_Value();

    /**
     * The meta object literal for the '{@link org.eclipse.emf.eson.eFactory.impl.NullAttributeImpl <em>Null Attribute</em>}' class.
     * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
     * @see org.eclipse.emf.eson.eFactory.impl.NullAttributeImpl
     * @see org.eclipse.emf.eson.eFactory.impl.EFactoryPackageImpl#getNullAttribute()
     * @generated
     */
    EClass NULL_ATTRIBUTE = eINSTANCE.getNullAttribute();

    /**
     * The meta object literal for the '<em><b>Value</b></em>' attribute feature.
     * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
     * @generated
     */
    EAttribute NULL_ATTRIBUTE__VALUE = eINSTANCE.getNullAttribute_Value();

    /**
     * The meta object literal for the '{@link org.eclipse.emf.eson.eFactory.impl.BooleanAttributeImpl <em>Boolean Attribute</em>}' class.
     * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
     * @see org.eclipse.emf.eson.eFactory.impl.BooleanAttributeImpl
     * @see org.eclipse.emf.eson.eFactory.impl.EFactoryPackageImpl#getBooleanAttribute()
     * @generated
     */
    EClass BOOLEAN_ATTRIBUTE = eINSTANCE.getBooleanAttribute();

    /**
     * The meta object literal for the '<em><b>Value</b></em>' attribute feature.
     * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
     * @generated
     */
    EAttribute BOOLEAN_ATTRIBUTE__VALUE = eINSTANCE.getBooleanAttribute_Value();

    /**
     * The meta object literal for the '{@link org.eclipse.emf.eson.eFactory.impl.ContainmentImpl <em>Containment</em>}' class.
     * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
     * @see org.eclipse.emf.eson.eFactory.impl.ContainmentImpl
     * @see org.eclipse.emf.eson.eFactory.impl.EFactoryPackageImpl#getContainment()
     * @generated
     */
    EClass CONTAINMENT = eINSTANCE.getContainment();

    /**
     * The meta object literal for the '<em><b>Value</b></em>' containment reference feature.
     * <!-- begin-user-doc -->
     * <!-- end-user-doc -->
     * @generated
     */
    EReference CONTAINMENT__VALUE = eINSTANCE.getContainment_Value();

  }

} //EFactoryPackage
