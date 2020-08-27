/**
 */
package org.eclipse.emf.eson.eFactory.impl;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;

import org.eclipse.emf.ecore.impl.EFactoryImpl;

import org.eclipse.emf.ecore.plugin.EcorePlugin;

import org.eclipse.emf.eson.eFactory.*;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model <b>Factory</b>.
 * <!-- end-user-doc -->
 * @generated
 */
public class EFactoryFactoryImpl extends EFactoryImpl implements EFactoryFactory
{
  /**
   * Creates the default factory implementation.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  public static EFactoryFactory init()
  {
    try
    {
      EFactoryFactory theEFactoryFactory = (EFactoryFactory)EPackage.Registry.INSTANCE.getEFactory(EFactoryPackage.eNS_URI);
      if (theEFactoryFactory != null)
      {
        return theEFactoryFactory;
      }
    }
    catch (Exception exception)
    {
      EcorePlugin.INSTANCE.log(exception);
    }
    return new EFactoryFactoryImpl();
  }

  /**
   * Creates an instance of the factory.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  public EFactoryFactoryImpl()
  {
    super();
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  @Override
  public EObject create(EClass eClass)
  {
    switch (eClass.getClassifierID())
    {
      case EFactoryPackage.FACTORY: return createFactory();
      case EFactoryPackage.PACKAGE_IMPORT: return createPackageImport();
      case EFactoryPackage.NAMESPACE_IMPORT: return createNamespaceImport();
      case EFactoryPackage.ANNOTATION: return createAnnotation();
      case EFactoryPackage.CUSTOM_NAME_MAPPING: return createCustomNameMapping();
      case EFactoryPackage.FEATURE: return createFeature();
      case EFactoryPackage.NEW_OBJECT: return createNewObject();
      case EFactoryPackage.VALUE: return createValue();
      case EFactoryPackage.MULTI_VALUE: return createMultiValue();
      case EFactoryPackage.REFERENCE: return createReference();
      case EFactoryPackage.ATTRIBUTE: return createAttribute();
      case EFactoryPackage.ENUM_ATTRIBUTE: return createEnumAttribute();
      case EFactoryPackage.STRING_ATTRIBUTE: return createStringAttribute();
      case EFactoryPackage.INTEGER_ATTRIBUTE: return createIntegerAttribute();
      case EFactoryPackage.DOUBLE_ATTRIBUTE: return createDoubleAttribute();
      case EFactoryPackage.DATE_ATTRIBUTE: return createDateAttribute();
      case EFactoryPackage.NULL_ATTRIBUTE: return createNullAttribute();
      case EFactoryPackage.BOOLEAN_ATTRIBUTE: return createBooleanAttribute();
      case EFactoryPackage.CONTAINMENT: return createContainment();
      default:
        throw new IllegalArgumentException("The class '" + eClass.getName() + "' is not a valid classifier");
    }
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  public Factory createFactory()
  {
    FactoryImpl factory = new FactoryImpl();
    return factory;
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  public PackageImport createPackageImport()
  {
    PackageImportImpl packageImport = new PackageImportImpl();
    return packageImport;
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  public NamespaceImport createNamespaceImport()
  {
    NamespaceImportImpl namespaceImport = new NamespaceImportImpl();
    return namespaceImport;
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  public Annotation createAnnotation()
  {
    AnnotationImpl annotation = new AnnotationImpl();
    return annotation;
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  public CustomNameMapping createCustomNameMapping()
  {
    CustomNameMappingImpl customNameMapping = new CustomNameMappingImpl();
    return customNameMapping;
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  public Feature createFeature()
  {
    FeatureImpl feature = new FeatureImpl();
    return feature;
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  public NewObject createNewObject()
  {
    NewObjectImpl newObject = new NewObjectImpl();
    return newObject;
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  public Value createValue()
  {
    ValueImpl value = new ValueImpl();
    return value;
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  public MultiValue createMultiValue()
  {
    MultiValueImpl multiValue = new MultiValueImpl();
    return multiValue;
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  public Reference createReference()
  {
    ReferenceImpl reference = new ReferenceImpl();
    return reference;
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  public Attribute createAttribute()
  {
    AttributeImpl attribute = new AttributeImpl();
    return attribute;
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  public EnumAttribute createEnumAttribute()
  {
    EnumAttributeImpl enumAttribute = new EnumAttributeImpl();
    return enumAttribute;
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  public StringAttribute createStringAttribute()
  {
    StringAttributeImpl stringAttribute = new StringAttributeImpl();
    return stringAttribute;
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  public IntegerAttribute createIntegerAttribute()
  {
    IntegerAttributeImpl integerAttribute = new IntegerAttributeImpl();
    return integerAttribute;
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  public DoubleAttribute createDoubleAttribute()
  {
    DoubleAttributeImpl doubleAttribute = new DoubleAttributeImpl();
    return doubleAttribute;
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  public DateAttribute createDateAttribute()
  {
    DateAttributeImpl dateAttribute = new DateAttributeImpl();
    return dateAttribute;
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  public NullAttribute createNullAttribute()
  {
    NullAttributeImpl nullAttribute = new NullAttributeImpl();
    return nullAttribute;
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  public BooleanAttribute createBooleanAttribute()
  {
    BooleanAttributeImpl booleanAttribute = new BooleanAttributeImpl();
    return booleanAttribute;
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  public Containment createContainment()
  {
    ContainmentImpl containment = new ContainmentImpl();
    return containment;
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  public EFactoryPackage getEFactoryPackage()
  {
    return (EFactoryPackage)getEPackage();
  }

  /**
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @deprecated
   * @generated
   */
  @Deprecated
  public static EFactoryPackage getPackage()
  {
    return EFactoryPackage.eINSTANCE;
  }

} //EFactoryFactoryImpl
