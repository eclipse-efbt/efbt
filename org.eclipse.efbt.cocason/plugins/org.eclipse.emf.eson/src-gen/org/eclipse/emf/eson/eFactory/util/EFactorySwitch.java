/**
 */
package org.eclipse.emf.eson.eFactory.util;

import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;

import org.eclipse.emf.ecore.util.Switch;

import org.eclipse.emf.eson.eFactory.*;

/**
 * <!-- begin-user-doc -->
 * The <b>Switch</b> for the model's inheritance hierarchy.
 * It supports the call {@link #doSwitch(EObject) doSwitch(object)}
 * to invoke the <code>caseXXX</code> method for each class of the model,
 * starting with the actual class of the object
 * and proceeding up the inheritance hierarchy
 * until a non-null result is returned,
 * which is the result of the switch.
 * <!-- end-user-doc -->
 * @see org.eclipse.emf.eson.eFactory.EFactoryPackage
 * @generated
 */
public class EFactorySwitch<T> extends Switch<T>
{
  /**
   * The cached model package
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  protected static EFactoryPackage modelPackage;

  /**
   * Creates an instance of the switch.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  public EFactorySwitch()
  {
    if (modelPackage == null)
    {
      modelPackage = EFactoryPackage.eINSTANCE;
    }
  }

  /**
   * Checks whether this is a switch for the given package.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @param ePackage the package in question.
   * @return whether this is a switch for the given package.
   * @generated
   */
  @Override
  protected boolean isSwitchFor(EPackage ePackage)
  {
    return ePackage == modelPackage;
  }

  /**
   * Calls <code>caseXXX</code> for each class of the model until one returns a non null result; it yields that result.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return the first non-null result returned by a <code>caseXXX</code> call.
   * @generated
   */
  @Override
  protected T doSwitch(int classifierID, EObject theEObject)
  {
    switch (classifierID)
    {
      case EFactoryPackage.FACTORY:
      {
        Factory factory = (Factory)theEObject;
        T result = caseFactory(factory);
        if (result == null) result = defaultCase(theEObject);
        return result;
      }
      case EFactoryPackage.PACKAGE_IMPORT:
      {
        PackageImport packageImport = (PackageImport)theEObject;
        T result = casePackageImport(packageImport);
        if (result == null) result = defaultCase(theEObject);
        return result;
      }
      case EFactoryPackage.NAMESPACE_IMPORT:
      {
        NamespaceImport namespaceImport = (NamespaceImport)theEObject;
        T result = caseNamespaceImport(namespaceImport);
        if (result == null) result = defaultCase(theEObject);
        return result;
      }
      case EFactoryPackage.ANNOTATION:
      {
        Annotation annotation = (Annotation)theEObject;
        T result = caseAnnotation(annotation);
        if (result == null) result = defaultCase(theEObject);
        return result;
      }
      case EFactoryPackage.CUSTOM_NAME_MAPPING:
      {
        CustomNameMapping customNameMapping = (CustomNameMapping)theEObject;
        T result = caseCustomNameMapping(customNameMapping);
        if (result == null) result = caseAnnotation(customNameMapping);
        if (result == null) result = defaultCase(theEObject);
        return result;
      }
      case EFactoryPackage.FEATURE:
      {
        Feature feature = (Feature)theEObject;
        T result = caseFeature(feature);
        if (result == null) result = defaultCase(theEObject);
        return result;
      }
      case EFactoryPackage.NEW_OBJECT:
      {
        NewObject newObject = (NewObject)theEObject;
        T result = caseNewObject(newObject);
        if (result == null) result = caseValue(newObject);
        if (result == null) result = defaultCase(theEObject);
        return result;
      }
      case EFactoryPackage.VALUE:
      {
        Value value = (Value)theEObject;
        T result = caseValue(value);
        if (result == null) result = defaultCase(theEObject);
        return result;
      }
      case EFactoryPackage.MULTI_VALUE:
      {
        MultiValue multiValue = (MultiValue)theEObject;
        T result = caseMultiValue(multiValue);
        if (result == null) result = caseValue(multiValue);
        if (result == null) result = defaultCase(theEObject);
        return result;
      }
      case EFactoryPackage.REFERENCE:
      {
        Reference reference = (Reference)theEObject;
        T result = caseReference(reference);
        if (result == null) result = caseValue(reference);
        if (result == null) result = defaultCase(theEObject);
        return result;
      }
      case EFactoryPackage.ATTRIBUTE:
      {
        Attribute attribute = (Attribute)theEObject;
        T result = caseAttribute(attribute);
        if (result == null) result = caseValue(attribute);
        if (result == null) result = defaultCase(theEObject);
        return result;
      }
      case EFactoryPackage.ENUM_ATTRIBUTE:
      {
        EnumAttribute enumAttribute = (EnumAttribute)theEObject;
        T result = caseEnumAttribute(enumAttribute);
        if (result == null) result = caseAttribute(enumAttribute);
        if (result == null) result = caseValue(enumAttribute);
        if (result == null) result = defaultCase(theEObject);
        return result;
      }
      case EFactoryPackage.STRING_ATTRIBUTE:
      {
        StringAttribute stringAttribute = (StringAttribute)theEObject;
        T result = caseStringAttribute(stringAttribute);
        if (result == null) result = caseAttribute(stringAttribute);
        if (result == null) result = caseValue(stringAttribute);
        if (result == null) result = defaultCase(theEObject);
        return result;
      }
      case EFactoryPackage.INTEGER_ATTRIBUTE:
      {
        IntegerAttribute integerAttribute = (IntegerAttribute)theEObject;
        T result = caseIntegerAttribute(integerAttribute);
        if (result == null) result = caseAttribute(integerAttribute);
        if (result == null) result = caseValue(integerAttribute);
        if (result == null) result = defaultCase(theEObject);
        return result;
      }
      case EFactoryPackage.DOUBLE_ATTRIBUTE:
      {
        DoubleAttribute doubleAttribute = (DoubleAttribute)theEObject;
        T result = caseDoubleAttribute(doubleAttribute);
        if (result == null) result = caseAttribute(doubleAttribute);
        if (result == null) result = caseValue(doubleAttribute);
        if (result == null) result = defaultCase(theEObject);
        return result;
      }
      case EFactoryPackage.DATE_ATTRIBUTE:
      {
        DateAttribute dateAttribute = (DateAttribute)theEObject;
        T result = caseDateAttribute(dateAttribute);
        if (result == null) result = caseAttribute(dateAttribute);
        if (result == null) result = caseValue(dateAttribute);
        if (result == null) result = defaultCase(theEObject);
        return result;
      }
      case EFactoryPackage.NULL_ATTRIBUTE:
      {
        NullAttribute nullAttribute = (NullAttribute)theEObject;
        T result = caseNullAttribute(nullAttribute);
        if (result == null) result = caseAttribute(nullAttribute);
        if (result == null) result = caseValue(nullAttribute);
        if (result == null) result = defaultCase(theEObject);
        return result;
      }
      case EFactoryPackage.BOOLEAN_ATTRIBUTE:
      {
        BooleanAttribute booleanAttribute = (BooleanAttribute)theEObject;
        T result = caseBooleanAttribute(booleanAttribute);
        if (result == null) result = caseAttribute(booleanAttribute);
        if (result == null) result = caseValue(booleanAttribute);
        if (result == null) result = defaultCase(theEObject);
        return result;
      }
      case EFactoryPackage.CONTAINMENT:
      {
        Containment containment = (Containment)theEObject;
        T result = caseContainment(containment);
        if (result == null) result = caseValue(containment);
        if (result == null) result = defaultCase(theEObject);
        return result;
      }
      default: return defaultCase(theEObject);
    }
  }

  /**
   * Returns the result of interpreting the object as an instance of '<em>Factory</em>'.
   * <!-- begin-user-doc -->
   * This implementation returns null;
   * returning a non-null result will terminate the switch.
   * <!-- end-user-doc -->
   * @param object the target of the switch.
   * @return the result of interpreting the object as an instance of '<em>Factory</em>'.
   * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
   * @generated
   */
  public T caseFactory(Factory object)
  {
    return null;
  }

  /**
   * Returns the result of interpreting the object as an instance of '<em>Package Import</em>'.
   * <!-- begin-user-doc -->
   * This implementation returns null;
   * returning a non-null result will terminate the switch.
   * <!-- end-user-doc -->
   * @param object the target of the switch.
   * @return the result of interpreting the object as an instance of '<em>Package Import</em>'.
   * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
   * @generated
   */
  public T casePackageImport(PackageImport object)
  {
    return null;
  }

  /**
   * Returns the result of interpreting the object as an instance of '<em>Namespace Import</em>'.
   * <!-- begin-user-doc -->
   * This implementation returns null;
   * returning a non-null result will terminate the switch.
   * <!-- end-user-doc -->
   * @param object the target of the switch.
   * @return the result of interpreting the object as an instance of '<em>Namespace Import</em>'.
   * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
   * @generated
   */
  public T caseNamespaceImport(NamespaceImport object)
  {
    return null;
  }

  /**
   * Returns the result of interpreting the object as an instance of '<em>Annotation</em>'.
   * <!-- begin-user-doc -->
   * This implementation returns null;
   * returning a non-null result will terminate the switch.
   * <!-- end-user-doc -->
   * @param object the target of the switch.
   * @return the result of interpreting the object as an instance of '<em>Annotation</em>'.
   * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
   * @generated
   */
  public T caseAnnotation(Annotation object)
  {
    return null;
  }

  /**
   * Returns the result of interpreting the object as an instance of '<em>Custom Name Mapping</em>'.
   * <!-- begin-user-doc -->
   * This implementation returns null;
   * returning a non-null result will terminate the switch.
   * <!-- end-user-doc -->
   * @param object the target of the switch.
   * @return the result of interpreting the object as an instance of '<em>Custom Name Mapping</em>'.
   * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
   * @generated
   */
  public T caseCustomNameMapping(CustomNameMapping object)
  {
    return null;
  }

  /**
   * Returns the result of interpreting the object as an instance of '<em>Feature</em>'.
   * <!-- begin-user-doc -->
   * This implementation returns null;
   * returning a non-null result will terminate the switch.
   * <!-- end-user-doc -->
   * @param object the target of the switch.
   * @return the result of interpreting the object as an instance of '<em>Feature</em>'.
   * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
   * @generated
   */
  public T caseFeature(Feature object)
  {
    return null;
  }

  /**
   * Returns the result of interpreting the object as an instance of '<em>New Object</em>'.
   * <!-- begin-user-doc -->
   * This implementation returns null;
   * returning a non-null result will terminate the switch.
   * <!-- end-user-doc -->
   * @param object the target of the switch.
   * @return the result of interpreting the object as an instance of '<em>New Object</em>'.
   * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
   * @generated
   */
  public T caseNewObject(NewObject object)
  {
    return null;
  }

  /**
   * Returns the result of interpreting the object as an instance of '<em>Value</em>'.
   * <!-- begin-user-doc -->
   * This implementation returns null;
   * returning a non-null result will terminate the switch.
   * <!-- end-user-doc -->
   * @param object the target of the switch.
   * @return the result of interpreting the object as an instance of '<em>Value</em>'.
   * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
   * @generated
   */
  public T caseValue(Value object)
  {
    return null;
  }

  /**
   * Returns the result of interpreting the object as an instance of '<em>Multi Value</em>'.
   * <!-- begin-user-doc -->
   * This implementation returns null;
   * returning a non-null result will terminate the switch.
   * <!-- end-user-doc -->
   * @param object the target of the switch.
   * @return the result of interpreting the object as an instance of '<em>Multi Value</em>'.
   * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
   * @generated
   */
  public T caseMultiValue(MultiValue object)
  {
    return null;
  }

  /**
   * Returns the result of interpreting the object as an instance of '<em>Reference</em>'.
   * <!-- begin-user-doc -->
   * This implementation returns null;
   * returning a non-null result will terminate the switch.
   * <!-- end-user-doc -->
   * @param object the target of the switch.
   * @return the result of interpreting the object as an instance of '<em>Reference</em>'.
   * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
   * @generated
   */
  public T caseReference(Reference object)
  {
    return null;
  }

  /**
   * Returns the result of interpreting the object as an instance of '<em>Attribute</em>'.
   * <!-- begin-user-doc -->
   * This implementation returns null;
   * returning a non-null result will terminate the switch.
   * <!-- end-user-doc -->
   * @param object the target of the switch.
   * @return the result of interpreting the object as an instance of '<em>Attribute</em>'.
   * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
   * @generated
   */
  public T caseAttribute(Attribute object)
  {
    return null;
  }

  /**
   * Returns the result of interpreting the object as an instance of '<em>Enum Attribute</em>'.
   * <!-- begin-user-doc -->
   * This implementation returns null;
   * returning a non-null result will terminate the switch.
   * <!-- end-user-doc -->
   * @param object the target of the switch.
   * @return the result of interpreting the object as an instance of '<em>Enum Attribute</em>'.
   * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
   * @generated
   */
  public T caseEnumAttribute(EnumAttribute object)
  {
    return null;
  }

  /**
   * Returns the result of interpreting the object as an instance of '<em>String Attribute</em>'.
   * <!-- begin-user-doc -->
   * This implementation returns null;
   * returning a non-null result will terminate the switch.
   * <!-- end-user-doc -->
   * @param object the target of the switch.
   * @return the result of interpreting the object as an instance of '<em>String Attribute</em>'.
   * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
   * @generated
   */
  public T caseStringAttribute(StringAttribute object)
  {
    return null;
  }

  /**
   * Returns the result of interpreting the object as an instance of '<em>Integer Attribute</em>'.
   * <!-- begin-user-doc -->
   * This implementation returns null;
   * returning a non-null result will terminate the switch.
   * <!-- end-user-doc -->
   * @param object the target of the switch.
   * @return the result of interpreting the object as an instance of '<em>Integer Attribute</em>'.
   * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
   * @generated
   */
  public T caseIntegerAttribute(IntegerAttribute object)
  {
    return null;
  }

  /**
   * Returns the result of interpreting the object as an instance of '<em>Double Attribute</em>'.
   * <!-- begin-user-doc -->
   * This implementation returns null;
   * returning a non-null result will terminate the switch.
   * <!-- end-user-doc -->
   * @param object the target of the switch.
   * @return the result of interpreting the object as an instance of '<em>Double Attribute</em>'.
   * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
   * @generated
   */
  public T caseDoubleAttribute(DoubleAttribute object)
  {
    return null;
  }

  /**
   * Returns the result of interpreting the object as an instance of '<em>Date Attribute</em>'.
   * <!-- begin-user-doc -->
   * This implementation returns null;
   * returning a non-null result will terminate the switch.
   * <!-- end-user-doc -->
   * @param object the target of the switch.
   * @return the result of interpreting the object as an instance of '<em>Date Attribute</em>'.
   * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
   * @generated
   */
  public T caseDateAttribute(DateAttribute object)
  {
    return null;
  }

  /**
   * Returns the result of interpreting the object as an instance of '<em>Null Attribute</em>'.
   * <!-- begin-user-doc -->
   * This implementation returns null;
   * returning a non-null result will terminate the switch.
   * <!-- end-user-doc -->
   * @param object the target of the switch.
   * @return the result of interpreting the object as an instance of '<em>Null Attribute</em>'.
   * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
   * @generated
   */
  public T caseNullAttribute(NullAttribute object)
  {
    return null;
  }

  /**
   * Returns the result of interpreting the object as an instance of '<em>Boolean Attribute</em>'.
   * <!-- begin-user-doc -->
   * This implementation returns null;
   * returning a non-null result will terminate the switch.
   * <!-- end-user-doc -->
   * @param object the target of the switch.
   * @return the result of interpreting the object as an instance of '<em>Boolean Attribute</em>'.
   * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
   * @generated
   */
  public T caseBooleanAttribute(BooleanAttribute object)
  {
    return null;
  }

  /**
   * Returns the result of interpreting the object as an instance of '<em>Containment</em>'.
   * <!-- begin-user-doc -->
   * This implementation returns null;
   * returning a non-null result will terminate the switch.
   * <!-- end-user-doc -->
   * @param object the target of the switch.
   * @return the result of interpreting the object as an instance of '<em>Containment</em>'.
   * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
   * @generated
   */
  public T caseContainment(Containment object)
  {
    return null;
  }

  /**
   * Returns the result of interpreting the object as an instance of '<em>EObject</em>'.
   * <!-- begin-user-doc -->
   * This implementation returns null;
   * returning a non-null result will terminate the switch, but this is the last case anyway.
   * <!-- end-user-doc -->
   * @param object the target of the switch.
   * @return the result of interpreting the object as an instance of '<em>EObject</em>'.
   * @see #doSwitch(org.eclipse.emf.ecore.EObject)
   * @generated
   */
  @Override
  public T defaultCase(EObject object)
  {
    return null;
  }

} //EFactorySwitch
