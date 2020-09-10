/**
 */
package org.eclipse.emf.eson.eFactory;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.emf.eson.eFactory.EFactoryPackage
 * @generated
 */
public interface EFactoryFactory extends EFactory
{
  /**
   * The singleton instance of the factory.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @generated
   */
  EFactoryFactory eINSTANCE = org.eclipse.emf.eson.eFactory.impl.EFactoryFactoryImpl.init();

  /**
   * Returns a new object of class '<em>Factory</em>'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return a new object of class '<em>Factory</em>'.
   * @generated
   */
  Factory createFactory();

  /**
   * Returns a new object of class '<em>Package Import</em>'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return a new object of class '<em>Package Import</em>'.
   * @generated
   */
  PackageImport createPackageImport();

  /**
   * Returns a new object of class '<em>Namespace Import</em>'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return a new object of class '<em>Namespace Import</em>'.
   * @generated
   */
  NamespaceImport createNamespaceImport();

  /**
   * Returns a new object of class '<em>Annotation</em>'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return a new object of class '<em>Annotation</em>'.
   * @generated
   */
  Annotation createAnnotation();

  /**
   * Returns a new object of class '<em>Custom Name Mapping</em>'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return a new object of class '<em>Custom Name Mapping</em>'.
   * @generated
   */
  CustomNameMapping createCustomNameMapping();

  /**
   * Returns a new object of class '<em>Feature</em>'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return a new object of class '<em>Feature</em>'.
   * @generated
   */
  Feature createFeature();

  /**
   * Returns a new object of class '<em>New Object</em>'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return a new object of class '<em>New Object</em>'.
   * @generated
   */
  NewObject createNewObject();

  /**
   * Returns a new object of class '<em>Value</em>'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return a new object of class '<em>Value</em>'.
   * @generated
   */
  Value createValue();

  /**
   * Returns a new object of class '<em>Multi Value</em>'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return a new object of class '<em>Multi Value</em>'.
   * @generated
   */
  MultiValue createMultiValue();

  /**
   * Returns a new object of class '<em>Reference</em>'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return a new object of class '<em>Reference</em>'.
   * @generated
   */
  Reference createReference();

  /**
   * Returns a new object of class '<em>Attribute</em>'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return a new object of class '<em>Attribute</em>'.
   * @generated
   */
  Attribute createAttribute();

  /**
   * Returns a new object of class '<em>Enum Attribute</em>'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return a new object of class '<em>Enum Attribute</em>'.
   * @generated
   */
  EnumAttribute createEnumAttribute();

  /**
   * Returns a new object of class '<em>String Attribute</em>'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return a new object of class '<em>String Attribute</em>'.
   * @generated
   */
  StringAttribute createStringAttribute();

  /**
   * Returns a new object of class '<em>Integer Attribute</em>'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return a new object of class '<em>Integer Attribute</em>'.
   * @generated
   */
  IntegerAttribute createIntegerAttribute();

  /**
   * Returns a new object of class '<em>Double Attribute</em>'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return a new object of class '<em>Double Attribute</em>'.
   * @generated
   */
  DoubleAttribute createDoubleAttribute();

  /**
   * Returns a new object of class '<em>Date Attribute</em>'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return a new object of class '<em>Date Attribute</em>'.
   * @generated
   */
  DateAttribute createDateAttribute();

  /**
   * Returns a new object of class '<em>Null Attribute</em>'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return a new object of class '<em>Null Attribute</em>'.
   * @generated
   */
  NullAttribute createNullAttribute();

  /**
   * Returns a new object of class '<em>Boolean Attribute</em>'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return a new object of class '<em>Boolean Attribute</em>'.
   * @generated
   */
  BooleanAttribute createBooleanAttribute();

  /**
   * Returns a new object of class '<em>Containment</em>'.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return a new object of class '<em>Containment</em>'.
   * @generated
   */
  Containment createContainment();

  /**
   * Returns the package supported by this factory.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @return the package supported by this factory.
   * @generated
   */
  EFactoryPackage getEFactoryPackage();

} //EFactoryFactory
