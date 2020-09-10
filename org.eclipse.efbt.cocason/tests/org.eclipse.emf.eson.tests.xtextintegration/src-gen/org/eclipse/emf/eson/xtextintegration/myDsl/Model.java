/**
 */
package org.eclipse.emf.eson.xtextintegration.myDsl;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Model</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * <ul>
 *   <li>{@link org.eclipse.emf.eson.xtextintegration.myDsl.Model#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.emf.eson.xtextintegration.myDsl.Model#getRefModel <em>Ref Model</em>}</li>
 *   <li>{@link org.eclipse.emf.eson.xtextintegration.myDsl.Model#getRefModels <em>Ref Models</em>}</li>
 *   <li>{@link org.eclipse.emf.eson.xtextintegration.myDsl.Model#getGreetings <em>Greetings</em>}</li>
 * </ul>
 * </p>
 *
 * @see org.eclipse.emf.eson.xtextintegration.myDsl.MyDslPackage#getModel()
 * @model
 * @generated
 */
public interface Model extends EObject
{
  /**
   * Returns the value of the '<em><b>Name</b></em>' attribute.
   * <!-- begin-user-doc -->
   * <p>
   * If the meaning of the '<em>Name</em>' attribute isn't clear,
   * there really should be more of a description here...
   * </p>
   * <!-- end-user-doc -->
   * @return the value of the '<em>Name</em>' attribute.
   * @see #setName(String)
   * @see org.eclipse.emf.eson.xtextintegration.myDsl.MyDslPackage#getModel_Name()
   * @model
   * @generated
   */
  String getName();

  /**
   * Sets the value of the '{@link org.eclipse.emf.eson.xtextintegration.myDsl.Model#getName <em>Name</em>}' attribute.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @param value the new value of the '<em>Name</em>' attribute.
   * @see #getName()
   * @generated
   */
  void setName(String value);

  /**
   * Returns the value of the '<em><b>Ref Model</b></em>' reference.
   * <!-- begin-user-doc -->
   * <p>
   * If the meaning of the '<em>Ref Model</em>' reference isn't clear,
   * there really should be more of a description here...
   * </p>
   * <!-- end-user-doc -->
   * @return the value of the '<em>Ref Model</em>' reference.
   * @see #setRefModel(Model)
   * @see org.eclipse.emf.eson.xtextintegration.myDsl.MyDslPackage#getModel_RefModel()
   * @model
   * @generated
   */
  Model getRefModel();

  /**
   * Sets the value of the '{@link org.eclipse.emf.eson.xtextintegration.myDsl.Model#getRefModel <em>Ref Model</em>}' reference.
   * <!-- begin-user-doc -->
   * <!-- end-user-doc -->
   * @param value the new value of the '<em>Ref Model</em>' reference.
   * @see #getRefModel()
   * @generated
   */
  void setRefModel(Model value);

  /**
   * Returns the value of the '<em><b>Ref Models</b></em>' reference list.
   * The list contents are of type {@link org.eclipse.emf.eson.xtextintegration.myDsl.Model}.
   * <!-- begin-user-doc -->
   * <p>
   * If the meaning of the '<em>Ref Models</em>' reference list isn't clear,
   * there really should be more of a description here...
   * </p>
   * <!-- end-user-doc -->
   * @return the value of the '<em>Ref Models</em>' reference list.
   * @see org.eclipse.emf.eson.xtextintegration.myDsl.MyDslPackage#getModel_RefModels()
   * @model
   * @generated
   */
  EList<Model> getRefModels();

  /**
   * Returns the value of the '<em><b>Greetings</b></em>' containment reference list.
   * The list contents are of type {@link org.eclipse.emf.eson.xtextintegration.myDsl.Greeting}.
   * <!-- begin-user-doc -->
   * <p>
   * If the meaning of the '<em>Greetings</em>' containment reference list isn't clear,
   * there really should be more of a description here...
   * </p>
   * <!-- end-user-doc -->
   * @return the value of the '<em>Greetings</em>' containment reference list.
   * @see org.eclipse.emf.eson.xtextintegration.myDsl.MyDslPackage#getModel_Greetings()
   * @model containment="true"
   * @generated
   */
  EList<Greeting> getGreetings();

} // Model
