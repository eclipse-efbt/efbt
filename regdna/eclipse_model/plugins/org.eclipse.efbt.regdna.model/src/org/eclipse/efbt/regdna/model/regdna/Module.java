/**
 */
package org.eclipse.efbt.regdna.model.regdna;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>Module</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.Module#getDependencies <em>Dependencies</em>}</li>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.Module#getTheDescription <em>The Description</em>}</li>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.Module#getLicense <em>License</em>}</li>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.Module#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.Module#getVersion <em>Version</em>}</li>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.Module#getImports <em>Imports</em>}</li>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.Module#getAnnotationDirectives <em>Annotation Directives</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getModule()
 * @model
 * @generated
 */
public interface Module extends EObject {
	/**
	 * Returns the value of the '<em><b>Dependencies</b></em>' reference list.
	 * The list contents are of type {@link org.eclipse.efbt.regdna.model.regdna.Module}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Dependencies</em>' reference list.
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getModule_Dependencies()
	 * @model
	 * @generated
	 */
	EList<Module> getDependencies();

	/**
	 * Returns the value of the '<em><b>The Description</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>The Description</em>' attribute.
	 * @see #setTheDescription(String)
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getModule_TheDescription()
	 * @model
	 * @generated
	 */
	String getTheDescription();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.regdna.model.regdna.Module#getTheDescription <em>The Description</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>The Description</em>' attribute.
	 * @see #getTheDescription()
	 * @generated
	 */
	void setTheDescription(String value);

	/**
	 * Returns the value of the '<em><b>License</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>License</em>' attribute.
	 * @see #setLicense(String)
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getModule_License()
	 * @model
	 * @generated
	 */
	String getLicense();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.regdna.model.regdna.Module#getLicense <em>License</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>License</em>' attribute.
	 * @see #getLicense()
	 * @generated
	 */
	void setLicense(String value);

	/**
	 * Returns the value of the '<em><b>Name</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Name</em>' attribute.
	 * @see #setName(String)
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getModule_Name()
	 * @model id="true"
	 * @generated
	 */
	String getName();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.regdna.model.regdna.Module#getName <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Name</em>' attribute.
	 * @see #getName()
	 * @generated
	 */
	void setName(String value);

	/**
	 * Returns the value of the '<em><b>Version</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Version</em>' attribute.
	 * @see #setVersion(String)
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getModule_Version()
	 * @model
	 * @generated
	 */
	String getVersion();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.regdna.model.regdna.Module#getVersion <em>Version</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Version</em>' attribute.
	 * @see #getVersion()
	 * @generated
	 */
	void setVersion(String value);

	/**
	 * Returns the value of the '<em><b>Imports</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.regdna.model.regdna.Import}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Imports</em>' containment reference list.
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getModule_Imports()
	 * @model containment="true"
	 * @generated
	 */
	EList<Import> getImports();

	/**
	 * Returns the value of the '<em><b>Annotation Directives</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.regdna.model.regdna.ELAnnotationDirective}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Annotation Directives</em>' containment reference list.
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#getModule_AnnotationDirectives()
	 * @model containment="true"
	 * @generated
	 */
	EList<ELAnnotationDirective> getAnnotationDirectives();

} // Module
