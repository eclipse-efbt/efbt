/**
 */
package org.eclipse.efbt.language.dtm.model.transformation;

import org.eclipse.efbt.cocalimo.smcubes.model.program.SMCubesStaticModel;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.ArrayTypedVariableModule;
import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.efbt_advanced_data_definition.StructTypedVariablesModule;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * A representation of the model object '<em><b>dtm Program</b></em>'.
 * <!-- end-user-doc -->
 *
 * <p>
 * The following features are supported:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.transformation.dtmProgram#getExecutableLogic <em>Executable Logic</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.transformation.dtmProgram#getSmcubesStaticModel <em>Smcubes Static Model</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.transformation.dtmProgram#getStructVariablesModule <em>Struct Variables Module</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.transformation.dtmProgram#getArrayTypedVariablesModule <em>Array Typed Variables Module</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.transformation.dtmProgram#getRelease <em>Release</em>}</li>
 * </ul>
 *
 * @see org.eclipse.efbt.language.dtm.model.transformation.TransformationPackage#getdtmProgram()
 * @model
 * @generated
 */
public interface dtmProgram extends EObject {
	/**
	 * Returns the value of the '<em><b>Executable Logic</b></em>' containment reference list.
	 * The list contents are of type {@link org.eclipse.efbt.language.dtm.model.transformation.dtmExecutableLogic}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Executable Logic</em>' containment reference list.
	 * @see org.eclipse.efbt.language.dtm.model.transformation.TransformationPackage#getdtmProgram_ExecutableLogic()
	 * @model containment="true"
	 * @generated
	 */
	EList<dtmExecutableLogic> getExecutableLogic();

	/**
	 * Returns the value of the '<em><b>Smcubes Static Model</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Smcubes Static Model</em>' containment reference.
	 * @see #setSmcubesStaticModel(SMCubesStaticModel)
	 * @see org.eclipse.efbt.language.dtm.model.transformation.TransformationPackage#getdtmProgram_SmcubesStaticModel()
	 * @model containment="true"
	 * @generated
	 */
	SMCubesStaticModel getSmcubesStaticModel();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.dtm.model.transformation.dtmProgram#getSmcubesStaticModel <em>Smcubes Static Model</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Smcubes Static Model</em>' containment reference.
	 * @see #getSmcubesStaticModel()
	 * @generated
	 */
	void setSmcubesStaticModel(SMCubesStaticModel value);

	/**
	 * Returns the value of the '<em><b>Struct Variables Module</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Struct Variables Module</em>' containment reference.
	 * @see #setStructVariablesModule(StructTypedVariablesModule)
	 * @see org.eclipse.efbt.language.dtm.model.transformation.TransformationPackage#getdtmProgram_StructVariablesModule()
	 * @model containment="true"
	 * @generated
	 */
	StructTypedVariablesModule getStructVariablesModule();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.dtm.model.transformation.dtmProgram#getStructVariablesModule <em>Struct Variables Module</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Struct Variables Module</em>' containment reference.
	 * @see #getStructVariablesModule()
	 * @generated
	 */
	void setStructVariablesModule(StructTypedVariablesModule value);

	/**
	 * Returns the value of the '<em><b>Array Typed Variables Module</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Array Typed Variables Module</em>' containment reference.
	 * @see #setArrayTypedVariablesModule(ArrayTypedVariableModule)
	 * @see org.eclipse.efbt.language.dtm.model.transformation.TransformationPackage#getdtmProgram_ArrayTypedVariablesModule()
	 * @model containment="true"
	 * @generated
	 */
	ArrayTypedVariableModule getArrayTypedVariablesModule();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.dtm.model.transformation.dtmProgram#getArrayTypedVariablesModule <em>Array Typed Variables Module</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Array Typed Variables Module</em>' containment reference.
	 * @see #getArrayTypedVariablesModule()
	 * @generated
	 */
	void setArrayTypedVariablesModule(ArrayTypedVariableModule value);

	/**
	 * Returns the value of the '<em><b>Release</b></em>' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the value of the '<em>Release</em>' containment reference.
	 * @see #setRelease(Release)
	 * @see org.eclipse.efbt.language.dtm.model.transformation.TransformationPackage#getdtmProgram_Release()
	 * @model containment="true"
	 * @generated
	 */
	Release getRelease();

	/**
	 * Sets the value of the '{@link org.eclipse.efbt.language.dtm.model.transformation.dtmProgram#getRelease <em>Release</em>}' containment reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param value the new value of the '<em>Release</em>' containment reference.
	 * @see #getRelease()
	 * @generated
	 */
	void setRelease(Release value);

} // dtmProgram
