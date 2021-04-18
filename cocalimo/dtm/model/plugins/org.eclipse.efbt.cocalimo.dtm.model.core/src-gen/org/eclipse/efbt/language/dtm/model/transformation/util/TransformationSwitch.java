/**
 */
package org.eclipse.efbt.language.dtm.model.transformation.util;

import org.eclipse.efbt.language.dtm.model.transformation.*;

import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;

import org.eclipse.emf.ecore.util.Switch;

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
 * @see org.eclipse.efbt.language.dtm.model.transformation.TransformationPackage
 * @generated
 */
public class TransformationSwitch<T> extends Switch<T>
{
	/**
	 * The cached model package
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static TransformationPackage modelPackage;

	/**
	 * Creates an instance of the switch.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TransformationSwitch()
	{
		if (modelPackage == null) {
			modelPackage = TransformationPackage.eINSTANCE;
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
		switch (classifierID) {
			case TransformationPackage.DATA_SET_TRANSFORMATION: {
				DataSetTransformation dataSetTransformation = (DataSetTransformation)theEObject;
				T result = caseDataSetTransformation(dataSetTransformation);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case TransformationPackage.RELEASE: {
				Release release = (Release)theEObject;
				T result = caseRelease(release);
				if (result == null) result = caseVersionedComponentsSet(release);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case TransformationPackage.VERSIONED_COMPONENTS_SET: {
				VersionedComponentsSet versionedComponentsSet = (VersionedComponentsSet)theEObject;
				T result = caseVersionedComponentsSet(versionedComponentsSet);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case TransformationPackage.VERSIONED_CUBE_SCHEMA_MODULE: {
				VersionedCubeSchemaModule versionedCubeSchemaModule = (VersionedCubeSchemaModule)theEObject;
				T result = caseVersionedCubeSchemaModule(versionedCubeSchemaModule);
				if (result == null) result = caseModule(versionedCubeSchemaModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case TransformationPackage.VERSIONED_FUNCTIONAL_MODULE_LOGIC: {
				VersionedFunctionalModuleLogic versionedFunctionalModuleLogic = (VersionedFunctionalModuleLogic)theEObject;
				T result = caseVersionedFunctionalModuleLogic(versionedFunctionalModuleLogic);
				if (result == null) result = caseModule(versionedFunctionalModuleLogic);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case TransformationPackage.DTM_EXECUTABLE_LOGIC: {
				dtmExecutableLogic dtmExecutableLogic = (dtmExecutableLogic)theEObject;
				T result = casedtmExecutableLogic(dtmExecutableLogic);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case TransformationPackage.DTM_PROGRAM: {
				dtmProgram dtmProgram = (dtmProgram)theEObject;
				T result = casedtmProgram(dtmProgram);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			default: return defaultCase(theEObject);
		}
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Data Set Transformation</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Data Set Transformation</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseDataSetTransformation(DataSetTransformation object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Release</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Release</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseRelease(Release object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Versioned Components Set</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Versioned Components Set</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseVersionedComponentsSet(VersionedComponentsSet object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Versioned Cube Schema Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Versioned Cube Schema Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseVersionedCubeSchemaModule(VersionedCubeSchemaModule object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Versioned Functional Module Logic</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Versioned Functional Module Logic</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseVersionedFunctionalModuleLogic(VersionedFunctionalModuleLogic object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>dtm Executable Logic</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>dtm Executable Logic</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T casedtmExecutableLogic(dtmExecutableLogic object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>dtm Program</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>dtm Program</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T casedtmProgram(dtmProgram object)
	{
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseModule(org.eclipse.efbt.cocalimo.core.model.module_management.Module object)
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

} //TransformationSwitch
