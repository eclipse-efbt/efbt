/**
 */
package org.eclipse.efbt.cocalimo.platform_call.model.platform_call.impl;

import org.eclipse.efbt.cocalimo.platform_call.model.platform_call.ImportBIRDFromMSAccess;
import org.eclipse.efbt.cocalimo.platform_call.model.platform_call.Platform_callPackage;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Import BIRD From MS Access</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.platform_call.model.platform_call.impl.ImportBIRDFromMSAccessImpl#getInputDirectory <em>Input Directory</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.platform_call.model.platform_call.impl.ImportBIRDFromMSAccessImpl#getOutputDirectory <em>Output Directory</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ImportBIRDFromMSAccessImpl extends PlatformCallImpl implements ImportBIRDFromMSAccess {
	/**
	 * The default value of the '{@link #getInputDirectory() <em>Input Directory</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getInputDirectory()
	 * @generated
	 * @ordered
	 */
	protected static final String INPUT_DIRECTORY_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getInputDirectory() <em>Input Directory</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getInputDirectory()
	 * @generated
	 * @ordered
	 */
	protected String inputDirectory = INPUT_DIRECTORY_EDEFAULT;

	/**
	 * The default value of the '{@link #getOutputDirectory() <em>Output Directory</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getOutputDirectory()
	 * @generated
	 * @ordered
	 */
	protected static final String OUTPUT_DIRECTORY_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getOutputDirectory() <em>Output Directory</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getOutputDirectory()
	 * @generated
	 * @ordered
	 */
	protected String outputDirectory = OUTPUT_DIRECTORY_EDEFAULT;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ImportBIRDFromMSAccessImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Platform_callPackage.Literals.IMPORT_BIRD_FROM_MS_ACCESS;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getInputDirectory() {
		return inputDirectory;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setInputDirectory(String newInputDirectory) {
		String oldInputDirectory = inputDirectory;
		inputDirectory = newInputDirectory;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Platform_callPackage.IMPORT_BIRD_FROM_MS_ACCESS__INPUT_DIRECTORY, oldInputDirectory, inputDirectory));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getOutputDirectory() {
		return outputDirectory;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setOutputDirectory(String newOutputDirectory) {
		String oldOutputDirectory = outputDirectory;
		outputDirectory = newOutputDirectory;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Platform_callPackage.IMPORT_BIRD_FROM_MS_ACCESS__OUTPUT_DIRECTORY, oldOutputDirectory, outputDirectory));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Platform_callPackage.IMPORT_BIRD_FROM_MS_ACCESS__INPUT_DIRECTORY:
				return getInputDirectory();
			case Platform_callPackage.IMPORT_BIRD_FROM_MS_ACCESS__OUTPUT_DIRECTORY:
				return getOutputDirectory();
		}
		return super.eGet(featureID, resolve, coreType);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void eSet(int featureID, Object newValue) {
		switch (featureID) {
			case Platform_callPackage.IMPORT_BIRD_FROM_MS_ACCESS__INPUT_DIRECTORY:
				setInputDirectory((String)newValue);
				return;
			case Platform_callPackage.IMPORT_BIRD_FROM_MS_ACCESS__OUTPUT_DIRECTORY:
				setOutputDirectory((String)newValue);
				return;
		}
		super.eSet(featureID, newValue);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void eUnset(int featureID) {
		switch (featureID) {
			case Platform_callPackage.IMPORT_BIRD_FROM_MS_ACCESS__INPUT_DIRECTORY:
				setInputDirectory(INPUT_DIRECTORY_EDEFAULT);
				return;
			case Platform_callPackage.IMPORT_BIRD_FROM_MS_ACCESS__OUTPUT_DIRECTORY:
				setOutputDirectory(OUTPUT_DIRECTORY_EDEFAULT);
				return;
		}
		super.eUnset(featureID);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public boolean eIsSet(int featureID) {
		switch (featureID) {
			case Platform_callPackage.IMPORT_BIRD_FROM_MS_ACCESS__INPUT_DIRECTORY:
				return INPUT_DIRECTORY_EDEFAULT == null ? inputDirectory != null : !INPUT_DIRECTORY_EDEFAULT.equals(inputDirectory);
			case Platform_callPackage.IMPORT_BIRD_FROM_MS_ACCESS__OUTPUT_DIRECTORY:
				return OUTPUT_DIRECTORY_EDEFAULT == null ? outputDirectory != null : !OUTPUT_DIRECTORY_EDEFAULT.equals(outputDirectory);
		}
		return super.eIsSet(featureID);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String toString() {
		if (eIsProxy()) return super.toString();

		StringBuilder result = new StringBuilder(super.toString());
		result.append(" (inputDirectory: ");
		result.append(inputDirectory);
		result.append(", outputDirectory: ");
		result.append(outputDirectory);
		result.append(')');
		return result.toString();
	}

} //ImportBIRDFromMSAccessImpl
