/**
 */
package org.eclipse.efbt.cocalimo.platform_call.model.platform_call.impl;

import org.eclipse.efbt.cocalimo.platform_call.model.platform_call.ConvertDataMetaModelToEcore;
import org.eclipse.efbt.cocalimo.platform_call.model.platform_call.Platform_callPackage;

import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.EntityModule;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Convert Data Meta Model To Ecore</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.platform_call.model.platform_call.impl.ConvertDataMetaModelToEcoreImpl#getOutputDirectory <em>Output Directory</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.platform_call.model.platform_call.impl.ConvertDataMetaModelToEcoreImpl#getEntityModule <em>Entity Module</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ConvertDataMetaModelToEcoreImpl extends PlatformCallImpl implements ConvertDataMetaModelToEcore {
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
	 * The cached value of the '{@link #getEntityModule() <em>Entity Module</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getEntityModule()
	 * @generated
	 * @ordered
	 */
	protected EntityModule entityModule;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ConvertDataMetaModelToEcoreImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Platform_callPackage.Literals.CONVERT_DATA_META_MODEL_TO_ECORE;
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
			eNotify(new ENotificationImpl(this, Notification.SET, Platform_callPackage.CONVERT_DATA_META_MODEL_TO_ECORE__OUTPUT_DIRECTORY, oldOutputDirectory, outputDirectory));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EntityModule getEntityModule() {
		if (entityModule != null && entityModule.eIsProxy()) {
			InternalEObject oldEntityModule = (InternalEObject)entityModule;
			entityModule = (EntityModule)eResolveProxy(oldEntityModule);
			if (entityModule != oldEntityModule) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Platform_callPackage.CONVERT_DATA_META_MODEL_TO_ECORE__ENTITY_MODULE, oldEntityModule, entityModule));
			}
		}
		return entityModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EntityModule basicGetEntityModule() {
		return entityModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setEntityModule(EntityModule newEntityModule) {
		EntityModule oldEntityModule = entityModule;
		entityModule = newEntityModule;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Platform_callPackage.CONVERT_DATA_META_MODEL_TO_ECORE__ENTITY_MODULE, oldEntityModule, entityModule));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Platform_callPackage.CONVERT_DATA_META_MODEL_TO_ECORE__OUTPUT_DIRECTORY:
				return getOutputDirectory();
			case Platform_callPackage.CONVERT_DATA_META_MODEL_TO_ECORE__ENTITY_MODULE:
				if (resolve) return getEntityModule();
				return basicGetEntityModule();
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
			case Platform_callPackage.CONVERT_DATA_META_MODEL_TO_ECORE__OUTPUT_DIRECTORY:
				setOutputDirectory((String)newValue);
				return;
			case Platform_callPackage.CONVERT_DATA_META_MODEL_TO_ECORE__ENTITY_MODULE:
				setEntityModule((EntityModule)newValue);
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
			case Platform_callPackage.CONVERT_DATA_META_MODEL_TO_ECORE__OUTPUT_DIRECTORY:
				setOutputDirectory(OUTPUT_DIRECTORY_EDEFAULT);
				return;
			case Platform_callPackage.CONVERT_DATA_META_MODEL_TO_ECORE__ENTITY_MODULE:
				setEntityModule((EntityModule)null);
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
			case Platform_callPackage.CONVERT_DATA_META_MODEL_TO_ECORE__OUTPUT_DIRECTORY:
				return OUTPUT_DIRECTORY_EDEFAULT == null ? outputDirectory != null : !OUTPUT_DIRECTORY_EDEFAULT.equals(outputDirectory);
			case Platform_callPackage.CONVERT_DATA_META_MODEL_TO_ECORE__ENTITY_MODULE:
				return entityModule != null;
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
		result.append(" (outputDirectory: ");
		result.append(outputDirectory);
		result.append(')');
		return result.toString();
	}

} //ConvertDataMetaModelToEcoreImpl
