/**
 */
package org.eclipse.efbt.cocalimo.core.model.platform_call.impl;

import org.eclipse.efbt.cocalimo.core.model.platform_call.*;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;

import org.eclipse.emf.ecore.impl.EFactoryImpl;

import org.eclipse.emf.ecore.plugin.EcorePlugin;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model <b>Factory</b>.
 * <!-- end-user-doc -->
 * @generated
 */
public class Platform_callFactoryImpl extends EFactoryImpl implements Platform_callFactory {
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static Platform_callFactory init() {
		try {
			Platform_callFactory thePlatform_callFactory = (Platform_callFactory)EPackage.Registry.INSTANCE.getEFactory(Platform_callPackage.eNS_URI);
			if (thePlatform_callFactory != null) {
				return thePlatform_callFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new Platform_callFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Platform_callFactoryImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EObject create(EClass eClass) {
		switch (eClass.getClassifierID()) {
			case Platform_callPackage.PLATFORM_CALL: return createPlatformCall();
			case Platform_callPackage.PLATFORM_CALL_MODULE: return createPlatformCallModule();
			case Platform_callPackage.CONVERT_SQL_DEVELOPER_MODEL_TO_ECORE: return createConvertSQLDeveloperModelToEcore();
			case Platform_callPackage.IMPORT_BIRD_FROM_MS_ACCESS: return createImportBIRDFromMSAccess();
			case Platform_callPackage.CREATE_LOGICAL_TRANSFORMATION_VIEW_FOR_SCOPE: return createCreateLogicalTransformationViewForScope();
			default:
				throw new IllegalArgumentException("The class '" + eClass.getName() + "' is not a valid classifier");
		}
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public PlatformCall createPlatformCall() {
		PlatformCallImpl platformCall = new PlatformCallImpl();
		return platformCall;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public PlatformCallModule createPlatformCallModule() {
		PlatformCallModuleImpl platformCallModule = new PlatformCallModuleImpl();
		return platformCallModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ConvertSQLDeveloperModelToEcore createConvertSQLDeveloperModelToEcore() {
		ConvertSQLDeveloperModelToEcoreImpl convertSQLDeveloperModelToEcore = new ConvertSQLDeveloperModelToEcoreImpl();
		return convertSQLDeveloperModelToEcore;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ImportBIRDFromMSAccess createImportBIRDFromMSAccess() {
		ImportBIRDFromMSAccessImpl importBIRDFromMSAccess = new ImportBIRDFromMSAccessImpl();
		return importBIRDFromMSAccess;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public CreateLogicalTransformationViewForScope createCreateLogicalTransformationViewForScope() {
		CreateLogicalTransformationViewForScopeImpl createLogicalTransformationViewForScope = new CreateLogicalTransformationViewForScopeImpl();
		return createLogicalTransformationViewForScope;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Platform_callPackage getPlatform_callPackage() {
		return (Platform_callPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static Platform_callPackage getPackage() {
		return Platform_callPackage.eINSTANCE;
	}

} //Platform_callFactoryImpl
