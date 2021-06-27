/**
 */
package org.eclipse.efbt.cocalimo.core.model.functionality_module.impl;

import org.eclipse.efbt.cocalimo.core.model.functionality_module.*;

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
public class Functionality_moduleFactoryImpl extends EFactoryImpl implements Functionality_moduleFactory {
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static Functionality_moduleFactory init() {
		try {
			Functionality_moduleFactory theFunctionality_moduleFactory = (Functionality_moduleFactory)EPackage.Registry.INSTANCE.getEFactory(Functionality_modulePackage.eNS_URI);
			if (theFunctionality_moduleFactory != null) {
				return theFunctionality_moduleFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new Functionality_moduleFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Functionality_moduleFactoryImpl() {
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
			case Functionality_modulePackage.DATA_PROCESSING_FUNCTIONALITY_MODULE: return createDataProcessingFunctionalityModule();
			case Functionality_modulePackage.LEAF_FUNCTIONALITY_MODULE: return createLeafFunctionalityModule();
			case Functionality_modulePackage.PARALLEL_FUNCTIONALITY_MODULE: return createParallelFunctionalityModule();
			case Functionality_modulePackage.SCENARIO_SET_FUNCTIONALITY_MODULE: return createScenarioSetFunctionalityModule();
			case Functionality_modulePackage.SERIAL_FUNCTIONALITY_MODULE: return createSerialFunctionalityModule();
			case Functionality_modulePackage.FUNCTIONALITY_MODULE_SET: return createFunctionalityModuleSet();
			case Functionality_modulePackage.READ_DATA_SOURCE_FUNCTIONALITY_MODULE: return createReadDataSourceFunctionalityModule();
			case Functionality_modulePackage.REPORT_CELL_CREATION_FUNCTIONALITY_MODULE: return createReportCellCreationFunctionalityModule();
			case Functionality_modulePackage.FUNCTIONALITY_MODULE_MODULE: return createFunctionalityModuleModule();
			case Functionality_modulePackage.FUNCTIONALITY_MODULE_TAG: return createFunctionalityModuleTag();
			case Functionality_modulePackage.VALIDATION_FUNCTIONALITY_MODULE: return createValidationFunctionalityModule();
			case Functionality_modulePackage.PARALLEL_CHOICE_FUNCTIONALITY_MODULE: return createParallelChoiceFunctionalityModule();
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
	public DataProcessingFunctionalityModule createDataProcessingFunctionalityModule() {
		DataProcessingFunctionalityModuleImpl dataProcessingFunctionalityModule = new DataProcessingFunctionalityModuleImpl();
		return dataProcessingFunctionalityModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public LeafFunctionalityModule createLeafFunctionalityModule() {
		LeafFunctionalityModuleImpl leafFunctionalityModule = new LeafFunctionalityModuleImpl();
		return leafFunctionalityModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ParallelFunctionalityModule createParallelFunctionalityModule() {
		ParallelFunctionalityModuleImpl parallelFunctionalityModule = new ParallelFunctionalityModuleImpl();
		return parallelFunctionalityModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ScenarioSetFunctionalityModule createScenarioSetFunctionalityModule() {
		ScenarioSetFunctionalityModuleImpl scenarioSetFunctionalityModule = new ScenarioSetFunctionalityModuleImpl();
		return scenarioSetFunctionalityModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public SerialFunctionalityModule createSerialFunctionalityModule() {
		SerialFunctionalityModuleImpl serialFunctionalityModule = new SerialFunctionalityModuleImpl();
		return serialFunctionalityModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public FunctionalityModuleSet createFunctionalityModuleSet() {
		FunctionalityModuleSetImpl functionalityModuleSet = new FunctionalityModuleSetImpl();
		return functionalityModuleSet;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ReadDataSourceFunctionalityModule createReadDataSourceFunctionalityModule() {
		ReadDataSourceFunctionalityModuleImpl readDataSourceFunctionalityModule = new ReadDataSourceFunctionalityModuleImpl();
		return readDataSourceFunctionalityModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ReportCellCreationFunctionalityModule createReportCellCreationFunctionalityModule() {
		ReportCellCreationFunctionalityModuleImpl reportCellCreationFunctionalityModule = new ReportCellCreationFunctionalityModuleImpl();
		return reportCellCreationFunctionalityModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public FunctionalityModuleModule createFunctionalityModuleModule() {
		FunctionalityModuleModuleImpl functionalityModuleModule = new FunctionalityModuleModuleImpl();
		return functionalityModuleModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public FunctionalityModuleTag createFunctionalityModuleTag() {
		FunctionalityModuleTagImpl functionalityModuleTag = new FunctionalityModuleTagImpl();
		return functionalityModuleTag;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ValidationFunctionalityModule createValidationFunctionalityModule() {
		ValidationFunctionalityModuleImpl validationFunctionalityModule = new ValidationFunctionalityModuleImpl();
		return validationFunctionalityModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ParallelChoiceFunctionalityModule createParallelChoiceFunctionalityModule() {
		ParallelChoiceFunctionalityModuleImpl parallelChoiceFunctionalityModule = new ParallelChoiceFunctionalityModuleImpl();
		return parallelChoiceFunctionalityModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Functionality_modulePackage getFunctionality_modulePackage() {
		return (Functionality_modulePackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static Functionality_modulePackage getPackage() {
		return Functionality_modulePackage.eINSTANCE;
	}

} //Functionality_moduleFactoryImpl
