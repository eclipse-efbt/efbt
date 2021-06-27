/**
 */
package org.eclipse.efbt.cocalimo.core.model.functionality_module.util;

import org.eclipse.efbt.cocalimo.core.model.functionality_module.*;

import org.eclipse.efbt.cocalimo.core.model.requirements_text.Tag;

import org.eclipse.emf.common.notify.Adapter;
import org.eclipse.emf.common.notify.Notifier;

import org.eclipse.emf.common.notify.impl.AdapterFactoryImpl;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * The <b>Adapter Factory</b> for the model.
 * It provides an adapter <code>createXXX</code> method for each class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.Functionality_modulePackage
 * @generated
 */
public class Functionality_moduleAdapterFactory extends AdapterFactoryImpl {
	/**
	 * The cached model package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static Functionality_modulePackage modelPackage;

	/**
	 * Creates an instance of the adapter factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Functionality_moduleAdapterFactory() {
		if (modelPackage == null) {
			modelPackage = Functionality_modulePackage.eINSTANCE;
		}
	}

	/**
	 * Returns whether this factory is applicable for the type of the object.
	 * <!-- begin-user-doc -->
	 * This implementation returns <code>true</code> if the object is either the model's package or is an instance object of the model.
	 * <!-- end-user-doc -->
	 * @return whether this factory is applicable for the type of the object.
	 * @generated
	 */
	@Override
	public boolean isFactoryForType(Object object) {
		if (object == modelPackage) {
			return true;
		}
		if (object instanceof EObject) {
			return ((EObject)object).eClass().getEPackage() == modelPackage;
		}
		return false;
	}

	/**
	 * The switch that delegates to the <code>createXXX</code> methods.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected Functionality_moduleSwitch<Adapter> modelSwitch =
		new Functionality_moduleSwitch<Adapter>() {
			@Override
			public Adapter caseDataProcessingFunctionalityModule(DataProcessingFunctionalityModule object) {
				return createDataProcessingFunctionalityModuleAdapter();
			}
			@Override
			public Adapter caseLeafFunctionalityModule(LeafFunctionalityModule object) {
				return createLeafFunctionalityModuleAdapter();
			}
			@Override
			public Adapter caseParallelFunctionalityModule(ParallelFunctionalityModule object) {
				return createParallelFunctionalityModuleAdapter();
			}
			@Override
			public Adapter caseScenarioSetFunctionalityModule(ScenarioSetFunctionalityModule object) {
				return createScenarioSetFunctionalityModuleAdapter();
			}
			@Override
			public Adapter caseSerialFunctionalityModule(SerialFunctionalityModule object) {
				return createSerialFunctionalityModuleAdapter();
			}
			@Override
			public Adapter caseFunctionalityModuleSet(FunctionalityModuleSet object) {
				return createFunctionalityModuleSetAdapter();
			}
			@Override
			public Adapter caseReadDataSourceFunctionalityModule(ReadDataSourceFunctionalityModule object) {
				return createReadDataSourceFunctionalityModuleAdapter();
			}
			@Override
			public Adapter caseReportCellCreationFunctionalityModule(ReportCellCreationFunctionalityModule object) {
				return createReportCellCreationFunctionalityModuleAdapter();
			}
			@Override
			public Adapter caseFunctionalityModule(FunctionalityModule object) {
				return createFunctionalityModuleAdapter();
			}
			@Override
			public Adapter caseFunctionalityModuleModule(FunctionalityModuleModule object) {
				return createFunctionalityModuleModuleAdapter();
			}
			@Override
			public Adapter caseFunctionalityModuleTag(FunctionalityModuleTag object) {
				return createFunctionalityModuleTagAdapter();
			}
			@Override
			public Adapter caseValidationFunctionalityModule(ValidationFunctionalityModule object) {
				return createValidationFunctionalityModuleAdapter();
			}
			@Override
			public Adapter caseParallelChoiceFunctionalityModule(ParallelChoiceFunctionalityModule object) {
				return createParallelChoiceFunctionalityModuleAdapter();
			}
			@Override
			public Adapter caseModule(org.eclipse.efbt.cocalimo.core.model.module_management.Module object) {
				return createModuleAdapter();
			}
			@Override
			public Adapter caseTag(Tag object) {
				return createTagAdapter();
			}
			@Override
			public Adapter defaultCase(EObject object) {
				return createEObjectAdapter();
			}
		};

	/**
	 * Creates an adapter for the <code>target</code>.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param target the object to adapt.
	 * @return the adapter for the <code>target</code>.
	 * @generated
	 */
	@Override
	public Adapter createAdapter(Notifier target) {
		return modelSwitch.doSwitch((EObject)target);
	}


	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.DataProcessingFunctionalityModule <em>Data Processing Functionality Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.DataProcessingFunctionalityModule
	 * @generated
	 */
	public Adapter createDataProcessingFunctionalityModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.LeafFunctionalityModule <em>Leaf Functionality Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.LeafFunctionalityModule
	 * @generated
	 */
	public Adapter createLeafFunctionalityModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.ParallelFunctionalityModule <em>Parallel Functionality Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.ParallelFunctionalityModule
	 * @generated
	 */
	public Adapter createParallelFunctionalityModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.ScenarioSetFunctionalityModule <em>Scenario Set Functionality Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.ScenarioSetFunctionalityModule
	 * @generated
	 */
	public Adapter createScenarioSetFunctionalityModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.SerialFunctionalityModule <em>Serial Functionality Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.SerialFunctionalityModule
	 * @generated
	 */
	public Adapter createSerialFunctionalityModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModuleSet <em>Functionality Module Set</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModuleSet
	 * @generated
	 */
	public Adapter createFunctionalityModuleSetAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.ReadDataSourceFunctionalityModule <em>Read Data Source Functionality Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.ReadDataSourceFunctionalityModule
	 * @generated
	 */
	public Adapter createReadDataSourceFunctionalityModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.ReportCellCreationFunctionalityModule <em>Report Cell Creation Functionality Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.ReportCellCreationFunctionalityModule
	 * @generated
	 */
	public Adapter createReportCellCreationFunctionalityModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModule <em>Functionality Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModule
	 * @generated
	 */
	public Adapter createFunctionalityModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModuleModule <em>Functionality Module Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModuleModule
	 * @generated
	 */
	public Adapter createFunctionalityModuleModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModuleTag <em>Functionality Module Tag</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.FunctionalityModuleTag
	 * @generated
	 */
	public Adapter createFunctionalityModuleTagAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.ValidationFunctionalityModule <em>Validation Functionality Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.ValidationFunctionalityModule
	 * @generated
	 */
	public Adapter createValidationFunctionalityModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.core.model.functionality_module.ParallelChoiceFunctionalityModule <em>Parallel Choice Functionality Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.core.model.functionality_module.ParallelChoiceFunctionalityModule
	 * @generated
	 */
	public Adapter createParallelChoiceFunctionalityModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.core.model.module_management.Module <em>Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.core.model.module_management.Module
	 * @generated
	 */
	public Adapter createModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.core.model.requirements_text.Tag <em>Tag</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.core.model.requirements_text.Tag
	 * @generated
	 */
	public Adapter createTagAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for the default case.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @generated
	 */
	public Adapter createEObjectAdapter() {
		return null;
	}

} //Functionality_moduleAdapterFactory
