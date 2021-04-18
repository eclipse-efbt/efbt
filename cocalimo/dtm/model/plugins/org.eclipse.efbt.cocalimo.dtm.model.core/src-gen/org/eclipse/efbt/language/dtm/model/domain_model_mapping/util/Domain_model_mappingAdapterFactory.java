/**
 */
package org.eclipse.efbt.language.dtm.model.domain_model_mapping.util;

import org.eclipse.efbt.language.dtm.model.domain_model_mapping.*;

import org.eclipse.emf.common.notify.Adapter;
import org.eclipse.emf.common.notify.Notifier;

import org.eclipse.emf.common.notify.impl.AdapterFactoryImpl;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * The <b>Adapter Factory</b> for the model.
 * It provides an adapter <code>createXXX</code> method for each class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.language.dtm.model.domain_model_mapping.Domain_model_mappingPackage
 * @generated
 */
public class Domain_model_mappingAdapterFactory extends AdapterFactoryImpl
{
	/**
	 * The cached model package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static Domain_model_mappingPackage modelPackage;

	/**
	 * Creates an instance of the adapter factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Domain_model_mappingAdapterFactory()
	{
		if (modelPackage == null) {
			modelPackage = Domain_model_mappingPackage.eINSTANCE;
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
	public boolean isFactoryForType(Object object)
	{
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
	protected Domain_model_mappingSwitch<Adapter> modelSwitch =
		new Domain_model_mappingSwitch<Adapter>() {
			@Override
			public Adapter caseLeafModuleToVersionedSQLViewsModule(LeafModuleToVersionedSQLViewsModule object) {
				return createLeafModuleToVersionedSQLViewsModuleAdapter();
			}
			@Override
			public Adapter caseLeafModuleToVersionedCubeSchemaModule(LeafModuleToVersionedCubeSchemaModule object) {
				return createLeafModuleToVersionedCubeSchemaModuleAdapter();
			}
			@Override
			public Adapter caseLeafModuleToReportCellViewModule(LeafModuleToReportCellViewModule object) {
				return createLeafModuleToReportCellViewModuleAdapter();
			}
			@Override
			public Adapter caseScenarioToCodeConditions(ScenarioToCodeConditions object) {
				return createScenarioToCodeConditionsAdapter();
			}
			@Override
			public Adapter caseCodeCondition(CodeCondition object) {
				return createCodeConditionAdapter();
			}
			@Override
			public Adapter caseColumnSettingCondition(ColumnSettingCondition object) {
				return createColumnSettingConditionAdapter();
			}
			@Override
			public Adapter caseDataSetFilterCondition(DataSetFilterCondition object) {
				return createDataSetFilterConditionAdapter();
			}
			@Override
			public Adapter caseUnionChoiceCondition(UnionChoiceCondition object) {
				return createUnionChoiceConditionAdapter();
			}
			@Override
			public Adapter caseDomainMappingModule(DomainMappingModule object) {
				return createDomainMappingModuleAdapter();
			}
			@Override
			public Adapter caseModule(org.eclipse.efbt.cocalimo.core.model.module_management.Module object) {
				return createModuleAdapter();
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
	public Adapter createAdapter(Notifier target)
	{
		return modelSwitch.doSwitch((EObject)target);
	}


	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.dtm.model.domain_model_mapping.LeafModuleToVersionedSQLViewsModule <em>Leaf Module To Versioned SQL Views Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.dtm.model.domain_model_mapping.LeafModuleToVersionedSQLViewsModule
	 * @generated
	 */
	public Adapter createLeafModuleToVersionedSQLViewsModuleAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.dtm.model.domain_model_mapping.LeafModuleToVersionedCubeSchemaModule <em>Leaf Module To Versioned Cube Schema Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.dtm.model.domain_model_mapping.LeafModuleToVersionedCubeSchemaModule
	 * @generated
	 */
	public Adapter createLeafModuleToVersionedCubeSchemaModuleAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.dtm.model.domain_model_mapping.LeafModuleToReportCellViewModule <em>Leaf Module To Report Cell View Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.dtm.model.domain_model_mapping.LeafModuleToReportCellViewModule
	 * @generated
	 */
	public Adapter createLeafModuleToReportCellViewModuleAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.dtm.model.domain_model_mapping.ScenarioToCodeConditions <em>Scenario To Code Conditions</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.dtm.model.domain_model_mapping.ScenarioToCodeConditions
	 * @generated
	 */
	public Adapter createScenarioToCodeConditionsAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.dtm.model.domain_model_mapping.CodeCondition <em>Code Condition</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.dtm.model.domain_model_mapping.CodeCondition
	 * @generated
	 */
	public Adapter createCodeConditionAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.dtm.model.domain_model_mapping.ColumnSettingCondition <em>Column Setting Condition</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.dtm.model.domain_model_mapping.ColumnSettingCondition
	 * @generated
	 */
	public Adapter createColumnSettingConditionAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.dtm.model.domain_model_mapping.DataSetFilterCondition <em>Data Set Filter Condition</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.dtm.model.domain_model_mapping.DataSetFilterCondition
	 * @generated
	 */
	public Adapter createDataSetFilterConditionAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.dtm.model.domain_model_mapping.UnionChoiceCondition <em>Union Choice Condition</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.dtm.model.domain_model_mapping.UnionChoiceCondition
	 * @generated
	 */
	public Adapter createUnionChoiceConditionAdapter()
	{
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.language.dtm.model.domain_model_mapping.DomainMappingModule <em>Domain Mapping Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.language.dtm.model.domain_model_mapping.DomainMappingModule
	 * @generated
	 */
	public Adapter createDomainMappingModuleAdapter()
	{
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
	public Adapter createModuleAdapter()
	{
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
	public Adapter createEObjectAdapter()
	{
		return null;
	}

} //Domain_model_mappingAdapterFactory
