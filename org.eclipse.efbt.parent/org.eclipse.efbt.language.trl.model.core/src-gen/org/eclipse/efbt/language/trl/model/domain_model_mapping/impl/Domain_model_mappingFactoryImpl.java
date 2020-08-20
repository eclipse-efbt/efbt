/**
 */
package org.eclipse.efbt.language.trl.model.domain_model_mapping.impl;

import org.eclipse.efbt.language.trl.model.domain_model_mapping.*;

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
public class Domain_model_mappingFactoryImpl extends EFactoryImpl implements Domain_model_mappingFactory
{
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static Domain_model_mappingFactory init()
	{
		try
		{
			Domain_model_mappingFactory theDomain_model_mappingFactory = (Domain_model_mappingFactory)EPackage.Registry.INSTANCE.getEFactory(Domain_model_mappingPackage.eNS_URI);
			if (theDomain_model_mappingFactory != null)
			{
				return theDomain_model_mappingFactory;
			}
		}
		catch (Exception exception)
		{
			EcorePlugin.INSTANCE.log(exception);
		}
		return new Domain_model_mappingFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Domain_model_mappingFactoryImpl()
	{
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EObject create(EClass eClass)
	{
		switch (eClass.getClassifierID())
		{
			case Domain_model_mappingPackage.LEAF_MODULE_TO_VERSIONED_SQL_VIEWS_MODULE: return createLeafModuleToVersionedSQLViewsModule();
			case Domain_model_mappingPackage.LEAF_MODULE_TO_VERSIONED_CUBE_SCHEMA_MODULE: return createLeafModuleToVersionedCubeSchemaModule();
			case Domain_model_mappingPackage.LEAF_MODULE_TO_REPORT_CELL_VIEW_MODULE: return createLeafModuleToReportCellViewModule();
			case Domain_model_mappingPackage.SCENARIO_TO_CODE_CONDITIONS: return createScenarioToCodeConditions();
			case Domain_model_mappingPackage.CODE_CONDITION: return createCodeCondition();
			case Domain_model_mappingPackage.COLUMN_SETTING_CONDITION: return createColumnSettingCondition();
			case Domain_model_mappingPackage.DATA_SET_FILTER_CONDITION: return createDataSetFilterCondition();
			case Domain_model_mappingPackage.UNION_CHOICE_CONDITION: return createUnionChoiceCondition();
			case Domain_model_mappingPackage.DOMAIN_MAPPING_MODULE: return createDomainMappingModule();
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
	public LeafModuleToVersionedSQLViewsModule createLeafModuleToVersionedSQLViewsModule()
	{
		LeafModuleToVersionedSQLViewsModuleImpl leafModuleToVersionedSQLViewsModule = new LeafModuleToVersionedSQLViewsModuleImpl();
		return leafModuleToVersionedSQLViewsModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public LeafModuleToVersionedCubeSchemaModule createLeafModuleToVersionedCubeSchemaModule()
	{
		LeafModuleToVersionedCubeSchemaModuleImpl leafModuleToVersionedCubeSchemaModule = new LeafModuleToVersionedCubeSchemaModuleImpl();
		return leafModuleToVersionedCubeSchemaModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public LeafModuleToReportCellViewModule createLeafModuleToReportCellViewModule()
	{
		LeafModuleToReportCellViewModuleImpl leafModuleToReportCellViewModule = new LeafModuleToReportCellViewModuleImpl();
		return leafModuleToReportCellViewModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ScenarioToCodeConditions createScenarioToCodeConditions()
	{
		ScenarioToCodeConditionsImpl scenarioToCodeConditions = new ScenarioToCodeConditionsImpl();
		return scenarioToCodeConditions;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public CodeCondition createCodeCondition()
	{
		CodeConditionImpl codeCondition = new CodeConditionImpl();
		return codeCondition;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ColumnSettingCondition createColumnSettingCondition()
	{
		ColumnSettingConditionImpl columnSettingCondition = new ColumnSettingConditionImpl();
		return columnSettingCondition;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public DataSetFilterCondition createDataSetFilterCondition()
	{
		DataSetFilterConditionImpl dataSetFilterCondition = new DataSetFilterConditionImpl();
		return dataSetFilterCondition;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public UnionChoiceCondition createUnionChoiceCondition()
	{
		UnionChoiceConditionImpl unionChoiceCondition = new UnionChoiceConditionImpl();
		return unionChoiceCondition;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public DomainMappingModule createDomainMappingModule()
	{
		DomainMappingModuleImpl domainMappingModule = new DomainMappingModuleImpl();
		return domainMappingModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Domain_model_mappingPackage getDomain_model_mappingPackage()
	{
		return (Domain_model_mappingPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static Domain_model_mappingPackage getPackage()
	{
		return Domain_model_mappingPackage.eINSTANCE;
	}

} //Domain_model_mappingFactoryImpl
