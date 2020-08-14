/**
 */
package org.eclipse.efbt.language.trl.model.domain_model_mapping.impl;

import java.util.Collection;

import org.eclipse.efbt.common.model.module_management.impl.ModuleImpl;

import org.eclipse.efbt.language.trl.model.domain_model_mapping.DomainMappingModule;
import org.eclipse.efbt.language.trl.model.domain_model_mapping.Domain_model_mappingPackage;
import org.eclipse.efbt.language.trl.model.domain_model_mapping.LeafModuleToReportCellViewModule;
import org.eclipse.efbt.language.trl.model.domain_model_mapping.LeafModuleToVersionedCubeSchemaModule;
import org.eclipse.efbt.language.trl.model.domain_model_mapping.LeafModuleToVersionedSQLViewsModule;
import org.eclipse.efbt.language.trl.model.domain_model_mapping.ScenarioToCodeConditions;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Domain Mapping Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.trl.model.domain_model_mapping.impl.DomainMappingModuleImpl#getLeafModuleToVersionedSQLViewsModules <em>Leaf Module To Versioned SQL Views Modules</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.trl.model.domain_model_mapping.impl.DomainMappingModuleImpl#getLeafModuleToVersionedCubeSchemaModules <em>Leaf Module To Versioned Cube Schema Modules</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.trl.model.domain_model_mapping.impl.DomainMappingModuleImpl#getLeafModuleToReportCellViewModules <em>Leaf Module To Report Cell View Modules</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.trl.model.domain_model_mapping.impl.DomainMappingModuleImpl#getScenarioToCodeConditions <em>Scenario To Code Conditions</em>}</li>
 * </ul>
 *
 * @generated
 */
public class DomainMappingModuleImpl extends ModuleImpl implements DomainMappingModule
{
	/**
	 * The cached value of the '{@link #getLeafModuleToVersionedSQLViewsModules() <em>Leaf Module To Versioned SQL Views Modules</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getLeafModuleToVersionedSQLViewsModules()
	 * @generated
	 * @ordered
	 */
	protected EList<LeafModuleToVersionedSQLViewsModule> leafModuleToVersionedSQLViewsModules;

	/**
	 * The cached value of the '{@link #getLeafModuleToVersionedCubeSchemaModules() <em>Leaf Module To Versioned Cube Schema Modules</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getLeafModuleToVersionedCubeSchemaModules()
	 * @generated
	 * @ordered
	 */
	protected EList<LeafModuleToVersionedCubeSchemaModule> leafModuleToVersionedCubeSchemaModules;

	/**
	 * The cached value of the '{@link #getLeafModuleToReportCellViewModules() <em>Leaf Module To Report Cell View Modules</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getLeafModuleToReportCellViewModules()
	 * @generated
	 * @ordered
	 */
	protected EList<LeafModuleToReportCellViewModule> leafModuleToReportCellViewModules;

	/**
	 * The cached value of the '{@link #getScenarioToCodeConditions() <em>Scenario To Code Conditions</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getScenarioToCodeConditions()
	 * @generated
	 * @ordered
	 */
	protected EList<ScenarioToCodeConditions> scenarioToCodeConditions;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected DomainMappingModuleImpl()
	{
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass()
	{
		return Domain_model_mappingPackage.Literals.DOMAIN_MAPPING_MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<LeafModuleToVersionedSQLViewsModule> getLeafModuleToVersionedSQLViewsModules()
	{
		if (leafModuleToVersionedSQLViewsModules == null)
		{
			leafModuleToVersionedSQLViewsModules = new EObjectContainmentEList<LeafModuleToVersionedSQLViewsModule>(LeafModuleToVersionedSQLViewsModule.class, this, Domain_model_mappingPackage.DOMAIN_MAPPING_MODULE__LEAF_MODULE_TO_VERSIONED_SQL_VIEWS_MODULES);
		}
		return leafModuleToVersionedSQLViewsModules;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<LeafModuleToVersionedCubeSchemaModule> getLeafModuleToVersionedCubeSchemaModules()
	{
		if (leafModuleToVersionedCubeSchemaModules == null)
		{
			leafModuleToVersionedCubeSchemaModules = new EObjectContainmentEList<LeafModuleToVersionedCubeSchemaModule>(LeafModuleToVersionedCubeSchemaModule.class, this, Domain_model_mappingPackage.DOMAIN_MAPPING_MODULE__LEAF_MODULE_TO_VERSIONED_CUBE_SCHEMA_MODULES);
		}
		return leafModuleToVersionedCubeSchemaModules;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<LeafModuleToReportCellViewModule> getLeafModuleToReportCellViewModules()
	{
		if (leafModuleToReportCellViewModules == null)
		{
			leafModuleToReportCellViewModules = new EObjectContainmentEList<LeafModuleToReportCellViewModule>(LeafModuleToReportCellViewModule.class, this, Domain_model_mappingPackage.DOMAIN_MAPPING_MODULE__LEAF_MODULE_TO_REPORT_CELL_VIEW_MODULES);
		}
		return leafModuleToReportCellViewModules;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<ScenarioToCodeConditions> getScenarioToCodeConditions()
	{
		if (scenarioToCodeConditions == null)
		{
			scenarioToCodeConditions = new EObjectContainmentEList<ScenarioToCodeConditions>(ScenarioToCodeConditions.class, this, Domain_model_mappingPackage.DOMAIN_MAPPING_MODULE__SCENARIO_TO_CODE_CONDITIONS);
		}
		return scenarioToCodeConditions;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs)
	{
		switch (featureID)
		{
			case Domain_model_mappingPackage.DOMAIN_MAPPING_MODULE__LEAF_MODULE_TO_VERSIONED_SQL_VIEWS_MODULES:
				return ((InternalEList<?>)getLeafModuleToVersionedSQLViewsModules()).basicRemove(otherEnd, msgs);
			case Domain_model_mappingPackage.DOMAIN_MAPPING_MODULE__LEAF_MODULE_TO_VERSIONED_CUBE_SCHEMA_MODULES:
				return ((InternalEList<?>)getLeafModuleToVersionedCubeSchemaModules()).basicRemove(otherEnd, msgs);
			case Domain_model_mappingPackage.DOMAIN_MAPPING_MODULE__LEAF_MODULE_TO_REPORT_CELL_VIEW_MODULES:
				return ((InternalEList<?>)getLeafModuleToReportCellViewModules()).basicRemove(otherEnd, msgs);
			case Domain_model_mappingPackage.DOMAIN_MAPPING_MODULE__SCENARIO_TO_CODE_CONDITIONS:
				return ((InternalEList<?>)getScenarioToCodeConditions()).basicRemove(otherEnd, msgs);
		}
		return super.eInverseRemove(otherEnd, featureID, msgs);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType)
	{
		switch (featureID)
		{
			case Domain_model_mappingPackage.DOMAIN_MAPPING_MODULE__LEAF_MODULE_TO_VERSIONED_SQL_VIEWS_MODULES:
				return getLeafModuleToVersionedSQLViewsModules();
			case Domain_model_mappingPackage.DOMAIN_MAPPING_MODULE__LEAF_MODULE_TO_VERSIONED_CUBE_SCHEMA_MODULES:
				return getLeafModuleToVersionedCubeSchemaModules();
			case Domain_model_mappingPackage.DOMAIN_MAPPING_MODULE__LEAF_MODULE_TO_REPORT_CELL_VIEW_MODULES:
				return getLeafModuleToReportCellViewModules();
			case Domain_model_mappingPackage.DOMAIN_MAPPING_MODULE__SCENARIO_TO_CODE_CONDITIONS:
				return getScenarioToCodeConditions();
		}
		return super.eGet(featureID, resolve, coreType);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void eSet(int featureID, Object newValue)
	{
		switch (featureID)
		{
			case Domain_model_mappingPackage.DOMAIN_MAPPING_MODULE__LEAF_MODULE_TO_VERSIONED_SQL_VIEWS_MODULES:
				getLeafModuleToVersionedSQLViewsModules().clear();
				getLeafModuleToVersionedSQLViewsModules().addAll((Collection<? extends LeafModuleToVersionedSQLViewsModule>)newValue);
				return;
			case Domain_model_mappingPackage.DOMAIN_MAPPING_MODULE__LEAF_MODULE_TO_VERSIONED_CUBE_SCHEMA_MODULES:
				getLeafModuleToVersionedCubeSchemaModules().clear();
				getLeafModuleToVersionedCubeSchemaModules().addAll((Collection<? extends LeafModuleToVersionedCubeSchemaModule>)newValue);
				return;
			case Domain_model_mappingPackage.DOMAIN_MAPPING_MODULE__LEAF_MODULE_TO_REPORT_CELL_VIEW_MODULES:
				getLeafModuleToReportCellViewModules().clear();
				getLeafModuleToReportCellViewModules().addAll((Collection<? extends LeafModuleToReportCellViewModule>)newValue);
				return;
			case Domain_model_mappingPackage.DOMAIN_MAPPING_MODULE__SCENARIO_TO_CODE_CONDITIONS:
				getScenarioToCodeConditions().clear();
				getScenarioToCodeConditions().addAll((Collection<? extends ScenarioToCodeConditions>)newValue);
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
	public void eUnset(int featureID)
	{
		switch (featureID)
		{
			case Domain_model_mappingPackage.DOMAIN_MAPPING_MODULE__LEAF_MODULE_TO_VERSIONED_SQL_VIEWS_MODULES:
				getLeafModuleToVersionedSQLViewsModules().clear();
				return;
			case Domain_model_mappingPackage.DOMAIN_MAPPING_MODULE__LEAF_MODULE_TO_VERSIONED_CUBE_SCHEMA_MODULES:
				getLeafModuleToVersionedCubeSchemaModules().clear();
				return;
			case Domain_model_mappingPackage.DOMAIN_MAPPING_MODULE__LEAF_MODULE_TO_REPORT_CELL_VIEW_MODULES:
				getLeafModuleToReportCellViewModules().clear();
				return;
			case Domain_model_mappingPackage.DOMAIN_MAPPING_MODULE__SCENARIO_TO_CODE_CONDITIONS:
				getScenarioToCodeConditions().clear();
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
	public boolean eIsSet(int featureID)
	{
		switch (featureID)
		{
			case Domain_model_mappingPackage.DOMAIN_MAPPING_MODULE__LEAF_MODULE_TO_VERSIONED_SQL_VIEWS_MODULES:
				return leafModuleToVersionedSQLViewsModules != null && !leafModuleToVersionedSQLViewsModules.isEmpty();
			case Domain_model_mappingPackage.DOMAIN_MAPPING_MODULE__LEAF_MODULE_TO_VERSIONED_CUBE_SCHEMA_MODULES:
				return leafModuleToVersionedCubeSchemaModules != null && !leafModuleToVersionedCubeSchemaModules.isEmpty();
			case Domain_model_mappingPackage.DOMAIN_MAPPING_MODULE__LEAF_MODULE_TO_REPORT_CELL_VIEW_MODULES:
				return leafModuleToReportCellViewModules != null && !leafModuleToReportCellViewModules.isEmpty();
			case Domain_model_mappingPackage.DOMAIN_MAPPING_MODULE__SCENARIO_TO_CODE_CONDITIONS:
				return scenarioToCodeConditions != null && !scenarioToCodeConditions.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //DomainMappingModuleImpl
