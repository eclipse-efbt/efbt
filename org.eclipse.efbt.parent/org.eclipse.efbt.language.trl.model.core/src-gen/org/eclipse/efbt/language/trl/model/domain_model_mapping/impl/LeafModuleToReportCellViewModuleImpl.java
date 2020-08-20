/**
 */
package org.eclipse.efbt.language.trl.model.domain_model_mapping.impl;

import org.eclipse.efbt.functional_design.core.model.functionality_module.LeafFunctionalityModule;

import org.eclipse.efbt.language.trl.model.domain_model_mapping.Domain_model_mappingPackage;
import org.eclipse.efbt.language.trl.model.domain_model_mapping.LeafModuleToReportCellViewModule;

import org.eclipse.efbt.language.trl.model.trl_report_cell_views.ReportCellViewModule;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Leaf Module To Report Cell View Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.trl.model.domain_model_mapping.impl.LeafModuleToReportCellViewModuleImpl#getScheme <em>Scheme</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.trl.model.domain_model_mapping.impl.LeafModuleToReportCellViewModuleImpl#getModule <em>Module</em>}</li>
 * </ul>
 *
 * @generated
 */
public class LeafModuleToReportCellViewModuleImpl extends MinimalEObjectImpl.Container implements LeafModuleToReportCellViewModule
{
	/**
	 * The cached value of the '{@link #getScheme() <em>Scheme</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getScheme()
	 * @generated
	 * @ordered
	 */
	protected LeafFunctionalityModule scheme;

	/**
	 * The cached value of the '{@link #getModule() <em>Module</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getModule()
	 * @generated
	 * @ordered
	 */
	protected ReportCellViewModule module;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected LeafModuleToReportCellViewModuleImpl()
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
		return Domain_model_mappingPackage.Literals.LEAF_MODULE_TO_REPORT_CELL_VIEW_MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public LeafFunctionalityModule getScheme()
	{
		if (scheme != null && scheme.eIsProxy())
		{
			InternalEObject oldScheme = (InternalEObject)scheme;
			scheme = (LeafFunctionalityModule)eResolveProxy(oldScheme);
			if (scheme != oldScheme)
			{
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Domain_model_mappingPackage.LEAF_MODULE_TO_REPORT_CELL_VIEW_MODULE__SCHEME, oldScheme, scheme));
			}
		}
		return scheme;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public LeafFunctionalityModule basicGetScheme()
	{
		return scheme;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setScheme(LeafFunctionalityModule newScheme)
	{
		LeafFunctionalityModule oldScheme = scheme;
		scheme = newScheme;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Domain_model_mappingPackage.LEAF_MODULE_TO_REPORT_CELL_VIEW_MODULE__SCHEME, oldScheme, scheme));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ReportCellViewModule getModule()
	{
		if (module != null && module.eIsProxy())
		{
			InternalEObject oldModule = (InternalEObject)module;
			module = (ReportCellViewModule)eResolveProxy(oldModule);
			if (module != oldModule)
			{
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Domain_model_mappingPackage.LEAF_MODULE_TO_REPORT_CELL_VIEW_MODULE__MODULE, oldModule, module));
			}
		}
		return module;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ReportCellViewModule basicGetModule()
	{
		return module;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setModule(ReportCellViewModule newModule)
	{
		ReportCellViewModule oldModule = module;
		module = newModule;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Domain_model_mappingPackage.LEAF_MODULE_TO_REPORT_CELL_VIEW_MODULE__MODULE, oldModule, module));
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
			case Domain_model_mappingPackage.LEAF_MODULE_TO_REPORT_CELL_VIEW_MODULE__SCHEME:
				if (resolve) return getScheme();
				return basicGetScheme();
			case Domain_model_mappingPackage.LEAF_MODULE_TO_REPORT_CELL_VIEW_MODULE__MODULE:
				if (resolve) return getModule();
				return basicGetModule();
		}
		return super.eGet(featureID, resolve, coreType);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void eSet(int featureID, Object newValue)
	{
		switch (featureID)
		{
			case Domain_model_mappingPackage.LEAF_MODULE_TO_REPORT_CELL_VIEW_MODULE__SCHEME:
				setScheme((LeafFunctionalityModule)newValue);
				return;
			case Domain_model_mappingPackage.LEAF_MODULE_TO_REPORT_CELL_VIEW_MODULE__MODULE:
				setModule((ReportCellViewModule)newValue);
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
			case Domain_model_mappingPackage.LEAF_MODULE_TO_REPORT_CELL_VIEW_MODULE__SCHEME:
				setScheme((LeafFunctionalityModule)null);
				return;
			case Domain_model_mappingPackage.LEAF_MODULE_TO_REPORT_CELL_VIEW_MODULE__MODULE:
				setModule((ReportCellViewModule)null);
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
			case Domain_model_mappingPackage.LEAF_MODULE_TO_REPORT_CELL_VIEW_MODULE__SCHEME:
				return scheme != null;
			case Domain_model_mappingPackage.LEAF_MODULE_TO_REPORT_CELL_VIEW_MODULE__MODULE:
				return module != null;
		}
		return super.eIsSet(featureID);
	}

} //LeafModuleToReportCellViewModuleImpl
