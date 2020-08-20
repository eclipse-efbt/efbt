/**
 */
package org.eclipse.efbt.language.trl.model.trl_sql_views.impl;

import java.util.Collection;

import org.eclipse.efbt.language.trl.model.trl_sql_views.EnrichmentView;
import org.eclipse.efbt.language.trl.model.trl_sql_views.RenameAs;
import org.eclipse.efbt.language.trl.model.trl_sql_views.Trl_sql_viewsPackage;

import org.eclipse.efbt.lineage.common.model.column_transformation_logic.BasicColumnFunction;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Enrichment View</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.trl.model.trl_sql_views.impl.EnrichmentViewImpl#getFunctions <em>Functions</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.trl.model.trl_sql_views.impl.EnrichmentViewImpl#getRenames <em>Renames</em>}</li>
 * </ul>
 *
 * @generated
 */
public class EnrichmentViewImpl extends SQLViewImpl implements EnrichmentView
{
	/**
	 * The cached value of the '{@link #getFunctions() <em>Functions</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getFunctions()
	 * @generated
	 * @ordered
	 */
	protected EList<BasicColumnFunction> functions;

	/**
	 * The cached value of the '{@link #getRenames() <em>Renames</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getRenames()
	 * @generated
	 * @ordered
	 */
	protected EList<RenameAs> renames;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected EnrichmentViewImpl()
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
		return Trl_sql_viewsPackage.Literals.ENRICHMENT_VIEW;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<BasicColumnFunction> getFunctions()
	{
		if (functions == null)
		{
			functions = new EObjectContainmentEList<BasicColumnFunction>(BasicColumnFunction.class, this, Trl_sql_viewsPackage.ENRICHMENT_VIEW__FUNCTIONS);
		}
		return functions;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<RenameAs> getRenames()
	{
		if (renames == null)
		{
			renames = new EObjectContainmentEList<RenameAs>(RenameAs.class, this, Trl_sql_viewsPackage.ENRICHMENT_VIEW__RENAMES);
		}
		return renames;
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
			case Trl_sql_viewsPackage.ENRICHMENT_VIEW__FUNCTIONS:
				return ((InternalEList<?>)getFunctions()).basicRemove(otherEnd, msgs);
			case Trl_sql_viewsPackage.ENRICHMENT_VIEW__RENAMES:
				return ((InternalEList<?>)getRenames()).basicRemove(otherEnd, msgs);
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
			case Trl_sql_viewsPackage.ENRICHMENT_VIEW__FUNCTIONS:
				return getFunctions();
			case Trl_sql_viewsPackage.ENRICHMENT_VIEW__RENAMES:
				return getRenames();
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
			case Trl_sql_viewsPackage.ENRICHMENT_VIEW__FUNCTIONS:
				getFunctions().clear();
				getFunctions().addAll((Collection<? extends BasicColumnFunction>)newValue);
				return;
			case Trl_sql_viewsPackage.ENRICHMENT_VIEW__RENAMES:
				getRenames().clear();
				getRenames().addAll((Collection<? extends RenameAs>)newValue);
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
			case Trl_sql_viewsPackage.ENRICHMENT_VIEW__FUNCTIONS:
				getFunctions().clear();
				return;
			case Trl_sql_viewsPackage.ENRICHMENT_VIEW__RENAMES:
				getRenames().clear();
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
			case Trl_sql_viewsPackage.ENRICHMENT_VIEW__FUNCTIONS:
				return functions != null && !functions.isEmpty();
			case Trl_sql_viewsPackage.ENRICHMENT_VIEW__RENAMES:
				return renames != null && !renames.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //EnrichmentViewImpl
