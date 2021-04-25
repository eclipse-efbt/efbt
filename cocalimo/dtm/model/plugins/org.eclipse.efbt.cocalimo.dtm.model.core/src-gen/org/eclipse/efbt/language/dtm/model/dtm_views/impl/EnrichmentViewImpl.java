/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_views.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.functions.BasicColumnFunction;

import org.eclipse.efbt.language.dtm.model.dtm_views.EnrichmentView;
import org.eclipse.efbt.language.dtm.model.dtm_views.IgnoreColumn;
import org.eclipse.efbt.language.dtm.model.dtm_views.RenameAs;
import org.eclipse.efbt.language.dtm.model.dtm_views.dtm_viewsPackage;

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
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.EnrichmentViewImpl#getFunctions <em>Functions</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.EnrichmentViewImpl#getRenames <em>Renames</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.EnrichmentViewImpl#getIgnores <em>Ignores</em>}</li>
 * </ul>
 *
 * @generated
 */
public class EnrichmentViewImpl extends DTMViewImpl implements EnrichmentView {
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
	 * The cached value of the '{@link #getIgnores() <em>Ignores</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getIgnores()
	 * @generated
	 * @ordered
	 */
	protected EList<IgnoreColumn> ignores;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected EnrichmentViewImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return dtm_viewsPackage.Literals.ENRICHMENT_VIEW;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<BasicColumnFunction> getFunctions() {
		if (functions == null) {
			functions = new EObjectContainmentEList<BasicColumnFunction>(BasicColumnFunction.class, this, dtm_viewsPackage.ENRICHMENT_VIEW__FUNCTIONS);
		}
		return functions;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<RenameAs> getRenames() {
		if (renames == null) {
			renames = new EObjectContainmentEList<RenameAs>(RenameAs.class, this, dtm_viewsPackage.ENRICHMENT_VIEW__RENAMES);
		}
		return renames;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<IgnoreColumn> getIgnores() {
		if (ignores == null) {
			ignores = new EObjectContainmentEList<IgnoreColumn>(IgnoreColumn.class, this, dtm_viewsPackage.ENRICHMENT_VIEW__IGNORES);
		}
		return ignores;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case dtm_viewsPackage.ENRICHMENT_VIEW__FUNCTIONS:
				return ((InternalEList<?>)getFunctions()).basicRemove(otherEnd, msgs);
			case dtm_viewsPackage.ENRICHMENT_VIEW__RENAMES:
				return ((InternalEList<?>)getRenames()).basicRemove(otherEnd, msgs);
			case dtm_viewsPackage.ENRICHMENT_VIEW__IGNORES:
				return ((InternalEList<?>)getIgnores()).basicRemove(otherEnd, msgs);
		}
		return super.eInverseRemove(otherEnd, featureID, msgs);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case dtm_viewsPackage.ENRICHMENT_VIEW__FUNCTIONS:
				return getFunctions();
			case dtm_viewsPackage.ENRICHMENT_VIEW__RENAMES:
				return getRenames();
			case dtm_viewsPackage.ENRICHMENT_VIEW__IGNORES:
				return getIgnores();
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
	public void eSet(int featureID, Object newValue) {
		switch (featureID) {
			case dtm_viewsPackage.ENRICHMENT_VIEW__FUNCTIONS:
				getFunctions().clear();
				getFunctions().addAll((Collection<? extends BasicColumnFunction>)newValue);
				return;
			case dtm_viewsPackage.ENRICHMENT_VIEW__RENAMES:
				getRenames().clear();
				getRenames().addAll((Collection<? extends RenameAs>)newValue);
				return;
			case dtm_viewsPackage.ENRICHMENT_VIEW__IGNORES:
				getIgnores().clear();
				getIgnores().addAll((Collection<? extends IgnoreColumn>)newValue);
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
			case dtm_viewsPackage.ENRICHMENT_VIEW__FUNCTIONS:
				getFunctions().clear();
				return;
			case dtm_viewsPackage.ENRICHMENT_VIEW__RENAMES:
				getRenames().clear();
				return;
			case dtm_viewsPackage.ENRICHMENT_VIEW__IGNORES:
				getIgnores().clear();
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
			case dtm_viewsPackage.ENRICHMENT_VIEW__FUNCTIONS:
				return functions != null && !functions.isEmpty();
			case dtm_viewsPackage.ENRICHMENT_VIEW__RENAMES:
				return renames != null && !renames.isEmpty();
			case dtm_viewsPackage.ENRICHMENT_VIEW__IGNORES:
				return ignores != null && !ignores.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //EnrichmentViewImpl
