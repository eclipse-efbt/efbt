/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_views.impl;

import java.util.Collection;

import org.eclipse.efbt.language.dtm.model.dtm_views.DTMView;
import org.eclipse.efbt.language.dtm.model.dtm_views.VersionedDTMViewsModule;
import org.eclipse.efbt.language.dtm.model.dtm_views.dtm_viewsPackage;

import org.eclipse.efbt.language.dtm.model.transformation.impl.VersionedFunctionalModuleLogicImpl;

import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Versioned DTM Views Module</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_views.impl.VersionedDTMViewsModuleImpl#getDTMViews <em>DTM Views</em>}</li>
 * </ul>
 *
 * @generated
 */
public class VersionedDTMViewsModuleImpl extends VersionedFunctionalModuleLogicImpl implements VersionedDTMViewsModule {
	/**
	 * The cached value of the '{@link #getDTMViews() <em>DTM Views</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getDTMViews()
	 * @generated
	 * @ordered
	 */
	protected EList<DTMView> dtmViews;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected VersionedDTMViewsModuleImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return dtm_viewsPackage.Literals.VERSIONED_DTM_VIEWS_MODULE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<DTMView> getDTMViews() {
		if (dtmViews == null) {
			dtmViews = new EObjectContainmentEList<DTMView>(DTMView.class, this, dtm_viewsPackage.VERSIONED_DTM_VIEWS_MODULE__DTM_VIEWS);
		}
		return dtmViews;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case dtm_viewsPackage.VERSIONED_DTM_VIEWS_MODULE__DTM_VIEWS:
				return ((InternalEList<?>)getDTMViews()).basicRemove(otherEnd, msgs);
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
			case dtm_viewsPackage.VERSIONED_DTM_VIEWS_MODULE__DTM_VIEWS:
				return getDTMViews();
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
			case dtm_viewsPackage.VERSIONED_DTM_VIEWS_MODULE__DTM_VIEWS:
				getDTMViews().clear();
				getDTMViews().addAll((Collection<? extends DTMView>)newValue);
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
			case dtm_viewsPackage.VERSIONED_DTM_VIEWS_MODULE__DTM_VIEWS:
				getDTMViews().clear();
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
			case dtm_viewsPackage.VERSIONED_DTM_VIEWS_MODULE__DTM_VIEWS:
				return dtmViews != null && !dtmViews.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //VersionedDTMViewsModuleImpl
