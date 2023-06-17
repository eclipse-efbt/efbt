/**
 */
package org.eclipse.efbt.ecore4reg.model.ecore4reg.impl;

import org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.RulesForReport;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLForView;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.VTLGeneratedOutputlayer;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>VTL For View</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLForViewImpl#getName <em>Name</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLForViewImpl#getView <em>View</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.VTLForViewImpl#getVtl <em>Vtl</em>}</li>
 * </ul>
 *
 * @generated
 */
public class VTLForViewImpl extends MinimalEObjectImpl.Container implements VTLForView {
	/**
	 * The default value of the '{@link #getName() <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getName()
	 * @generated
	 * @ordered
	 */
	protected static final String NAME_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getName() <em>Name</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getName()
	 * @generated
	 * @ordered
	 */
	protected String name = NAME_EDEFAULT;

	/**
	 * The cached value of the '{@link #getView() <em>View</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getView()
	 * @generated
	 * @ordered
	 */
	protected RulesForReport view;

	/**
	 * The cached value of the '{@link #getVtl() <em>Vtl</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getVtl()
	 * @generated
	 * @ordered
	 */
	protected VTLGeneratedOutputlayer vtl;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected VTLForViewImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Ecore4regPackage.eINSTANCE.getVTLForView();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getName() {
		return name;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setName(String newName) {
		String oldName = name;
		name = newName;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore4regPackage.VTL_FOR_VIEW__NAME, oldName, name));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public RulesForReport getView() {
		if (view != null && view.eIsProxy()) {
			InternalEObject oldView = (InternalEObject)view;
			view = (RulesForReport)eResolveProxy(oldView);
			if (view != oldView) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Ecore4regPackage.VTL_FOR_VIEW__VIEW, oldView, view));
			}
		}
		return view;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public RulesForReport basicGetView() {
		return view;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setView(RulesForReport newView) {
		RulesForReport oldView = view;
		view = newView;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore4regPackage.VTL_FOR_VIEW__VIEW, oldView, view));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VTLGeneratedOutputlayer getVtl() {
		if (vtl != null && vtl.eIsProxy()) {
			InternalEObject oldVtl = (InternalEObject)vtl;
			vtl = (VTLGeneratedOutputlayer)eResolveProxy(oldVtl);
			if (vtl != oldVtl) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Ecore4regPackage.VTL_FOR_VIEW__VTL, oldVtl, vtl));
			}
		}
		return vtl;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VTLGeneratedOutputlayer basicGetVtl() {
		return vtl;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setVtl(VTLGeneratedOutputlayer newVtl) {
		VTLGeneratedOutputlayer oldVtl = vtl;
		vtl = newVtl;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore4regPackage.VTL_FOR_VIEW__VTL, oldVtl, vtl));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Ecore4regPackage.VTL_FOR_VIEW__NAME:
				return getName();
			case Ecore4regPackage.VTL_FOR_VIEW__VIEW:
				if (resolve) return getView();
				return basicGetView();
			case Ecore4regPackage.VTL_FOR_VIEW__VTL:
				if (resolve) return getVtl();
				return basicGetVtl();
		}
		return super.eGet(featureID, resolve, coreType);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void eSet(int featureID, Object newValue) {
		switch (featureID) {
			case Ecore4regPackage.VTL_FOR_VIEW__NAME:
				setName((String)newValue);
				return;
			case Ecore4regPackage.VTL_FOR_VIEW__VIEW:
				setView((RulesForReport)newValue);
				return;
			case Ecore4regPackage.VTL_FOR_VIEW__VTL:
				setVtl((VTLGeneratedOutputlayer)newValue);
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
			case Ecore4regPackage.VTL_FOR_VIEW__NAME:
				setName(NAME_EDEFAULT);
				return;
			case Ecore4regPackage.VTL_FOR_VIEW__VIEW:
				setView((RulesForReport)null);
				return;
			case Ecore4regPackage.VTL_FOR_VIEW__VTL:
				setVtl((VTLGeneratedOutputlayer)null);
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
			case Ecore4regPackage.VTL_FOR_VIEW__NAME:
				return NAME_EDEFAULT == null ? name != null : !NAME_EDEFAULT.equals(name);
			case Ecore4regPackage.VTL_FOR_VIEW__VIEW:
				return view != null;
			case Ecore4regPackage.VTL_FOR_VIEW__VTL:
				return vtl != null;
		}
		return super.eIsSet(featureID);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String toString() {
		if (eIsProxy()) return super.toString();

		StringBuilder result = new StringBuilder(super.toString());
		result.append(" (name: ");
		result.append(name);
		result.append(')');
		return result.toString();
	}

} //VTLForViewImpl
