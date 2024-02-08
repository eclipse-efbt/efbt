/**
 */
package org.eclipse.efbt.regdna.model.regdna.impl;

import org.eclipse.efbt.regdna.model.regdna.ELAnnotationDirective;
import org.eclipse.efbt.regdna.model.regdna.regdnaPackage;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>EL Annotation Directive</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.impl.ELAnnotationDirectiveImpl#getModule <em>Module</em>}</li>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.impl.ELAnnotationDirectiveImpl#getSourceURI <em>Source URI</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ELAnnotationDirectiveImpl extends ELNamedElementImpl implements ELAnnotationDirective {
	/**
	 * The cached value of the '{@link #getModule() <em>Module</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getModule()
	 * @generated
	 * @ordered
	 */
	protected org.eclipse.efbt.regdna.model.regdna.Module module;

	/**
	 * The default value of the '{@link #getSourceURI() <em>Source URI</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSourceURI()
	 * @generated
	 * @ordered
	 */
	protected static final String SOURCE_URI_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getSourceURI() <em>Source URI</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSourceURI()
	 * @generated
	 * @ordered
	 */
	protected String sourceURI = SOURCE_URI_EDEFAULT;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ELAnnotationDirectiveImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return regdnaPackage.eINSTANCE.getELAnnotationDirective();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public org.eclipse.efbt.regdna.model.regdna.Module getModule() {
		if (module != null && module.eIsProxy()) {
			InternalEObject oldModule = (InternalEObject)module;
			module = (org.eclipse.efbt.regdna.model.regdna.Module)eResolveProxy(oldModule);
			if (module != oldModule) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, regdnaPackage.EL_ANNOTATION_DIRECTIVE__MODULE, oldModule, module));
			}
		}
		return module;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public org.eclipse.efbt.regdna.model.regdna.Module basicGetModule() {
		return module;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setModule(org.eclipse.efbt.regdna.model.regdna.Module newModule) {
		org.eclipse.efbt.regdna.model.regdna.Module oldModule = module;
		module = newModule;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, regdnaPackage.EL_ANNOTATION_DIRECTIVE__MODULE, oldModule, module));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getSourceURI() {
		return sourceURI;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setSourceURI(String newSourceURI) {
		String oldSourceURI = sourceURI;
		sourceURI = newSourceURI;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, regdnaPackage.EL_ANNOTATION_DIRECTIVE__SOURCE_URI, oldSourceURI, sourceURI));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case regdnaPackage.EL_ANNOTATION_DIRECTIVE__MODULE:
				if (resolve) return getModule();
				return basicGetModule();
			case regdnaPackage.EL_ANNOTATION_DIRECTIVE__SOURCE_URI:
				return getSourceURI();
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
			case regdnaPackage.EL_ANNOTATION_DIRECTIVE__MODULE:
				setModule((org.eclipse.efbt.regdna.model.regdna.Module)newValue);
				return;
			case regdnaPackage.EL_ANNOTATION_DIRECTIVE__SOURCE_URI:
				setSourceURI((String)newValue);
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
			case regdnaPackage.EL_ANNOTATION_DIRECTIVE__MODULE:
				setModule((org.eclipse.efbt.regdna.model.regdna.Module)null);
				return;
			case regdnaPackage.EL_ANNOTATION_DIRECTIVE__SOURCE_URI:
				setSourceURI(SOURCE_URI_EDEFAULT);
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
			case regdnaPackage.EL_ANNOTATION_DIRECTIVE__MODULE:
				return module != null;
			case regdnaPackage.EL_ANNOTATION_DIRECTIVE__SOURCE_URI:
				return SOURCE_URI_EDEFAULT == null ? sourceURI != null : !SOURCE_URI_EDEFAULT.equals(sourceURI);
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
		result.append(" (sourceURI: ");
		result.append(sourceURI);
		result.append(')');
		return result.toString();
	}

} //ELAnnotationDirectiveImpl
