/**
 */
package org.eclipse.efbt.cocalimo.core.model.test_definition.impl;

import java.util.Collection;

import org.eclipse.efbt.cocalimo.core.model.requirements_text.TitledRequirementsSection;

import org.eclipse.efbt.cocalimo.core.model.test_definition.Clause;
import org.eclipse.efbt.cocalimo.core.model.test_definition.ClauseText;
import org.eclipse.efbt.cocalimo.core.model.test_definition.Param;
import org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionPackage;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.EObjectResolvingEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Clause</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.ClauseImpl#getText <em>Text</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.ClauseImpl#getParams <em>Params</em>}</li>
 *   <li>{@link org.eclipse.efbt.cocalimo.core.model.test_definition.impl.ClauseImpl#getRegulatoryTextLinks <em>Regulatory Text Links</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ClauseImpl extends MinimalEObjectImpl.Container implements Clause
{
	/**
	 * The cached value of the '{@link #getText() <em>Text</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getText()
	 * @generated
	 * @ordered
	 */
	protected ClauseText text;

	/**
	 * The cached value of the '{@link #getParams() <em>Params</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getParams()
	 * @generated
	 * @ordered
	 */
	protected EList<Param> params;

	/**
	 * The cached value of the '{@link #getRegulatoryTextLinks() <em>Regulatory Text Links</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getRegulatoryTextLinks()
	 * @generated
	 * @ordered
	 */
	protected EList<TitledRequirementsSection> regulatoryTextLinks;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ClauseImpl()
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
		return Test_definitionPackage.Literals.CLAUSE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ClauseText getText()
	{
		if (text != null && text.eIsProxy()) {
			InternalEObject oldText = (InternalEObject)text;
			text = (ClauseText)eResolveProxy(oldText);
			if (text != oldText) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Test_definitionPackage.CLAUSE__TEXT, oldText, text));
			}
		}
		return text;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ClauseText basicGetText()
	{
		return text;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setText(ClauseText newText)
	{
		ClauseText oldText = text;
		text = newText;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Test_definitionPackage.CLAUSE__TEXT, oldText, text));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<Param> getParams()
	{
		if (params == null) {
			params = new EObjectContainmentEList<Param>(Param.class, this, Test_definitionPackage.CLAUSE__PARAMS);
		}
		return params;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<TitledRequirementsSection> getRegulatoryTextLinks()
	{
		if (regulatoryTextLinks == null) {
			regulatoryTextLinks = new EObjectResolvingEList<TitledRequirementsSection>(TitledRequirementsSection.class, this, Test_definitionPackage.CLAUSE__REGULATORY_TEXT_LINKS);
		}
		return regulatoryTextLinks;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs)
	{
		switch (featureID) {
			case Test_definitionPackage.CLAUSE__PARAMS:
				return ((InternalEList<?>)getParams()).basicRemove(otherEnd, msgs);
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
		switch (featureID) {
			case Test_definitionPackage.CLAUSE__TEXT:
				if (resolve) return getText();
				return basicGetText();
			case Test_definitionPackage.CLAUSE__PARAMS:
				return getParams();
			case Test_definitionPackage.CLAUSE__REGULATORY_TEXT_LINKS:
				return getRegulatoryTextLinks();
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
		switch (featureID) {
			case Test_definitionPackage.CLAUSE__TEXT:
				setText((ClauseText)newValue);
				return;
			case Test_definitionPackage.CLAUSE__PARAMS:
				getParams().clear();
				getParams().addAll((Collection<? extends Param>)newValue);
				return;
			case Test_definitionPackage.CLAUSE__REGULATORY_TEXT_LINKS:
				getRegulatoryTextLinks().clear();
				getRegulatoryTextLinks().addAll((Collection<? extends TitledRequirementsSection>)newValue);
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
		switch (featureID) {
			case Test_definitionPackage.CLAUSE__TEXT:
				setText((ClauseText)null);
				return;
			case Test_definitionPackage.CLAUSE__PARAMS:
				getParams().clear();
				return;
			case Test_definitionPackage.CLAUSE__REGULATORY_TEXT_LINKS:
				getRegulatoryTextLinks().clear();
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
		switch (featureID) {
			case Test_definitionPackage.CLAUSE__TEXT:
				return text != null;
			case Test_definitionPackage.CLAUSE__PARAMS:
				return params != null && !params.isEmpty();
			case Test_definitionPackage.CLAUSE__REGULATORY_TEXT_LINKS:
				return regulatoryTextLinks != null && !regulatoryTextLinks.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //ClauseImpl
