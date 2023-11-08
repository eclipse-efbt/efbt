/**
 */
package org.eclipse.efbt.regdna.model.regdna.impl;

import java.util.Collection;

import org.eclipse.efbt.regdna.model.regdna.RequirementType;
import org.eclipse.efbt.regdna.model.regdna.RequirementsSection;
import org.eclipse.efbt.regdna.model.regdna.RequirementsSectionLinkWithText;
import org.eclipse.efbt.regdna.model.regdna.TitledRequirementsSection;
import org.eclipse.efbt.regdna.model.regdna.regdnaPackage;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

import org.eclipse.emf.ecore.util.EObjectContainmentEList;
import org.eclipse.emf.ecore.util.InternalEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Titled Requirements Section</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.impl.TitledRequirementsSectionImpl#getSections <em>Sections</em>}</li>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.impl.TitledRequirementsSectionImpl#getReferencingSections <em>Referencing Sections</em>}</li>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.impl.TitledRequirementsSectionImpl#getRequirementsType <em>Requirements Type</em>}</li>
 *   <li>{@link org.eclipse.efbt.regdna.model.regdna.impl.TitledRequirementsSectionImpl#getTitle <em>Title</em>}</li>
 * </ul>
 *
 * @generated
 */
public class TitledRequirementsSectionImpl extends RequirementsSectionImpl implements TitledRequirementsSection {
	/**
	 * The cached value of the '{@link #getSections() <em>Sections</em>}' containment reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSections()
	 * @generated
	 * @ordered
	 */
	protected EList<RequirementsSection> sections;

	/**
	 * The cached value of the '{@link #getReferencingSections() <em>Referencing Sections</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getReferencingSections()
	 * @generated
	 * @ordered
	 */
	protected RequirementsSectionLinkWithText referencingSections;

	/**
	 * The cached value of the '{@link #getRequirementsType() <em>Requirements Type</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getRequirementsType()
	 * @generated
	 * @ordered
	 */
	protected RequirementType requirementsType;

	/**
	 * The default value of the '{@link #getTitle() <em>Title</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTitle()
	 * @generated
	 * @ordered
	 */
	protected static final String TITLE_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getTitle() <em>Title</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getTitle()
	 * @generated
	 * @ordered
	 */
	protected String title = TITLE_EDEFAULT;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected TitledRequirementsSectionImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return regdnaPackage.eINSTANCE.getTitledRequirementsSection();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<RequirementsSection> getSections() {
		if (sections == null) {
			sections = new EObjectContainmentEList<RequirementsSection>(RequirementsSection.class, this, regdnaPackage.TITLED_REQUIREMENTS_SECTION__SECTIONS);
		}
		return sections;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public RequirementsSectionLinkWithText getReferencingSections() {
		if (referencingSections != null && referencingSections.eIsProxy()) {
			InternalEObject oldReferencingSections = (InternalEObject)referencingSections;
			referencingSections = (RequirementsSectionLinkWithText)eResolveProxy(oldReferencingSections);
			if (referencingSections != oldReferencingSections) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, regdnaPackage.TITLED_REQUIREMENTS_SECTION__REFERENCING_SECTIONS, oldReferencingSections, referencingSections));
			}
		}
		return referencingSections;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public RequirementsSectionLinkWithText basicGetReferencingSections() {
		return referencingSections;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetReferencingSections(RequirementsSectionLinkWithText newReferencingSections, NotificationChain msgs) {
		RequirementsSectionLinkWithText oldReferencingSections = referencingSections;
		referencingSections = newReferencingSections;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, regdnaPackage.TITLED_REQUIREMENTS_SECTION__REFERENCING_SECTIONS, oldReferencingSections, newReferencingSections);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setReferencingSections(RequirementsSectionLinkWithText newReferencingSections) {
		if (newReferencingSections != referencingSections) {
			NotificationChain msgs = null;
			if (referencingSections != null)
				msgs = ((InternalEObject)referencingSections).eInverseRemove(this, regdnaPackage.REQUIREMENTS_SECTION_LINK_WITH_TEXT__LINKED_RULE_SECTION, RequirementsSectionLinkWithText.class, msgs);
			if (newReferencingSections != null)
				msgs = ((InternalEObject)newReferencingSections).eInverseAdd(this, regdnaPackage.REQUIREMENTS_SECTION_LINK_WITH_TEXT__LINKED_RULE_SECTION, RequirementsSectionLinkWithText.class, msgs);
			msgs = basicSetReferencingSections(newReferencingSections, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, regdnaPackage.TITLED_REQUIREMENTS_SECTION__REFERENCING_SECTIONS, newReferencingSections, newReferencingSections));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public RequirementType getRequirementsType() {
		if (requirementsType != null && requirementsType.eIsProxy()) {
			InternalEObject oldRequirementsType = (InternalEObject)requirementsType;
			requirementsType = (RequirementType)eResolveProxy(oldRequirementsType);
			if (requirementsType != oldRequirementsType) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, regdnaPackage.TITLED_REQUIREMENTS_SECTION__REQUIREMENTS_TYPE, oldRequirementsType, requirementsType));
			}
		}
		return requirementsType;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public RequirementType basicGetRequirementsType() {
		return requirementsType;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setRequirementsType(RequirementType newRequirementsType) {
		RequirementType oldRequirementsType = requirementsType;
		requirementsType = newRequirementsType;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, regdnaPackage.TITLED_REQUIREMENTS_SECTION__REQUIREMENTS_TYPE, oldRequirementsType, requirementsType));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getTitle() {
		return title;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setTitle(String newTitle) {
		String oldTitle = title;
		title = newTitle;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, regdnaPackage.TITLED_REQUIREMENTS_SECTION__TITLE, oldTitle, title));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseAdd(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case regdnaPackage.TITLED_REQUIREMENTS_SECTION__REFERENCING_SECTIONS:
				if (referencingSections != null)
					msgs = ((InternalEObject)referencingSections).eInverseRemove(this, regdnaPackage.REQUIREMENTS_SECTION_LINK_WITH_TEXT__LINKED_RULE_SECTION, RequirementsSectionLinkWithText.class, msgs);
				return basicSetReferencingSections((RequirementsSectionLinkWithText)otherEnd, msgs);
		}
		return super.eInverseAdd(otherEnd, featureID, msgs);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseRemove(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case regdnaPackage.TITLED_REQUIREMENTS_SECTION__SECTIONS:
				return ((InternalEList<?>)getSections()).basicRemove(otherEnd, msgs);
			case regdnaPackage.TITLED_REQUIREMENTS_SECTION__REFERENCING_SECTIONS:
				return basicSetReferencingSections(null, msgs);
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
			case regdnaPackage.TITLED_REQUIREMENTS_SECTION__SECTIONS:
				return getSections();
			case regdnaPackage.TITLED_REQUIREMENTS_SECTION__REFERENCING_SECTIONS:
				if (resolve) return getReferencingSections();
				return basicGetReferencingSections();
			case regdnaPackage.TITLED_REQUIREMENTS_SECTION__REQUIREMENTS_TYPE:
				if (resolve) return getRequirementsType();
				return basicGetRequirementsType();
			case regdnaPackage.TITLED_REQUIREMENTS_SECTION__TITLE:
				return getTitle();
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
			case regdnaPackage.TITLED_REQUIREMENTS_SECTION__SECTIONS:
				getSections().clear();
				getSections().addAll((Collection<? extends RequirementsSection>)newValue);
				return;
			case regdnaPackage.TITLED_REQUIREMENTS_SECTION__REFERENCING_SECTIONS:
				setReferencingSections((RequirementsSectionLinkWithText)newValue);
				return;
			case regdnaPackage.TITLED_REQUIREMENTS_SECTION__REQUIREMENTS_TYPE:
				setRequirementsType((RequirementType)newValue);
				return;
			case regdnaPackage.TITLED_REQUIREMENTS_SECTION__TITLE:
				setTitle((String)newValue);
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
			case regdnaPackage.TITLED_REQUIREMENTS_SECTION__SECTIONS:
				getSections().clear();
				return;
			case regdnaPackage.TITLED_REQUIREMENTS_SECTION__REFERENCING_SECTIONS:
				setReferencingSections((RequirementsSectionLinkWithText)null);
				return;
			case regdnaPackage.TITLED_REQUIREMENTS_SECTION__REQUIREMENTS_TYPE:
				setRequirementsType((RequirementType)null);
				return;
			case regdnaPackage.TITLED_REQUIREMENTS_SECTION__TITLE:
				setTitle(TITLE_EDEFAULT);
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
			case regdnaPackage.TITLED_REQUIREMENTS_SECTION__SECTIONS:
				return sections != null && !sections.isEmpty();
			case regdnaPackage.TITLED_REQUIREMENTS_SECTION__REFERENCING_SECTIONS:
				return referencingSections != null;
			case regdnaPackage.TITLED_REQUIREMENTS_SECTION__REQUIREMENTS_TYPE:
				return requirementsType != null;
			case regdnaPackage.TITLED_REQUIREMENTS_SECTION__TITLE:
				return TITLE_EDEFAULT == null ? title != null : !TITLE_EDEFAULT.equals(title);
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
		result.append(" (title: ");
		result.append(title);
		result.append(')');
		return result.toString();
	}

} //TitledRequirementsSectionImpl
