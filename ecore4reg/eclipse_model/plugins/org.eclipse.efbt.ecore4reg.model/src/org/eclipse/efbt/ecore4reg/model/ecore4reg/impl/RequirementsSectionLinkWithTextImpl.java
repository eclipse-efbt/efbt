/**
 */
package org.eclipse.efbt.ecore4reg.model.ecore4reg.impl;

import org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.RequirementsSectionLinkWithText;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.TitledRequirementsSection;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.common.notify.NotificationChain;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Requirements Section Link With Text</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.RequirementsSectionLinkWithTextImpl#getLinkedRuleSection <em>Linked Rule Section</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.RequirementsSectionLinkWithTextImpl#getLinkText <em>Link Text</em>}</li>
 *   <li>{@link org.eclipse.efbt.ecore4reg.model.ecore4reg.impl.RequirementsSectionLinkWithTextImpl#getSubsection <em>Subsection</em>}</li>
 * </ul>
 *
 * @generated
 */
public class RequirementsSectionLinkWithTextImpl extends RequirementsSectionImpl implements RequirementsSectionLinkWithText {
	/**
	 * The cached value of the '{@link #getLinkedRuleSection() <em>Linked Rule Section</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getLinkedRuleSection()
	 * @generated
	 * @ordered
	 */
	protected TitledRequirementsSection linkedRuleSection;

	/**
	 * The default value of the '{@link #getLinkText() <em>Link Text</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getLinkText()
	 * @generated
	 * @ordered
	 */
	protected static final String LINK_TEXT_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getLinkText() <em>Link Text</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getLinkText()
	 * @generated
	 * @ordered
	 */
	protected String linkText = LINK_TEXT_EDEFAULT;

	/**
	 * The default value of the '{@link #getSubsection() <em>Subsection</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSubsection()
	 * @generated
	 * @ordered
	 */
	protected static final String SUBSECTION_EDEFAULT = null;

	/**
	 * The cached value of the '{@link #getSubsection() <em>Subsection</em>}' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getSubsection()
	 * @generated
	 * @ordered
	 */
	protected String subsection = SUBSECTION_EDEFAULT;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected RequirementsSectionLinkWithTextImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Ecore4regPackage.eINSTANCE.getRequirementsSectionLinkWithText();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TitledRequirementsSection getLinkedRuleSection() {
		if (linkedRuleSection != null && linkedRuleSection.eIsProxy()) {
			InternalEObject oldLinkedRuleSection = (InternalEObject)linkedRuleSection;
			linkedRuleSection = (TitledRequirementsSection)eResolveProxy(oldLinkedRuleSection);
			if (linkedRuleSection != oldLinkedRuleSection) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Ecore4regPackage.REQUIREMENTS_SECTION_LINK_WITH_TEXT__LINKED_RULE_SECTION, oldLinkedRuleSection, linkedRuleSection));
			}
		}
		return linkedRuleSection;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TitledRequirementsSection basicGetLinkedRuleSection() {
		return linkedRuleSection;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public NotificationChain basicSetLinkedRuleSection(TitledRequirementsSection newLinkedRuleSection, NotificationChain msgs) {
		TitledRequirementsSection oldLinkedRuleSection = linkedRuleSection;
		linkedRuleSection = newLinkedRuleSection;
		if (eNotificationRequired()) {
			ENotificationImpl notification = new ENotificationImpl(this, Notification.SET, Ecore4regPackage.REQUIREMENTS_SECTION_LINK_WITH_TEXT__LINKED_RULE_SECTION, oldLinkedRuleSection, newLinkedRuleSection);
			if (msgs == null) msgs = notification; else msgs.add(notification);
		}
		return msgs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setLinkedRuleSection(TitledRequirementsSection newLinkedRuleSection) {
		if (newLinkedRuleSection != linkedRuleSection) {
			NotificationChain msgs = null;
			if (linkedRuleSection != null)
				msgs = ((InternalEObject)linkedRuleSection).eInverseRemove(this, Ecore4regPackage.TITLED_REQUIREMENTS_SECTION__REFERENCING_SECTIONS, TitledRequirementsSection.class, msgs);
			if (newLinkedRuleSection != null)
				msgs = ((InternalEObject)newLinkedRuleSection).eInverseAdd(this, Ecore4regPackage.TITLED_REQUIREMENTS_SECTION__REFERENCING_SECTIONS, TitledRequirementsSection.class, msgs);
			msgs = basicSetLinkedRuleSection(newLinkedRuleSection, msgs);
			if (msgs != null) msgs.dispatch();
		}
		else if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore4regPackage.REQUIREMENTS_SECTION_LINK_WITH_TEXT__LINKED_RULE_SECTION, newLinkedRuleSection, newLinkedRuleSection));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getLinkText() {
		return linkText;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setLinkText(String newLinkText) {
		String oldLinkText = linkText;
		linkText = newLinkText;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore4regPackage.REQUIREMENTS_SECTION_LINK_WITH_TEXT__LINK_TEXT, oldLinkText, linkText));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String getSubsection() {
		return subsection;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setSubsection(String newSubsection) {
		String oldSubsection = subsection;
		subsection = newSubsection;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Ecore4regPackage.REQUIREMENTS_SECTION_LINK_WITH_TEXT__SUBSECTION, oldSubsection, subsection));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotificationChain eInverseAdd(InternalEObject otherEnd, int featureID, NotificationChain msgs) {
		switch (featureID) {
			case Ecore4regPackage.REQUIREMENTS_SECTION_LINK_WITH_TEXT__LINKED_RULE_SECTION:
				if (linkedRuleSection != null)
					msgs = ((InternalEObject)linkedRuleSection).eInverseRemove(this, Ecore4regPackage.TITLED_REQUIREMENTS_SECTION__REFERENCING_SECTIONS, TitledRequirementsSection.class, msgs);
				return basicSetLinkedRuleSection((TitledRequirementsSection)otherEnd, msgs);
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
			case Ecore4regPackage.REQUIREMENTS_SECTION_LINK_WITH_TEXT__LINKED_RULE_SECTION:
				return basicSetLinkedRuleSection(null, msgs);
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
			case Ecore4regPackage.REQUIREMENTS_SECTION_LINK_WITH_TEXT__LINKED_RULE_SECTION:
				if (resolve) return getLinkedRuleSection();
				return basicGetLinkedRuleSection();
			case Ecore4regPackage.REQUIREMENTS_SECTION_LINK_WITH_TEXT__LINK_TEXT:
				return getLinkText();
			case Ecore4regPackage.REQUIREMENTS_SECTION_LINK_WITH_TEXT__SUBSECTION:
				return getSubsection();
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
			case Ecore4regPackage.REQUIREMENTS_SECTION_LINK_WITH_TEXT__LINKED_RULE_SECTION:
				setLinkedRuleSection((TitledRequirementsSection)newValue);
				return;
			case Ecore4regPackage.REQUIREMENTS_SECTION_LINK_WITH_TEXT__LINK_TEXT:
				setLinkText((String)newValue);
				return;
			case Ecore4regPackage.REQUIREMENTS_SECTION_LINK_WITH_TEXT__SUBSECTION:
				setSubsection((String)newValue);
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
			case Ecore4regPackage.REQUIREMENTS_SECTION_LINK_WITH_TEXT__LINKED_RULE_SECTION:
				setLinkedRuleSection((TitledRequirementsSection)null);
				return;
			case Ecore4regPackage.REQUIREMENTS_SECTION_LINK_WITH_TEXT__LINK_TEXT:
				setLinkText(LINK_TEXT_EDEFAULT);
				return;
			case Ecore4regPackage.REQUIREMENTS_SECTION_LINK_WITH_TEXT__SUBSECTION:
				setSubsection(SUBSECTION_EDEFAULT);
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
			case Ecore4regPackage.REQUIREMENTS_SECTION_LINK_WITH_TEXT__LINKED_RULE_SECTION:
				return linkedRuleSection != null;
			case Ecore4regPackage.REQUIREMENTS_SECTION_LINK_WITH_TEXT__LINK_TEXT:
				return LINK_TEXT_EDEFAULT == null ? linkText != null : !LINK_TEXT_EDEFAULT.equals(linkText);
			case Ecore4regPackage.REQUIREMENTS_SECTION_LINK_WITH_TEXT__SUBSECTION:
				return SUBSECTION_EDEFAULT == null ? subsection != null : !SUBSECTION_EDEFAULT.equals(subsection);
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
		result.append(" (linkText: ");
		result.append(linkText);
		result.append(", subsection: ");
		result.append(subsection);
		result.append(')');
		return result.toString();
	}

} //RequirementsSectionLinkWithTextImpl
