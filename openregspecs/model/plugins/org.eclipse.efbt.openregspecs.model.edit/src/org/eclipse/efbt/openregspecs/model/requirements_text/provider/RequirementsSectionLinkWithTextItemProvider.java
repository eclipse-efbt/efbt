/**
 */
package org.eclipse.efbt.openregspecs.model.requirements_text.provider;


import java.util.Collection;
import java.util.List;

import org.eclipse.efbt.openregspecs.model.requirements_text.RequirementsSectionLinkWithText;
import org.eclipse.efbt.openregspecs.model.requirements_text.Requirements_textPackage;

import org.eclipse.emf.common.notify.AdapterFactory;
import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.edit.provider.ComposeableAdapterFactory;
import org.eclipse.emf.edit.provider.IItemPropertyDescriptor;
import org.eclipse.emf.edit.provider.ItemPropertyDescriptor;
import org.eclipse.emf.edit.provider.ViewerNotification;

/**
 * This is the item provider adapter for a {@link org.eclipse.efbt.openregspecs.model.requirements_text.RequirementsSectionLinkWithText} object.
 * <!-- begin-user-doc -->
 * <!-- end-user-doc -->
 * @generated
 */
public class RequirementsSectionLinkWithTextItemProvider extends RequirementsSectionItemProvider {
	/**
	 * This constructs an instance from a factory and a notifier.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public RequirementsSectionLinkWithTextItemProvider(AdapterFactory adapterFactory) {
		super(adapterFactory);
	}

	/**
	 * This returns the property descriptors for the adapted class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public List<IItemPropertyDescriptor> getPropertyDescriptors(Object object) {
		if (itemPropertyDescriptors == null) {
			super.getPropertyDescriptors(object);

			addLinkedRuleSectionPropertyDescriptor(object);
			addLinkTextPropertyDescriptor(object);
			addSubsectionPropertyDescriptor(object);
		}
		return itemPropertyDescriptors;
	}

	/**
	 * This adds a property descriptor for the Linked Rule Section feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addLinkedRuleSectionPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_RequirementsSectionLinkWithText_linkedRuleSection_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_RequirementsSectionLinkWithText_linkedRuleSection_feature", "_UI_RequirementsSectionLinkWithText_type"),
				 Requirements_textPackage.Literals.REQUIREMENTS_SECTION_LINK_WITH_TEXT__LINKED_RULE_SECTION,
				 true,
				 false,
				 true,
				 null,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Link Text feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addLinkTextPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_RequirementsSectionLinkWithText_linkText_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_RequirementsSectionLinkWithText_linkText_feature", "_UI_RequirementsSectionLinkWithText_type"),
				 Requirements_textPackage.Literals.REQUIREMENTS_SECTION_LINK_WITH_TEXT__LINK_TEXT,
				 true,
				 false,
				 false,
				 ItemPropertyDescriptor.GENERIC_VALUE_IMAGE,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Subsection feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addSubsectionPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_RequirementsSectionLinkWithText_subsection_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_RequirementsSectionLinkWithText_subsection_feature", "_UI_RequirementsSectionLinkWithText_type"),
				 Requirements_textPackage.Literals.REQUIREMENTS_SECTION_LINK_WITH_TEXT__SUBSECTION,
				 true,
				 false,
				 false,
				 ItemPropertyDescriptor.GENERIC_VALUE_IMAGE,
				 null,
				 null));
	}

	/**
	 * This returns RequirementsSectionLinkWithText.gif.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object getImage(Object object) {
		return overlayImage(object, getResourceLocator().getImage("full/obj16/RequirementsSectionLinkWithText"));
	}

	/**
	 * This returns the label text for the adapted class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getText(Object object) {
		String label = ((RequirementsSectionLinkWithText)object).getName();
		return label == null || label.length() == 0 ?
			getString("_UI_RequirementsSectionLinkWithText_type") :
			getString("_UI_RequirementsSectionLinkWithText_type") + " " + label;
	}


	/**
	 * This handles model notifications by calling {@link #updateChildren} to update any cached
	 * children and by creating a viewer notification, which it passes to {@link #fireNotifyChanged}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void notifyChanged(Notification notification) {
		updateChildren(notification);

		switch (notification.getFeatureID(RequirementsSectionLinkWithText.class)) {
			case Requirements_textPackage.REQUIREMENTS_SECTION_LINK_WITH_TEXT__LINK_TEXT:
			case Requirements_textPackage.REQUIREMENTS_SECTION_LINK_WITH_TEXT__SUBSECTION:
				fireNotifyChanged(new ViewerNotification(notification, notification.getNotifier(), false, true));
				return;
		}
		super.notifyChanged(notification);
	}

	/**
	 * This adds {@link org.eclipse.emf.edit.command.CommandParameter}s describing the children
	 * that can be created under this object.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected void collectNewChildDescriptors(Collection<Object> newChildDescriptors, Object object) {
		super.collectNewChildDescriptors(newChildDescriptors, object);
	}

}
