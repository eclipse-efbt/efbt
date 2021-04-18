/**
 */
package org.eclipse.efbt.cocalimo.core.model.requirements_text.provider;


import java.util.Collection;
import java.util.List;

import org.eclipse.efbt.cocalimo.core.model.requirements_text.Requirements_textFactory;
import org.eclipse.efbt.cocalimo.core.model.requirements_text.Requirements_textPackage;
import org.eclipse.efbt.cocalimo.core.model.requirements_text.TitledRequirementsSection;

import org.eclipse.emf.common.notify.AdapterFactory;
import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EStructuralFeature;

import org.eclipse.emf.edit.provider.ComposeableAdapterFactory;
import org.eclipse.emf.edit.provider.IItemPropertyDescriptor;
import org.eclipse.emf.edit.provider.ItemPropertyDescriptor;
import org.eclipse.emf.edit.provider.ViewerNotification;

/**
 * This is the item provider adapter for a {@link org.eclipse.efbt.cocalimo.core.model.requirements_text.TitledRequirementsSection} object.
 * <!-- begin-user-doc -->
 * <!-- end-user-doc -->
 * @generated
 */
public class TitledRequirementsSectionItemProvider extends RequirementsSectionItemProvider {
	/**
	 * This constructs an instance from a factory and a notifier.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TitledRequirementsSectionItemProvider(AdapterFactory adapterFactory) {
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

			addReferencingSectionsPropertyDescriptor(object);
			addRequirementsTypePropertyDescriptor(object);
			addTitlePropertyDescriptor(object);
		}
		return itemPropertyDescriptors;
	}

	/**
	 * This adds a property descriptor for the Referencing Sections feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addReferencingSectionsPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_TitledRequirementsSection_referencingSections_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_TitledRequirementsSection_referencingSections_feature", "_UI_TitledRequirementsSection_type"),
				 Requirements_textPackage.Literals.TITLED_REQUIREMENTS_SECTION__REFERENCING_SECTIONS,
				 true,
				 false,
				 true,
				 null,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Requirements Type feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addRequirementsTypePropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_TitledRequirementsSection_requirementsType_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_TitledRequirementsSection_requirementsType_feature", "_UI_TitledRequirementsSection_type"),
				 Requirements_textPackage.Literals.TITLED_REQUIREMENTS_SECTION__REQUIREMENTS_TYPE,
				 true,
				 false,
				 true,
				 null,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Title feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addTitlePropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_TitledRequirementsSection_title_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_TitledRequirementsSection_title_feature", "_UI_TitledRequirementsSection_type"),
				 Requirements_textPackage.Literals.TITLED_REQUIREMENTS_SECTION__TITLE,
				 true,
				 false,
				 false,
				 ItemPropertyDescriptor.GENERIC_VALUE_IMAGE,
				 null,
				 null));
	}

	/**
	 * This specifies how to implement {@link #getChildren} and is used to deduce an appropriate feature for an
	 * {@link org.eclipse.emf.edit.command.AddCommand}, {@link org.eclipse.emf.edit.command.RemoveCommand} or
	 * {@link org.eclipse.emf.edit.command.MoveCommand} in {@link #createCommand}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Collection<? extends EStructuralFeature> getChildrenFeatures(Object object) {
		if (childrenFeatures == null) {
			super.getChildrenFeatures(object);
			childrenFeatures.add(Requirements_textPackage.Literals.TITLED_REQUIREMENTS_SECTION__SECTIONS);
		}
		return childrenFeatures;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EStructuralFeature getChildFeature(Object object, Object child) {
		// Check the type of the specified child object and return the proper feature to use for
		// adding (see {@link AddCommand}) it as a child.

		return super.getChildFeature(object, child);
	}

	/**
	 * This returns TitledRequirementsSection.gif.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object getImage(Object object) {
		return overlayImage(object, getResourceLocator().getImage("full/obj16/TitledRequirementsSection"));
	}

	/**
	 * This returns the label text for the adapted class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getText(Object object) {
		String label = ((TitledRequirementsSection)object).getName();
		return label == null || label.length() == 0 ?
			getString("_UI_TitledRequirementsSection_type") :
			getString("_UI_TitledRequirementsSection_type") + " " + label;
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

		switch (notification.getFeatureID(TitledRequirementsSection.class)) {
			case Requirements_textPackage.TITLED_REQUIREMENTS_SECTION__TITLE:
				fireNotifyChanged(new ViewerNotification(notification, notification.getNotifier(), false, true));
				return;
			case Requirements_textPackage.TITLED_REQUIREMENTS_SECTION__SECTIONS:
				fireNotifyChanged(new ViewerNotification(notification, notification.getNotifier(), true, false));
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

		newChildDescriptors.add
			(createChildParameter
				(Requirements_textPackage.Literals.TITLED_REQUIREMENTS_SECTION__SECTIONS,
				 Requirements_textFactory.eINSTANCE.createRequirementsSectionImage()));

		newChildDescriptors.add
			(createChildParameter
				(Requirements_textPackage.Literals.TITLED_REQUIREMENTS_SECTION__SECTIONS,
				 Requirements_textFactory.eINSTANCE.createRequirementsSectionLinkWithText()));

		newChildDescriptors.add
			(createChildParameter
				(Requirements_textPackage.Literals.TITLED_REQUIREMENTS_SECTION__SECTIONS,
				 Requirements_textFactory.eINSTANCE.createRequirementsSectionText()));

		newChildDescriptors.add
			(createChildParameter
				(Requirements_textPackage.Literals.TITLED_REQUIREMENTS_SECTION__SECTIONS,
				 Requirements_textFactory.eINSTANCE.createTitledRequirementsSection()));
	}

}
