/**
 */
package org.eclipse.efbt.controller.core.model.platform_call.provider;


import java.util.Collection;
import java.util.List;

import org.eclipse.efbt.controller.core.model.platform_call.Platform_callPackage;
import org.eclipse.efbt.controller.core.model.platform_call.TranslateBIRDWithOldTestFormatToCocason;

import org.eclipse.emf.common.notify.AdapterFactory;
import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.edit.provider.ComposeableAdapterFactory;
import org.eclipse.emf.edit.provider.IItemPropertyDescriptor;
import org.eclipse.emf.edit.provider.ItemPropertyDescriptor;
import org.eclipse.emf.edit.provider.ViewerNotification;

/**
 * This is the item provider adapter for a {@link org.eclipse.efbt.controller.core.model.platform_call.TranslateBIRDWithOldTestFormatToCocason} object.
 * <!-- begin-user-doc -->
 * <!-- end-user-doc -->
 * @generated
 */
public class TranslateBIRDWithOldTestFormatToCocasonItemProvider extends PlatformCallItemProvider {
	/**
	 * This constructs an instance from a factory and a notifier.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TranslateBIRDWithOldTestFormatToCocasonItemProvider(AdapterFactory adapterFactory) {
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

			addInputDirectoryPropertyDescriptor(object);
			addOutputDirectoryPropertyDescriptor(object);
			addTestDirectoryPropertyDescriptor(object);
		}
		return itemPropertyDescriptors;
	}

	/**
	 * This adds a property descriptor for the Input Directory feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addInputDirectoryPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_TranslateBIRDWithOldTestFormatToCocason_inputDirectory_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_TranslateBIRDWithOldTestFormatToCocason_inputDirectory_feature", "_UI_TranslateBIRDWithOldTestFormatToCocason_type"),
				 Platform_callPackage.Literals.TRANSLATE_BIRD_WITH_OLD_TEST_FORMAT_TO_COCASON__INPUT_DIRECTORY,
				 true,
				 false,
				 false,
				 ItemPropertyDescriptor.GENERIC_VALUE_IMAGE,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Output Directory feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addOutputDirectoryPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_TranslateBIRDWithOldTestFormatToCocason_outputDirectory_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_TranslateBIRDWithOldTestFormatToCocason_outputDirectory_feature", "_UI_TranslateBIRDWithOldTestFormatToCocason_type"),
				 Platform_callPackage.Literals.TRANSLATE_BIRD_WITH_OLD_TEST_FORMAT_TO_COCASON__OUTPUT_DIRECTORY,
				 true,
				 false,
				 false,
				 ItemPropertyDescriptor.GENERIC_VALUE_IMAGE,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Test Directory feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addTestDirectoryPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_TranslateBIRDWithOldTestFormatToCocason_testDirectory_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_TranslateBIRDWithOldTestFormatToCocason_testDirectory_feature", "_UI_TranslateBIRDWithOldTestFormatToCocason_type"),
				 Platform_callPackage.Literals.TRANSLATE_BIRD_WITH_OLD_TEST_FORMAT_TO_COCASON__TEST_DIRECTORY,
				 true,
				 false,
				 false,
				 ItemPropertyDescriptor.GENERIC_VALUE_IMAGE,
				 null,
				 null));
	}

	/**
	 * This returns TranslateBIRDWithOldTestFormatToCocason.gif.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object getImage(Object object) {
		return overlayImage(object, getResourceLocator().getImage("full/obj16/TranslateBIRDWithOldTestFormatToCocason"));
	}

	/**
	 * This returns the label text for the adapted class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getText(Object object) {
		String label = ((TranslateBIRDWithOldTestFormatToCocason)object).getName();
		return label == null || label.length() == 0 ?
			getString("_UI_TranslateBIRDWithOldTestFormatToCocason_type") :
			getString("_UI_TranslateBIRDWithOldTestFormatToCocason_type") + " " + label;
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

		switch (notification.getFeatureID(TranslateBIRDWithOldTestFormatToCocason.class)) {
			case Platform_callPackage.TRANSLATE_BIRD_WITH_OLD_TEST_FORMAT_TO_COCASON__INPUT_DIRECTORY:
			case Platform_callPackage.TRANSLATE_BIRD_WITH_OLD_TEST_FORMAT_TO_COCASON__OUTPUT_DIRECTORY:
			case Platform_callPackage.TRANSLATE_BIRD_WITH_OLD_TEST_FORMAT_TO_COCASON__TEST_DIRECTORY:
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
