/**
 */
package org.eclipse.efbt.cocalimo.core.model.test_definition.provider;


import java.util.Collection;
import java.util.List;

import org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestTemplate;
import org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionFactory;
import org.eclipse.efbt.cocalimo.core.model.test_definition.Test_definitionPackage;

import org.eclipse.emf.common.notify.AdapterFactory;
import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.common.util.ResourceLocator;

import org.eclipse.emf.ecore.EStructuralFeature;

import org.eclipse.emf.edit.provider.ComposeableAdapterFactory;
import org.eclipse.emf.edit.provider.IEditingDomainItemProvider;
import org.eclipse.emf.edit.provider.IItemLabelProvider;
import org.eclipse.emf.edit.provider.IItemPropertyDescriptor;
import org.eclipse.emf.edit.provider.IItemPropertySource;
import org.eclipse.emf.edit.provider.IStructuredItemContentProvider;
import org.eclipse.emf.edit.provider.ITreeItemContentProvider;
import org.eclipse.emf.edit.provider.ItemPropertyDescriptor;
import org.eclipse.emf.edit.provider.ItemProviderAdapter;
import org.eclipse.emf.edit.provider.ViewerNotification;

/**
 * This is the item provider adapter for a {@link org.eclipse.efbt.cocalimo.core.model.test_definition.BDDTestTemplate} object.
 * <!-- begin-user-doc -->
 * <!-- end-user-doc -->
 * @generated
 */
public class BDDTestTemplateItemProvider 
	extends ItemProviderAdapter
	implements
		IEditingDomainItemProvider,
		IStructuredItemContentProvider,
		ITreeItemContentProvider,
		IItemLabelProvider,
		IItemPropertySource {
	/**
	 * This constructs an instance from a factory and a notifier.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public BDDTestTemplateItemProvider(AdapterFactory adapterFactory) {
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

			addNamePropertyDescriptor(object);
		}
		return itemPropertyDescriptors;
	}

	/**
	 * This adds a property descriptor for the Name feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addNamePropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_BDDTestTemplate_name_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_BDDTestTemplate_name_feature", "_UI_BDDTestTemplate_type"),
				 Test_definitionPackage.Literals.BDD_TEST_TEMPLATE__NAME,
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
			childrenFeatures.add(Test_definitionPackage.Literals.BDD_TEST_TEMPLATE__GIVEN_TEXT);
			childrenFeatures.add(Test_definitionPackage.Literals.BDD_TEST_TEMPLATE__WHEN_TEXT);
			childrenFeatures.add(Test_definitionPackage.Literals.BDD_TEST_TEMPLATE__THEN_TEXT);
			childrenFeatures.add(Test_definitionPackage.Literals.BDD_TEST_TEMPLATE__GIVEN_PARAMS);
			childrenFeatures.add(Test_definitionPackage.Literals.BDD_TEST_TEMPLATE__WHEN_PARAMS);
			childrenFeatures.add(Test_definitionPackage.Literals.BDD_TEST_TEMPLATE__THEN_PARAMS);
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
	 * This returns BDDTestTemplate.gif.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object getImage(Object object) {
		return overlayImage(object, getResourceLocator().getImage("full/obj16/BDDTestTemplate"));
	}

	/**
	 * This returns the label text for the adapted class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getText(Object object) {
		String label = ((BDDTestTemplate)object).getName();
		return label == null || label.length() == 0 ?
			getString("_UI_BDDTestTemplate_type") :
			getString("_UI_BDDTestTemplate_type") + " " + label;
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

		switch (notification.getFeatureID(BDDTestTemplate.class)) {
			case Test_definitionPackage.BDD_TEST_TEMPLATE__NAME:
				fireNotifyChanged(new ViewerNotification(notification, notification.getNotifier(), false, true));
				return;
			case Test_definitionPackage.BDD_TEST_TEMPLATE__GIVEN_TEXT:
			case Test_definitionPackage.BDD_TEST_TEMPLATE__WHEN_TEXT:
			case Test_definitionPackage.BDD_TEST_TEMPLATE__THEN_TEXT:
			case Test_definitionPackage.BDD_TEST_TEMPLATE__GIVEN_PARAMS:
			case Test_definitionPackage.BDD_TEST_TEMPLATE__WHEN_PARAMS:
			case Test_definitionPackage.BDD_TEST_TEMPLATE__THEN_PARAMS:
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
				(Test_definitionPackage.Literals.BDD_TEST_TEMPLATE__GIVEN_TEXT,
				 Test_definitionFactory.eINSTANCE.createClauseText()));

		newChildDescriptors.add
			(createChildParameter
				(Test_definitionPackage.Literals.BDD_TEST_TEMPLATE__WHEN_TEXT,
				 Test_definitionFactory.eINSTANCE.createClauseText()));

		newChildDescriptors.add
			(createChildParameter
				(Test_definitionPackage.Literals.BDD_TEST_TEMPLATE__THEN_TEXT,
				 Test_definitionFactory.eINSTANCE.createClauseText()));

		newChildDescriptors.add
			(createChildParameter
				(Test_definitionPackage.Literals.BDD_TEST_TEMPLATE__GIVEN_PARAMS,
				 Test_definitionFactory.eINSTANCE.createParam()));

		newChildDescriptors.add
			(createChildParameter
				(Test_definitionPackage.Literals.BDD_TEST_TEMPLATE__WHEN_PARAMS,
				 Test_definitionFactory.eINSTANCE.createParam()));

		newChildDescriptors.add
			(createChildParameter
				(Test_definitionPackage.Literals.BDD_TEST_TEMPLATE__THEN_PARAMS,
				 Test_definitionFactory.eINSTANCE.createParam()));
	}

	/**
	 * This returns the label text for {@link org.eclipse.emf.edit.command.CreateChildCommand}.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getCreateChildText(Object owner, Object feature, Object child, Collection<?> selection) {
		Object childFeature = feature;
		Object childObject = child;

		boolean qualify =
			childFeature == Test_definitionPackage.Literals.BDD_TEST_TEMPLATE__GIVEN_TEXT ||
			childFeature == Test_definitionPackage.Literals.BDD_TEST_TEMPLATE__WHEN_TEXT ||
			childFeature == Test_definitionPackage.Literals.BDD_TEST_TEMPLATE__THEN_TEXT ||
			childFeature == Test_definitionPackage.Literals.BDD_TEST_TEMPLATE__GIVEN_PARAMS ||
			childFeature == Test_definitionPackage.Literals.BDD_TEST_TEMPLATE__WHEN_PARAMS ||
			childFeature == Test_definitionPackage.Literals.BDD_TEST_TEMPLATE__THEN_PARAMS;

		if (qualify) {
			return getString
				("_UI_CreateChild_text2",
				 new Object[] { getTypeText(childObject), getFeatureText(childFeature), getTypeText(owner) });
		}
		return super.getCreateChildText(owner, feature, child, selection);
	}

	/**
	 * Return the resource locator for this item provider's resources.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ResourceLocator getResourceLocator() {
		return TestingEditPlugin.INSTANCE;
	}

}
