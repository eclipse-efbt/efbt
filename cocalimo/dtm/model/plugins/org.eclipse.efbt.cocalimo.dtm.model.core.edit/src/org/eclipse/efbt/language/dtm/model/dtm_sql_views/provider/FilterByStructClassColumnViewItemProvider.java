/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_sql_views.provider;


import java.util.Collection;
import java.util.List;

import org.eclipse.efbt.language.dtm.model.dtm_sql_views.FilterByStructClassColumnView;
import org.eclipse.efbt.language.dtm.model.dtm_sql_views.dtm_sql_viewsPackage;

import org.eclipse.emf.common.notify.AdapterFactory;
import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.edit.provider.ComposeableAdapterFactory;
import org.eclipse.emf.edit.provider.IItemPropertyDescriptor;

/**
 * This is the item provider adapter for a {@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.FilterByStructClassColumnView} object.
 * <!-- begin-user-doc -->
 * <!-- end-user-doc -->
 * @generated
 */
public class FilterByStructClassColumnViewItemProvider extends SQLViewItemProvider {
	/**
	 * This constructs an instance from a factory and a notifier.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public FilterByStructClassColumnViewItemProvider(AdapterFactory adapterFactory) {
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

			addRenameAsStructColumnPropertyDescriptor(object);
			addStructClassPropertyDescriptor(object);
			addStructColumnPropertyDescriptor(object);
		}
		return itemPropertyDescriptors;
	}

	/**
	 * This adds a property descriptor for the Rename As Struct Column feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addRenameAsStructColumnPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_FilterByStructClassColumnView_renameAsStructColumn_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_FilterByStructClassColumnView_renameAsStructColumn_feature", "_UI_FilterByStructClassColumnView_type"),
				 dtm_sql_viewsPackage.Literals.FILTER_BY_STRUCT_CLASS_COLUMN_VIEW__RENAME_AS_STRUCT_COLUMN,
				 true,
				 false,
				 true,
				 null,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Struct Class feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addStructClassPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_FilterByStructClassColumnView_structClass_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_FilterByStructClassColumnView_structClass_feature", "_UI_FilterByStructClassColumnView_type"),
				 dtm_sql_viewsPackage.Literals.FILTER_BY_STRUCT_CLASS_COLUMN_VIEW__STRUCT_CLASS,
				 true,
				 false,
				 true,
				 null,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Struct Column feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addStructColumnPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_FilterByStructClassColumnView_structColumn_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_FilterByStructClassColumnView_structColumn_feature", "_UI_FilterByStructClassColumnView_type"),
				 dtm_sql_viewsPackage.Literals.FILTER_BY_STRUCT_CLASS_COLUMN_VIEW__STRUCT_COLUMN,
				 true,
				 false,
				 true,
				 null,
				 null,
				 null));
	}

	/**
	 * This returns FilterByStructClassColumnView.gif.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object getImage(Object object) {
		return overlayImage(object, getResourceLocator().getImage("full/obj16/FilterByStructClassColumnView"));
	}

	/**
	 * This returns the label text for the adapted class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getText(Object object) {
		String label = ((FilterByStructClassColumnView)object).getName();
		return label == null || label.length() == 0 ?
			getString("_UI_FilterByStructClassColumnView_type") :
			getString("_UI_FilterByStructClassColumnView_type") + " " + label;
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
