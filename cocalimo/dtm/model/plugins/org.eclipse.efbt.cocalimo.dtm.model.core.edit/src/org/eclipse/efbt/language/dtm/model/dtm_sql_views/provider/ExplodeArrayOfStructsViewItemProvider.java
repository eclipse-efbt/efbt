/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_sql_views.provider;


import java.util.Collection;
import java.util.List;

import org.eclipse.efbt.language.dtm.model.dtm_sql_views.ExplodeArrayOfStructsView;
import org.eclipse.efbt.language.dtm.model.dtm_sql_views.dtm_sql_viewsPackage;

import org.eclipse.emf.common.notify.AdapterFactory;
import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.edit.provider.ComposeableAdapterFactory;
import org.eclipse.emf.edit.provider.IItemPropertyDescriptor;

/**
 * This is the item provider adapter for a {@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.ExplodeArrayOfStructsView} object.
 * <!-- begin-user-doc -->
 * <!-- end-user-doc -->
 * @generated
 */
public class ExplodeArrayOfStructsViewItemProvider extends SQLViewItemProvider {
	/**
	 * This constructs an instance from a factory and a notifier.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ExplodeArrayOfStructsViewItemProvider(AdapterFactory adapterFactory) {
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

			addArraySourceColumnPropertyDescriptor(object);
			addIndexColumnPropertyDescriptor(object);
			addTargetStructColumnPropertyDescriptor(object);
		}
		return itemPropertyDescriptors;
	}

	/**
	 * This adds a property descriptor for the Array Source Column feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addArraySourceColumnPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_ExplodeArrayOfStructsView_arraySourceColumn_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_ExplodeArrayOfStructsView_arraySourceColumn_feature", "_UI_ExplodeArrayOfStructsView_type"),
				 dtm_sql_viewsPackage.Literals.EXPLODE_ARRAY_OF_STRUCTS_VIEW__ARRAY_SOURCE_COLUMN,
				 true,
				 false,
				 true,
				 null,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Index Column feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addIndexColumnPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_ExplodeArrayOfStructsView_indexColumn_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_ExplodeArrayOfStructsView_indexColumn_feature", "_UI_ExplodeArrayOfStructsView_type"),
				 dtm_sql_viewsPackage.Literals.EXPLODE_ARRAY_OF_STRUCTS_VIEW__INDEX_COLUMN,
				 true,
				 false,
				 true,
				 null,
				 null,
				 null));
	}

	/**
	 * This adds a property descriptor for the Target Struct Column feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addTargetStructColumnPropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_ExplodeArrayOfStructsView_targetStructColumn_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_ExplodeArrayOfStructsView_targetStructColumn_feature", "_UI_ExplodeArrayOfStructsView_type"),
				 dtm_sql_viewsPackage.Literals.EXPLODE_ARRAY_OF_STRUCTS_VIEW__TARGET_STRUCT_COLUMN,
				 true,
				 false,
				 true,
				 null,
				 null,
				 null));
	}

	/**
	 * This returns ExplodeArrayOfStructsView.gif.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object getImage(Object object) {
		return overlayImage(object, getResourceLocator().getImage("full/obj16/ExplodeArrayOfStructsView"));
	}

	/**
	 * This returns the label text for the adapted class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getText(Object object) {
		String label = ((ExplodeArrayOfStructsView)object).getName();
		return label == null || label.length() == 0 ?
			getString("_UI_ExplodeArrayOfStructsView_type") :
			getString("_UI_ExplodeArrayOfStructsView_type") + " " + label;
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
