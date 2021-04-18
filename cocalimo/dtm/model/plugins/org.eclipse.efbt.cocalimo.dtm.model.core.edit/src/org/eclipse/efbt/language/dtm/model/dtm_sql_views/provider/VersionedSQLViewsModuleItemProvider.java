/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_sql_views.provider;


import java.util.Collection;
import java.util.List;

import org.eclipse.efbt.language.dtm.model.domain_model_mapping.provider.dtmEditPlugin;

import org.eclipse.efbt.language.dtm.model.dtm_report_cell_views.dtm_report_cell_viewsFactory;

import org.eclipse.efbt.language.dtm.model.dtm_sql_views.VersionedSQLViewsModule;
import org.eclipse.efbt.language.dtm.model.dtm_sql_views.dtm_sql_viewsFactory;
import org.eclipse.efbt.language.dtm.model.dtm_sql_views.dtm_sql_viewsPackage;

import org.eclipse.efbt.language.dtm.model.transformation.provider.VersionedFunctionalModuleLogicItemProvider;

import org.eclipse.emf.common.notify.AdapterFactory;
import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.common.util.ResourceLocator;

import org.eclipse.emf.ecore.EStructuralFeature;

import org.eclipse.emf.edit.provider.IItemPropertyDescriptor;
import org.eclipse.emf.edit.provider.ViewerNotification;

/**
 * This is the item provider adapter for a {@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.VersionedSQLViewsModule} object.
 * <!-- begin-user-doc -->
 * <!-- end-user-doc -->
 * @generated
 */
public class VersionedSQLViewsModuleItemProvider extends VersionedFunctionalModuleLogicItemProvider {
	/**
	 * This constructs an instance from a factory and a notifier.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VersionedSQLViewsModuleItemProvider(AdapterFactory adapterFactory) {
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

		}
		return itemPropertyDescriptors;
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
			childrenFeatures.add(dtm_sql_viewsPackage.Literals.VERSIONED_SQL_VIEWS_MODULE__SQL_VIEWS);
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
	 * This returns VersionedSQLViewsModule.gif.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object getImage(Object object) {
		return overlayImage(object, getResourceLocator().getImage("full/obj16/VersionedSQLViewsModule"));
	}

	/**
	 * This returns the label text for the adapted class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getText(Object object) {
		String label = ((VersionedSQLViewsModule)object).getName();
		return label == null || label.length() == 0 ?
			getString("_UI_VersionedSQLViewsModule_type") :
			getString("_UI_VersionedSQLViewsModule_type") + " " + label;
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

		switch (notification.getFeatureID(VersionedSQLViewsModule.class)) {
			case dtm_sql_viewsPackage.VERSIONED_SQL_VIEWS_MODULE__SQL_VIEWS:
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
				(dtm_sql_viewsPackage.Literals.VERSIONED_SQL_VIEWS_MODULE__SQL_VIEWS,
				 dtm_sql_viewsFactory.eINSTANCE.createSQLView()));

		newChildDescriptors.add
			(createChildParameter
				(dtm_sql_viewsPackage.Literals.VERSIONED_SQL_VIEWS_MODULE__SQL_VIEWS,
				 dtm_sql_viewsFactory.eINSTANCE.createAggregateEnrichmentView()));

		newChildDescriptors.add
			(createChildParameter
				(dtm_sql_viewsPackage.Literals.VERSIONED_SQL_VIEWS_MODULE__SQL_VIEWS,
				 dtm_sql_viewsFactory.eINSTANCE.createCopyView()));

		newChildDescriptors.add
			(createChildParameter
				(dtm_sql_viewsPackage.Literals.VERSIONED_SQL_VIEWS_MODULE__SQL_VIEWS,
				 dtm_sql_viewsFactory.eINSTANCE.createEnrichmentView()));

		newChildDescriptors.add
			(createChildParameter
				(dtm_sql_viewsPackage.Literals.VERSIONED_SQL_VIEWS_MODULE__SQL_VIEWS,
				 dtm_sql_viewsFactory.eINSTANCE.createFilterByConditionView()));

		newChildDescriptors.add
			(createChildParameter
				(dtm_sql_viewsPackage.Literals.VERSIONED_SQL_VIEWS_MODULE__SQL_VIEWS,
				 dtm_sql_viewsFactory.eINSTANCE.createGenericView()));

		newChildDescriptors.add
			(createChildParameter
				(dtm_sql_viewsPackage.Literals.VERSIONED_SQL_VIEWS_MODULE__SQL_VIEWS,
				 dtm_sql_viewsFactory.eINSTANCE.createJoinView()));

		newChildDescriptors.add
			(createChildParameter
				(dtm_sql_viewsPackage.Literals.VERSIONED_SQL_VIEWS_MODULE__SQL_VIEWS,
				 dtm_sql_viewsFactory.eINSTANCE.createUnionView()));

		newChildDescriptors.add
			(createChildParameter
				(dtm_sql_viewsPackage.Literals.VERSIONED_SQL_VIEWS_MODULE__SQL_VIEWS,
				 dtm_sql_viewsFactory.eINSTANCE.createBaseViewIncorporatingDeltas()));

		newChildDescriptors.add
			(createChildParameter
				(dtm_sql_viewsPackage.Literals.VERSIONED_SQL_VIEWS_MODULE__SQL_VIEWS,
				 dtm_sql_viewsFactory.eINSTANCE.createDeltaAccumulation()));

		newChildDescriptors.add
			(createChildParameter
				(dtm_sql_viewsPackage.Literals.VERSIONED_SQL_VIEWS_MODULE__SQL_VIEWS,
				 dtm_sql_viewsFactory.eINSTANCE.createCastColumnView()));

		newChildDescriptors.add
			(createChildParameter
				(dtm_sql_viewsPackage.Literals.VERSIONED_SQL_VIEWS_MODULE__SQL_VIEWS,
				 dtm_sql_viewsFactory.eINSTANCE.createExplodeArrayOfStructsView()));

		newChildDescriptors.add
			(createChildParameter
				(dtm_sql_viewsPackage.Literals.VERSIONED_SQL_VIEWS_MODULE__SQL_VIEWS,
				 dtm_sql_viewsFactory.eINSTANCE.createExplodeStructView()));

		newChildDescriptors.add
			(createChildParameter
				(dtm_sql_viewsPackage.Literals.VERSIONED_SQL_VIEWS_MODULE__SQL_VIEWS,
				 dtm_sql_viewsFactory.eINSTANCE.createFilterByStructClassColumnView()));

		newChildDescriptors.add
			(createChildParameter
				(dtm_sql_viewsPackage.Literals.VERSIONED_SQL_VIEWS_MODULE__SQL_VIEWS,
				 dtm_sql_viewsFactory.eINSTANCE.createMakeArrayOfStructsView()));

		newChildDescriptors.add
			(createChildParameter
				(dtm_sql_viewsPackage.Literals.VERSIONED_SQL_VIEWS_MODULE__SQL_VIEWS,
				 dtm_sql_viewsFactory.eINSTANCE.createMakeStructView()));

		newChildDescriptors.add
			(createChildParameter
				(dtm_sql_viewsPackage.Literals.VERSIONED_SQL_VIEWS_MODULE__SQL_VIEWS,
				 dtm_sql_viewsFactory.eINSTANCE.createAggregateEnrichmentViewAndOrderBy()));

		newChildDescriptors.add
			(createChildParameter
				(dtm_sql_viewsPackage.Literals.VERSIONED_SQL_VIEWS_MODULE__SQL_VIEWS,
				 dtm_report_cell_viewsFactory.eINSTANCE.createReportCellView()));
	}

	/**
	 * Return the resource locator for this item provider's resources.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ResourceLocator getResourceLocator() {
		return dtmEditPlugin.INSTANCE;
	}

}
