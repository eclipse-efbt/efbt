/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_sql_views.provider;


import java.util.Collection;
import java.util.List;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.column_transformation_logic.Column_transformation_logicFactory;

import org.eclipse.efbt.language.dtm.model.dtm_sql_views.AggregateEnrichmentViewAndOrderBy;
import org.eclipse.efbt.language.dtm.model.dtm_sql_views.dtm_sql_viewsFactory;
import org.eclipse.efbt.language.dtm.model.dtm_sql_views.dtm_sql_viewsPackage;

import org.eclipse.emf.common.notify.AdapterFactory;
import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EStructuralFeature;

import org.eclipse.emf.edit.provider.ComposeableAdapterFactory;
import org.eclipse.emf.edit.provider.IItemPropertyDescriptor;
import org.eclipse.emf.edit.provider.ViewerNotification;

/**
 * This is the item provider adapter for a {@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.AggregateEnrichmentViewAndOrderBy} object.
 * <!-- begin-user-doc -->
 * <!-- end-user-doc -->
 * @generated
 */
public class AggregateEnrichmentViewAndOrderByItemProvider extends SQLViewItemProvider {
	/**
	 * This constructs an instance from a factory and a notifier.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public AggregateEnrichmentViewAndOrderByItemProvider(AdapterFactory adapterFactory) {
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

			addOrderByVariablePropertyDescriptor(object);
		}
		return itemPropertyDescriptors;
	}

	/**
	 * This adds a property descriptor for the Order By Variable feature.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected void addOrderByVariablePropertyDescriptor(Object object) {
		itemPropertyDescriptors.add
			(createItemPropertyDescriptor
				(((ComposeableAdapterFactory)adapterFactory).getRootAdapterFactory(),
				 getResourceLocator(),
				 getString("_UI_AggregateEnrichmentViewAndOrderBy_orderByVariable_feature"),
				 getString("_UI_PropertyDescriptor_description", "_UI_AggregateEnrichmentViewAndOrderBy_orderByVariable_feature", "_UI_AggregateEnrichmentViewAndOrderBy_type"),
				 dtm_sql_viewsPackage.Literals.AGGREGATE_ENRICHMENT_VIEW_AND_ORDER_BY__ORDER_BY_VARIABLE,
				 true,
				 false,
				 true,
				 null,
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
			childrenFeatures.add(dtm_sql_viewsPackage.Literals.AGGREGATE_ENRICHMENT_VIEW_AND_ORDER_BY__FUNCTIONS);
			childrenFeatures.add(dtm_sql_viewsPackage.Literals.AGGREGATE_ENRICHMENT_VIEW_AND_ORDER_BY__GROUP_BY_CLAUSE);
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
	 * This returns AggregateEnrichmentViewAndOrderBy.gif.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object getImage(Object object) {
		return overlayImage(object, getResourceLocator().getImage("full/obj16/AggregateEnrichmentViewAndOrderBy"));
	}

	/**
	 * This returns the label text for the adapted class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String getText(Object object) {
		String label = ((AggregateEnrichmentViewAndOrderBy)object).getName();
		return label == null || label.length() == 0 ?
			getString("_UI_AggregateEnrichmentViewAndOrderBy_type") :
			getString("_UI_AggregateEnrichmentViewAndOrderBy_type") + " " + label;
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

		switch (notification.getFeatureID(AggregateEnrichmentViewAndOrderBy.class)) {
			case dtm_sql_viewsPackage.AGGREGATE_ENRICHMENT_VIEW_AND_ORDER_BY__FUNCTIONS:
			case dtm_sql_viewsPackage.AGGREGATE_ENRICHMENT_VIEW_AND_ORDER_BY__GROUP_BY_CLAUSE:
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
				(dtm_sql_viewsPackage.Literals.AGGREGATE_ENRICHMENT_VIEW_AND_ORDER_BY__FUNCTIONS,
				 Column_transformation_logicFactory.eINSTANCE.createAggregateColumnFunction()));

		newChildDescriptors.add
			(createChildParameter
				(dtm_sql_viewsPackage.Literals.AGGREGATE_ENRICHMENT_VIEW_AND_ORDER_BY__GROUP_BY_CLAUSE,
				 dtm_sql_viewsFactory.eINSTANCE.createGroupByClause()));
	}

}
