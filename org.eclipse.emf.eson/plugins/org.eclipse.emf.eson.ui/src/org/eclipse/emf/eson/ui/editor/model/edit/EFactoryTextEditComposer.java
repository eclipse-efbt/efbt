/*
 * #%L
 * org.eclipse.emf.eson
 * %%
 * Copyright (C) 2015 Anton Kosyakov, Michael Vorburger & others.
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.ui.editor.model.edit;

import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.util.EContentAdapter;
import org.eclipse.emf.eson.util.ESONUtil;
import org.eclipse.text.edits.ReplaceEdit;
import org.eclipse.text.edits.TextEdit;
import org.eclipse.xtext.nodemodel.ICompositeNode;
import org.eclipse.xtext.nodemodel.util.NodeModelUtils;
import org.eclipse.xtext.resource.SaveOptions;
import org.eclipse.xtext.resource.SaveOptions.Builder;
import org.eclipse.xtext.serializer.ISerializer;
import org.eclipse.xtext.ui.editor.model.edit.ITextEditComposer;

import com.google.inject.Inject;

/**
 * ITextEditComposer which customizes the Xtext standard
 * DefaultTextEditComposer, to a) not track derived state changes, due to Xtext
 * Bug 460309, to fix/avoid "Cannot replace an obj that has no associated node"
 * from Serializer.serializeReplacement(EObject, SaveOptions); b) serialize and
 * replace not only modified objects but the entire document in getTextEdit();
 * while this is a little slower, it ensures correct indentation, which due to
 * Xtext Bug 396283 otherwise doesn't work (e.g. any new ESON Feature would
 * appear at column 0, which isn't intuitive / very nice for end-users in the
 * UI).
 * 
 * Does not actually subclass DefaultTextEditComposer because that uses private
 * fields, so its mostly copy/pasted into here. Should these two Xtext bugs ever
 * get fixed, this entire class can probably be removed from ESON.
 * 
 * @see https://bugs.eclipse.org/bugs/show_bug.cgi?id=460309
 * @see https://bugs.eclipse.org/bugs/show_bug.cgi?id=396283
 * 
 * @author Anton Kosyakov
 * @author Michael Vorburger JavaDoc blurb, and found "trick" to Serialize
 *         entire document, but NOT necessarily reformat all (which leads to the
 *         most natural user experience)
 */
public class EFactoryTextEditComposer extends EContentAdapter implements ITextEditComposer {

	@Inject
	private ISerializer serializer;

	private Resource resource;
	private int resourceSize;
	private boolean resourceChanged;

//	private Collection<EObject> modifiedObjects = new LinkedHashSet<EObject>();

	private boolean recording = false;

	@Override
	public void notifyChanged(Notification notification) {
		super.notifyChanged(notification);

		if (!doRecord(notification))
			return;

		if (notification.getNotifier() instanceof EObject) {
			EObject object = (EObject) notification.getNotifier();
			if (isSource(object)) {
				resourceChanged = true;
			}
		} else if (notification.getNotifier() instanceof Resource) {
			resourceChanged = true;
		}
	}

	/**
	 * This is needed to the Xtext bug 460309.
	 * @see https://bugs.eclipse.org/bugs/show_bug.cgi?id=460309 
	 */
//	@Override
//	protected void recordObjectModification(EObject obj) {
//		if (isSource(obj)) {
//			super.recordObjectModification(obj);
//		}
//	}
	protected boolean isSource(EObject obj) {
		return ESONUtil.isSource(obj);
	}

//	protected Collection<EObject> getModifiedObjects() {
//		return modifiedObjects;
//	}

	protected boolean doRecord(Notification notification) {
		if (!recording || notification.isTouch())
			return false;

		switch (notification.getEventType()) {
			case Notification.ADD:
			case Notification.ADD_MANY:
			case Notification.MOVE:
			case Notification.REMOVE:
			case Notification.REMOVE_MANY:
			case Notification.SET:
			case Notification.UNSET:
				return true;
			default:
				return false;
		}
	}

	protected void reset() {
//		getModifiedObjects().clear();
		resourceChanged = false;
	}

	public void beginRecording(Resource newResource) {
		reset();

		if (newResource != resource) {
			if (resource != null)
				resource.eAdapters().remove(this);
			newResource.eAdapters().add(this);
			resource = newResource;
		}
		if (resource.getContents().isEmpty()) {
			resourceSize = 0;
		} else {
			final EObject root = resource.getContents().get(0);
			ICompositeNode rootNode = NodeModelUtils.getNode(root);
			if (rootNode == null) {
				throw new IllegalStateException("Cannot find root node in resource " + resource.getURI());
			}
			resourceSize = rootNode.getTotalLength();
		}
		recording = true;
	}

	public TextEdit endRecording() {
		recording = false;
		TextEdit textEdit = getTextEdit();

		reset();
		return textEdit;
	}

	public TextEdit getTextEdit() {
		TextEdit result = null;

		if (resourceChanged) {
			Builder optionsBuilder = SaveOptions.newBuilder();
// TODO Create a Project (only, not needed on Workspace level) "Save Actions" Preference to allow end-user to set a "Format source" option, and use it here:
//			if ( ... )
//				optionsBuilder.format(); 
			SaveOptions options = optionsBuilder.getOptions();
			
			String text = resource.getContents().isEmpty() ? "" : serializer.serialize(resource.getContents().get(0), options);
			// see doc above & https://bugs.eclipse.org/bugs/show_bug.cgi?id=460309 
			result = new ReplaceEdit(0, resourceSize, text);
		}

		return result;
	}

}