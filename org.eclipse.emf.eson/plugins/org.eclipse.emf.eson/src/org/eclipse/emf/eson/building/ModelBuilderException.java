/*
 * #%L
 * org.eclipse.emf.eson
 * %%
 * Copyright (C) 2013 - 2014 Michael Vorburger
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.building;

/**
 * Exception for stuff going wrong in the ModelBuilder.
 * 
 * @author Michael Vorburger
 */
public class ModelBuilderException extends Exception {
	private static final long serialVersionUID = 218749336949211916L;

	public ModelBuilderException(String message) {
		super(message);
	}

	public ModelBuilderException(String message, Throwable e) {
		super(message, e);
	}
}
