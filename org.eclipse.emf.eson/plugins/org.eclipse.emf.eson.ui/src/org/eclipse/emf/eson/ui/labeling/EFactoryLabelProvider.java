/*
 * #%L
 * org.eclipse.emf.eson.ui
 * %%
 * Copyright (C) 2009 - 2014 Sebastian Benz, Michael Vorburger and others
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.ui.labeling;

import org.eclipse.emf.eson.eFactory.EFactoryPackage;
import org.eclipse.emf.eson.eFactory.impl.NewObjectImpl;
import org.eclipse.xtext.ui.label.DefaultEObjectLabelProvider;

public class EFactoryLabelProvider extends DefaultEObjectLabelProvider {

    public String text(NewObjectImpl element) {
        return new StringBuilder(element.getEClass().getName()).append(" ")
                .append((element.getName() == null) ? "" : element.getName()).toString();
	}

    public String image(NewObjectImpl element) {
        return (element.eContainer().eClass().getClassifierID() == EFactoryPackage.FACTORY) ? "RootObject.gif"
                : "Component.gif";
	}

}
