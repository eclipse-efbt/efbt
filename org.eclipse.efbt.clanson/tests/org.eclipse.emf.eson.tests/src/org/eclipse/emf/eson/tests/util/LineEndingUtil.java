/*
 * #%L
 * org.eclipse.emf.eson.tests
 * %%
 * Copyright (C) 2015 Michael Vorburger
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.tests.util;

public class LineEndingUtil {

	/**
	 * Not 100% sure when this is needed,
	 * but https://code.google.com/a/eclipselabs.org/p/spray/source/browse/tests/org.eclipselabs.spray.xtext.tests/src/org/eclipselabs/spray/xtext/tests/SprayFormatterTest.java
	 * is doing this, so might as well be safe rather than sorry.. ;-)
	 */
	public static String fixLineEndings(String formatted) {
		if (formatted != null && !formatted.trim().isEmpty()) {
			if (isWindowsEnding()) {
				formatted = formatted.replace("\r\n", "\n");
			}
			formatted = formatted.replace("\r\b", "\n");
			formatted = formatted + getEnding();
		}
		return formatted;
	}

	private static String getEnding() {
		return isWindowsEnding() ? "" : /* "\r" */"";
	}

	private static boolean isWindowsEnding() {
		String ls = System.getProperty("line.separator");
		return "\r\n".equals(ls);
	}

	@SuppressWarnings("unused")
	private static boolean isUnixEnding() {
		String ls = System.getProperty("line.separator");
		return "\n".equals(ls);
	}

}
