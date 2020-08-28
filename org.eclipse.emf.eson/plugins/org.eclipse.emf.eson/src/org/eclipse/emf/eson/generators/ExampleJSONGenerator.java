/*
 * #%L
 * org.eclipse.emf.eson
 * %%
 * Copyright (C) 2013 - 2015 Michael Vorburger
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.generators;

import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.eson.building.NewObjectExtensions;
import org.eclipse.emf.eson.eFactory.Attribute;
import org.eclipse.emf.eson.eFactory.BooleanAttribute;
import org.eclipse.emf.eson.eFactory.Containment;
import org.eclipse.emf.eson.eFactory.DateAttribute;
import org.eclipse.emf.eson.eFactory.DoubleAttribute;
import org.eclipse.emf.eson.eFactory.EnumAttribute;
import org.eclipse.emf.eson.eFactory.Factory;
import org.eclipse.emf.eson.eFactory.Feature;
import org.eclipse.emf.eson.eFactory.IntegerAttribute;
import org.eclipse.emf.eson.eFactory.MultiValue;
import org.eclipse.emf.eson.eFactory.NewObject;
import org.eclipse.emf.eson.eFactory.NullAttribute;
import org.eclipse.emf.eson.eFactory.Reference;
import org.eclipse.emf.eson.eFactory.StringAttribute;
import org.eclipse.emf.eson.eFactory.Value;
import org.eclipse.xtext.generator.IFileSystemAccess;
import org.eclipse.xtext.generator.IGenerator;

import com.google.common.base.Optional;

/**
 * Example of an ESON to JSON generator.
 * 
 * This is an example - you may wish to sub-class this to refine it for your particular needs.
 * To activate (a subclass of) this, list it in your META-INF/services/org.eclipse.xtext.generator.IGenerator
 * 
 * Currently this is implemented by working directly at the NewObject level. If
 * there are any advantages, this could later be changed to instead work at the
 * level of the "real" EObject which EFactory builds for each NewObject, and
 * serialize that to JSON using e.g. http://ghillairet.github.io/emfjson/.
 *  
 * @author Michael Vorburger
 */
public class ExampleJSONGenerator implements IGenerator {
	// TODO This class, if anything more than an example, would ideally need a test.. problem is inclusion of a JSON parser passing IP Review; @see https://github.com/vorburger/efactory/blob/master/com.googlecode.efactory.tests/src/com/googlecode/efactory/generator/tests/EFactoryJSONGeneratorTest.java

	protected GeneratorHelper helper = new GeneratorHelper(); // NOTE: @Inject is NOT supported here
	private NewObjectExtensions newObjectExtensions;

	protected NewObjectExtensions getNewObjectExtensions() {
		if (newObjectExtensions == null)
			newObjectExtensions = new NewObjectExtensions();
		return newObjectExtensions;
	}

	public void doGenerate(Resource resource, IFileSystemAccess fsa) {
		if (isJSONGenerationActive(resource)) {
			Optional<Factory> factory = helper.getFirstRootContentOfType(resource, Factory.class);
			if (!factory.isPresent())
				return;
			NewObject rootNewObject = factory.get().getRoot();
			if (rootNewObject == null)
				return;
			
			String jsonFileName = getJSONFileName(resource, fsa);
			StringBuilder sb = new StringBuilder();
			generateJSON(sb, rootNewObject);
			fsa.generateFile(jsonFileName, sb);
		}
	}

	protected boolean isJSONGenerationActive(Resource resource) {
		return true;
	}

	protected String getJSONFileName(Resource resource, IFileSystemAccess fsa) {
		URI uri = resource.getURI();
		String relativeFileName = helper.getProjectRelativeFileName(uri, fsa);
		return helper.replaceExtension(relativeFileName, "json");
	}
	
	protected void generateJSON(StringBuilder sb, NewObject newObject) {
		// see JavaDoc comment above at the top class level re. this impl:
		sb.append('{');
		boolean first = true;
		
		Optional<EClass> eClass = getNewObjectExtensions().getDeclaredOrInferredEClass(newObject);
		if (eClass.isPresent()) {
			first = !generateTypeInfoJSON(sb, eClass.get());
		}

		if (newObject.getName() != null) {
			if (!first)
				sb.append(',');
			else
				first = false;
			generateJSONName(sb, "name"); // TODO this is a quick hack, cauz the name may not actually be in an attribute named "name", if you see what I mean.. ;)
			generateJSON(sb, newObject.getName());
		}
		
		for (Feature feature : newObject.getFeatures()) {
			if (!first) {
				sb.append(',');
			} else {
				first = false;
			}
			generateJSONName(sb, feature.getEFeature().getName());
			Value value = feature.getValue();
			generateJSON(sb, value);
		}
		sb.append('}');
	}

	protected boolean generateTypeInfoJSON(StringBuilder sb, EClass eClass) {
		// TODO if(... only generate _type if there are sub-types?
		generateJSONName(sb, "_type");
		generateJSON(sb, eClass.getName());
		return true;
	}

	protected void generateJSONName(StringBuilder sb, String name) {
		generateJSON(sb, name);
		sb.append(':');
	}
		
	protected void generateJSON(StringBuilder sb, Value value) {
		if (value instanceof Attribute) {
			if (value instanceof StringAttribute) {
				StringAttribute stringAttribute = (StringAttribute) value;
				generateJSON(sb, stringAttribute.getValue());
			} else if (value instanceof BooleanAttribute) {
				BooleanAttribute booleanAttribute = (BooleanAttribute) value;
				sb.append(booleanAttribute.isValue());
			} else if (value instanceof IntegerAttribute) {
				IntegerAttribute intAttribute = (IntegerAttribute) value;
				sb.append(intAttribute.getValue());
			} else if (value instanceof EnumAttribute) {
				EnumAttribute enumAttribute = (EnumAttribute) value;
				// TODO not 100% sure if one would want the literal or the name here?
				generateJSON(sb, enumAttribute.getValue().getLiteral());
			} else if (value instanceof DoubleAttribute) {
				DoubleAttribute doubleAttribute = (DoubleAttribute) value;
				sb.append(doubleAttribute.getValue());
			} else if (value instanceof NullAttribute) {
				// Don't this this is relevant, will probably be removed
				sb.append("null");
			} else if (value instanceof DateAttribute) {
				// I know this may not be right.. there is no standard Date format for JSON..
				// My idea is that, later, this can be handled by custom EDataType with custom convert() methods
				DateAttribute dateAttribute = (DateAttribute) value;
				generateJSON(sb, dateAttribute.getValue().toString());
			}
		} else if (value instanceof Containment) {
			Containment containment = (Containment) value;
			generateJSON(sb, containment.getValue());
		} else if (value instanceof MultiValue) {
			MultiValue mv = (MultiValue) value;
			sb.append('[');
			boolean first = true;
			for (Value value2 : mv.getValues()) {
				if (!first) {
					sb.append(',');
				} else {
					first = false;
				}
				generateJSON(sb, value2);
			}
			sb.append(']');
		} else if (value instanceof Reference) {
			Reference r = (Reference) value;
			generateJSON(sb, r);
		}
	}

	protected void generateJSON(StringBuilder sb, String value) {
		sb.append('\"');
		// TODO need to escape any " in value?
		sb.append(value);
		sb.append('\"');
	}

	protected void generateJSON(StringBuilder sb, Reference reference) {
		// TODO handle this... ;-) but we have to emit something, not nothing, else JSON would be broken
		generateJSON(sb, "TODO");
	}
	
}
