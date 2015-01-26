/*
 * Copyright 2015 Ben Manes. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.benmanes.caffeine.cache;

import static com.github.benmanes.caffeine.cache.NodeSpec.NODE;
import static com.github.benmanes.caffeine.cache.NodeSpec.UNSAFE_ACCESS;
import static com.github.benmanes.caffeine.cache.NodeSpec.UNUSED;
import static com.github.benmanes.caffeine.cache.NodeSpec.kType;
import static com.github.benmanes.caffeine.cache.NodeSpec.kTypeVar;
import static com.github.benmanes.caffeine.cache.NodeSpec.keyRefQueueSpec;
import static com.github.benmanes.caffeine.cache.NodeSpec.keyRefSpec;
import static com.github.benmanes.caffeine.cache.NodeSpec.keySpec;
import static com.github.benmanes.caffeine.cache.NodeSpec.nodeType;
import static com.github.benmanes.caffeine.cache.NodeSpec.vRefQueueType;
import static com.github.benmanes.caffeine.cache.NodeSpec.vType;
import static com.github.benmanes.caffeine.cache.NodeSpec.vTypeVar;
import static com.github.benmanes.caffeine.cache.NodeSpec.valueRefQueueSpec;
import static com.github.benmanes.caffeine.cache.NodeSpec.valueSpec;

import java.lang.ref.Reference;
import java.lang.reflect.Type;
import java.util.Objects;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.lang.model.element.Modifier;

import com.github.benmanes.caffeine.cache.NodeSpec.Strength;
import com.github.benmanes.caffeine.cache.NodeSpec.Visibility;
import com.google.common.base.CaseFormat;
import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.TypeSpec;
import com.squareup.javapoet.Types;

/**
 * Generates a node implementation.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
public final class NodeGenerator {
  private final String className;
  private final Strength keyStrength;
  private final Strength valueStrength;
  private final boolean expireAfterAccess;
  private final boolean expireAfterWrite;
  private final boolean maximum;
  private final boolean weighed;

  private TypeSpec.Builder nodeSubtype;
  private MethodSpec.Builder constructorByKey;
  private MethodSpec.Builder constructorByKeyRef;

  public NodeGenerator(String className, Strength keyStrength, Strength valueStrength,
      boolean expireAfterAccess, boolean expireAfterWrite, boolean maximum, boolean weighed) {
    this.className = className;
    this.keyStrength = keyStrength;
    this.valueStrength = valueStrength;
    this.expireAfterAccess = expireAfterAccess;
    this.expireAfterWrite = expireAfterWrite;
    this.maximum = maximum;
    this.weighed = weighed;
  }

  /** Returns an node class implementation optimized for the provided configuration. */
  public TypeSpec.Builder createNodeType() {
    makeNodeSubtype();
    makeBaseConstructorByKey();
    makeBaseConstructorByKeyRef();

    addKey();
    addValue();
    addWeight();
    addExpiration();
    addDeques();

    return nodeSubtype
        .addMethod(constructorByKey.build())
        .addMethod(constructorByKeyRef.build())
        .addMethod(newToString());
  }

  private void makeNodeSubtype() {
    nodeSubtype = TypeSpec.classBuilder(className)
        .addModifiers(Modifier.STATIC, Modifier.FINAL)
        .addSuperinterface(Types.parameterizedType(nodeType, kType, vType));
  }

  private void addKey() {
    nodeSubtype.addTypeVariable(kTypeVar)
        .addField(newKeyField())
        .addMethod(newGetter(keyStrength, kType, "key", Visibility.IMMEDIATE))
        .addMethod(newGetterRef("key"));
  }

  private void addValue() {
    nodeSubtype.addTypeVariable(vTypeVar)
        .addField(newFieldOffset("value"))
        .addField(newValueField())
        .addMethod(newGetter(valueStrength, vType, "value", Visibility.LAZY))
        .addMethod(makeSetValue())
        .addMethod(makeContainsValue());
  }

  /** Creates the setValue method. */
  private MethodSpec makeSetValue() {
    MethodSpec.Builder setter = MethodSpec.methodBuilder("setValue")
        .addAnnotation(Override.class)
        .addModifiers(Modifier.PUBLIC)
        .addParameter(ParameterSpec.builder(Object.class, "keyReference")
            .addAnnotation(Nonnull.class).build())
        .addParameter(ParameterSpec.builder(vType, "value")
            .addAnnotation(Nonnull.class).build())
        .addParameter(ParameterSpec.builder(vRefQueueType, "referenceQueue")
          .addAnnotation(Nonnull.class).build());

    if (valueStrength == Strength.STRONG) {
      setter.addStatement("$T.UNSAFE.putOrderedObject(this, $N, $N)",
          UNSAFE_ACCESS, offsetName("value"), "value");
    } else {
      setter.addStatement("$T.UNSAFE.putOrderedObject(this, $N, new $T($N, $N, referenceQueue))",
          UNSAFE_ACCESS, offsetName("value"), valueStrength.valueReferenceType(),
          "keyReference", "value");
    }

    return setter.build();
  }

  private MethodSpec makeContainsValue() {
    MethodSpec.Builder containsValue = MethodSpec.methodBuilder("containsValue")
        .addParameter(ParameterSpec.builder(Object.class, "value")
            .addAnnotation(Nonnull.class).build())
        .addModifiers(Modifier.PUBLIC)
        .addAnnotation(Override.class)
        .returns(boolean.class);
    if (valueStrength == Strength.STRONG) {
      containsValue.addStatement("return $T.equals(value, getValue())", Objects.class);
    } else {
      containsValue.addStatement("return getValue() == value");
    }
    return containsValue.build();
  }

  private FieldSpec newKeyField() {
    Modifier[] modifiers = { Modifier.PRIVATE, Modifier.FINAL };
    FieldSpec.Builder fieldSpec = (keyStrength == Strength.STRONG)
        ? FieldSpec.builder(kType, "key", modifiers)
        : FieldSpec.builder(keyStrength.keyReferenceType(), "key", modifiers);
    return fieldSpec.build();
  }

  private FieldSpec newValueField() {
    Modifier[] modifiers = { Modifier.PRIVATE, Modifier.VOLATILE };
    FieldSpec.Builder fieldSpec = (valueStrength == Strength.STRONG)
        ? FieldSpec.builder(vType, "value", modifiers)
        : FieldSpec.builder(valueStrength.valueReferenceType(), "value", modifiers);
    fieldSpec.addAnnotation(UNUSED);
    return fieldSpec.build();
  }

  /** Adds the constructor by key to the node type. */
  private void makeBaseConstructorByKey() {
    constructorByKey = MethodSpec.constructorBuilder().addParameter(keySpec);
    constructorByKey.addParameter(keyRefQueueSpec);
    addKeyConstructorAssignment(constructorByKey, false);
    completeBaseConstructor(constructorByKey);
  }

  /** Adds the constructor by key reference to the node type. */
  private void makeBaseConstructorByKeyRef() {
    constructorByKeyRef = MethodSpec.constructorBuilder().addParameter(keyRefSpec);
    constructorByKeyRef.addAnnotation( AnnotationSpec.builder(SuppressWarnings.class)
        .addMember("value", "$S", "unchecked").build());
    addKeyConstructorAssignment(constructorByKeyRef, true);
    completeBaseConstructor(constructorByKeyRef);
  }

  private void completeBaseConstructor(MethodSpec.Builder constructor) {
    constructor.addParameter(valueSpec);
    if (valueStrength != Strength.STRONG) {
      constructor.addParameter(valueRefQueueSpec);
    }
    if (weighed) {
      constructor.addParameter(ParameterSpec.builder(int.class, "weight")
          .addAnnotation(Nonnegative.class).build());
    }
    if (expireAfterAccess || expireAfterWrite) {
      constructor.addParameter(ParameterSpec.builder(long.class, "now")
          .addAnnotation(Nonnegative.class).build());
    }
    addValueConstructorAssignment(constructor);
  }

  /** Adds a constructor assignment. */
  private void addKeyConstructorAssignment(MethodSpec.Builder constructor, boolean isReference) {
    if (isReference || (keyStrength == Strength.STRONG)) {
      String refAssignment = (keyStrength == Strength.STRONG)
          ? "(K) keyReference"
          : "(WeakKeyReference<K>) keyReference";
      constructor.addStatement("this.$N = $N", "key", isReference ? refAssignment : "key");
    } else {
      constructor.addStatement("this.$N = new $T($N, $N)",
          "key", keyStrength.keyReferenceType(), "key", "keyReferenceQueue");
    }
  }

  /** Adds a constructor assignment. */
  private void addValueConstructorAssignment(MethodSpec.Builder constructor) {
    if (valueStrength == Strength.STRONG) {
      constructor.addStatement("$T.UNSAFE.putOrderedObject(this, $N, $N)",
          UNSAFE_ACCESS, offsetName("value"), "value");
    } else {
      constructor.addStatement("$T.UNSAFE.putOrderedObject(this, $N, new $T(this.$N, $N, $N))",
          UNSAFE_ACCESS, offsetName("value"), valueStrength.valueReferenceType(),
          "key", "value", "valueReferenceQueue");
    }
  }

  /** Adds weight support, if enabled, to the node type. */
  private void addWeight() {
    if(weighed) {
      nodeSubtype.addField(int.class, "weight", Modifier.PRIVATE)
          .addMethod(newGetter(Strength.STRONG, int.class, "weight", Visibility.IMMEDIATE))
          .addMethod(newSetter(int.class, "weight", Visibility.IMMEDIATE));
      addIntConstructorAssignment(constructorByKey, "weight", "weight", Visibility.IMMEDIATE);
      addIntConstructorAssignment(constructorByKeyRef, "weight", "weight", Visibility.IMMEDIATE);
    }
  }

  /** Adds the expiration support, if enabled, to the node type. */
  private void addExpiration() {
    if (expireAfterAccess) {
      nodeSubtype.addField(newFieldOffset("accessTime"))
          .addField(FieldSpec.builder(long.class, "accessTime", Modifier.PRIVATE, Modifier.VOLATILE)
              .addAnnotation(UNUSED).build())
          .addMethod(newGetter(Strength.STRONG, long.class, "accessTime", Visibility.LAZY))
          .addMethod(newSetter(long.class, "accessTime", Visibility.LAZY));
      addLongConstructorAssignment(constructorByKey, "now", "accessTime", Visibility.LAZY);
      addLongConstructorAssignment(constructorByKeyRef, "now", "accessTime", Visibility.LAZY);
    }
    if (expireAfterWrite) {
      nodeSubtype.addField(newFieldOffset("writeTime"))
          .addField(FieldSpec.builder(long.class, "writeTime", Modifier.PRIVATE, Modifier.VOLATILE)
              .addAnnotation(UNUSED).build())
          .addMethod(newGetter(Strength.STRONG, long.class, "writeTime", Visibility.LAZY))
          .addMethod(newSetter(long.class, "writeTime", Visibility.LAZY));
      addLongConstructorAssignment(constructorByKey, "now", "writeTime", Visibility.LAZY);
      addLongConstructorAssignment(constructorByKeyRef, "now", "writeTime", Visibility.LAZY);
    }
  }

  /** Adds a integer constructor assignment. */
  private void addIntConstructorAssignment(MethodSpec.Builder constructor,
      String param, String field, Visibility visibility) {
    if (visibility.isRelaxed) {
      constructor.addStatement("$T.UNSAFE.putOrderedInt(this, $N, $N)",
          UNSAFE_ACCESS, offsetName(field), param);
      constructor.addStatement("this.$N = $N", field, param);
    } else {
      constructor.addStatement("this.$N = $N", field, param);
    }
  }

  /** Adds a long constructor assignment. */
  private void addLongConstructorAssignment(MethodSpec.Builder constructor,
      String param, String field, Visibility visibility) {
    if (visibility.isRelaxed) {
      constructor.addStatement("$T.UNSAFE.putOrderedLong(this, $N, $N)",
          UNSAFE_ACCESS, offsetName(field), param);
    } else {
      constructor.addStatement("this.$N = $N", field, param);
    }
  }

  /** Adds the access and write deques, if needed, to the type. */
  private void addDeques() {
    if (maximum || expireAfterAccess) {
      addFieldAndGetter(nodeSubtype, NODE, "previousInAccessOrder");
      addFieldAndGetter(nodeSubtype, NODE, "nextInAccessOrder");
    }
    if (expireAfterWrite) {
      addFieldAndGetter(nodeSubtype, NODE, "previousInWriteOrder");
      addFieldAndGetter(nodeSubtype, NODE, "nextInWriteOrder");
    }
  }

  /** Adds a simple field, accessor, and mutator for the variable. */
  private void addFieldAndGetter(TypeSpec.Builder typeSpec, Type varType, String varName) {
    typeSpec.addField(varType, varName, Modifier.PRIVATE)
        .addMethod(newGetter(Strength.STRONG, varType, varName, Visibility.IMMEDIATE))
        .addMethod(newSetter(varType, varName, Visibility.IMMEDIATE));
  }

  /** Creates a static field with an Unsafe address offset. */
  private FieldSpec newFieldOffset(String varName) {
    String name = offsetName(varName);
    return FieldSpec
        .builder(long.class, name, Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
        .initializer("$T.objectFieldOffset($T.class, $S)", UNSAFE_ACCESS,
            ClassName.bestGuess(className), varName).build();
  }

  /** Returns the offset constant to this variable. */
  private static String offsetName(String varName) {
    return CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, varName) + "_OFFSET";
  }

  /** Creates an accessor that returns the reference holding the variable. */
  private MethodSpec newGetterRef(String varName) {
    String methodName = String.format("get%sReference",
        Character.toUpperCase(varName.charAt(0)) + varName.substring(1));
    MethodSpec.Builder getter = MethodSpec.methodBuilder(methodName)
        .addAnnotation(Override.class)
        .addModifiers(Modifier.PUBLIC)
        .returns(Object.class);
    getter.addAnnotation(Nonnull.class);
    getter.addStatement("return $N", varName);
    return getter.build();
  }

  /** Creates an accessor that returns the unwrapped variable. */
  private MethodSpec newGetter(Strength strength, Type varType,
      String varName, Visibility visibility) {
    String methodName = "get" + Character.toUpperCase(varName.charAt(0)) + varName.substring(1);
    MethodSpec.Builder getter = MethodSpec.methodBuilder(methodName)
        .addAnnotation(Override.class)
        .addModifiers(Modifier.PUBLIC)
        .returns(varType);
    String type;
    boolean primitive = false;
    if ((varType == int.class) || (varType == long.class)) {
      primitive = true;
      getter.addAnnotation(Nonnegative.class);
      type = (varType == int.class) ? "Int" : "Long";
    } else {
      getter.addAnnotation(Nullable.class);
      type = "Object";
    }
    if (strength == Strength.STRONG) {
      if (visibility.isRelaxed) {
        if (primitive) {
          getter.addStatement("return $T.UNSAFE.get$N(this, $N)",
              UNSAFE_ACCESS, type, offsetName(varName));
        } else {
          getter.addStatement("return ($T) $T.UNSAFE.get$N(this, $N)",
              varType, UNSAFE_ACCESS, type, offsetName(varName));
        }
      } else {
        getter.addStatement("return $N", varName);
      }
    } else {
      if (visibility.isRelaxed) {
        getter.addStatement("return (($T<$T>) $T.UNSAFE.get$N(this, $N)).get()",
            Reference.class, varType, UNSAFE_ACCESS, type, offsetName(varName));
      } else {
        getter.addStatement("return $N.get()", varName);
      }
    }
    if (visibility.isRelaxed && type.equals("Object")) {
      getter.addAnnotation(AnnotationSpec.builder(SuppressWarnings.class)
          .addMember("value", "$S", "unchecked").build());
    }
    return getter.build();
  }

  /** Creates a mutator to the variable. */
  private MethodSpec newSetter(Type varType, String varName, Visibility visibility) {
    String methodName = "set" + Character.toUpperCase(varName.charAt(0)) + varName.substring(1);
    String type;
    boolean primitive = false;
    if ((varType == int.class) || (varType == long.class)) {
      primitive = true;
      type = (varType == int.class) ? "Int" : "Long";
    } else {
      type = "Object";
    }
    MethodSpec.Builder setter = MethodSpec.methodBuilder(methodName)
        .addAnnotation(Override.class)
        .addModifiers(Modifier.PUBLIC)
        .addParameter(ParameterSpec.builder(varType, varName)
            .addAnnotation(primitive ? Nonnegative.class : Nullable.class).build());
    if (visibility.isRelaxed) {
      setter.addStatement("$T.UNSAFE.putOrdered$L(this, $N, $N)",
          UNSAFE_ACCESS, type, offsetName(varName), varName);
    } else {
      setter.addStatement("this.$N = $N", varName, varName);
    }

    return setter.build();
  }

  public MethodSpec newToString() {
    StringBuilder start = new StringBuilder();
    StringBuilder end = new StringBuilder();
    start.append("return String.format(\"%s=[key=%s, value=%s");
    end.append("]\",\ngetClass().getSimpleName(), getKey(), getValue()");
    if (weighed) {
      start.append(", weight=%d");
      end.append(", getWeight()");
    }
    if (expireAfterAccess) {
      start.append(", accessTimeNS=%,d");
      end.append(", getAccessTime()");
    }
    if (expireAfterWrite) {
      start.append(", writeTimeNS=%,d");
      end.append(", getWriteTime()");
    }
    end.append(")");

    return MethodSpec.methodBuilder("toString")
        .addModifiers(Modifier.PUBLIC)
        .addAnnotation(Override.class)
        .returns(String.class)
        .addStatement(start.toString() + end.toString())
        .build();
  }
}