/*
 *  Copyright 2017-2026 Adobe.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.adobe.testing.s3mock

import com.tngtech.archunit.core.importer.ImportOption
import com.tngtech.archunit.junit.AnalyzeClasses
import com.tngtech.archunit.junit.ArchTest
import com.tngtech.archunit.lang.ArchRule
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses
import com.tngtech.archunit.library.dependencies.SlicesRuleDefinition.slices

@AnalyzeClasses(packages = ["com.adobe.testing.s3mock"], importOptions = [ImportOption.DoNotIncludeTests::class])
class ArchitectureTest {
  // -----------------------------------------------------------------------
  // Layering rules (Controller → Service → Store; model/dto/util are leaves)
  // -----------------------------------------------------------------------

  /**
   * Stores are the bottom layer. They may depend on model, dto, and util but must not
   * reach up into services or controllers.
   */
  @ArchTest
  val storesMayNotAccessUpperLayers: ArchRule =
    noClasses()
      .that()
      .resideInAPackage("..store..")
      .should()
      .accessClassesThat()
      .resideInAPackage("..controller..")
      .orShould()
      .accessClassesThat()
      .resideInAPackage("..service..")
      .because("stores are the bottom layer and must not depend on controllers or services")

  /**
   * Controllers must delegate to services; they must not call store classes directly.
   * KmsValidationFilter and ControllerConfiguration are intentional exceptions:
   * the filter must validate KMS key IDs before the request reaches the service, and
   * ControllerConfiguration wires the filter's store dependency.
   */
  @ArchTest
  val controllersMayNotAccessStoreDirect: ArchRule =
    noClasses()
      .that()
      .resideInAPackage("..controller..")
      .and()
      .doNotHaveSimpleName("KmsValidationFilter")
      .and()
      .doNotHaveSimpleName("ControllerConfiguration")
      .should()
      .accessClassesThat()
      .resideInAPackage("..store..")
      .because(
        "controllers must delegate to services, not call store methods directly; " +
          "KmsValidationFilter and ControllerConfiguration are the intentional exceptions",
      )

  /**
   * Services must not depend on controllers (no upward coupling).
   */
  @ArchTest
  val servicesMayNotAccessControllers: ArchRule =
    noClasses()
      .that()
      .resideInAPackage("..service..")
      .should()
      .accessClassesThat()
      .resideInAPackage("..controller..")
      .because("services must not depend on controllers")

  /**
   * The model package is a neutral domain leaf: it holds persistence metadata classes
   * that all layers may read, but it must not depend on the persistence layer (store),
   * the HTTP layer (controller), or the business logic layer (service).
   * model→dto is the intentional one-way edge: model classes store dto-typed fields and
   * [com.adobe.testing.s3mock.model.Mappers] defines extension functions that map model to dto.
   */
  @ArchTest
  val modelIsALeafPackage: ArchRule =
    noClasses()
      .that()
      .resideInAPackage("..model..")
      .should()
      .accessClassesThat()
      .resideInAPackage("..store..")
      .orShould()
      .accessClassesThat()
      .resideInAPackage("..service..")
      .orShould()
      .accessClassesThat()
      .resideInAPackage("..controller..")
      .because("model is a leaf package: it must not depend on store, service, or controller")

  /**
   * Utility classes are a leaf layer shared by all layers above the store.
   * They must not reach into the store (they may depend on model and dto).
   */
  @ArchTest
  val utilMayNotDependOnStore: ArchRule =
    noClasses()
      .that()
      .resideInAPackage("..util..")
      .should()
      .accessClassesThat()
      .resideInAPackage("..store..")
      .because("util is a leaf package and must not depend on the store layer")

  // -----------------------------------------------------------------------
  // Cycle-free package slices
  // -----------------------------------------------------------------------

  /**
   * No package slice may have a cyclic dependency on another package slice.
   * Each top-level package under `com.adobe.testing.s3mock` is a separate slice
   * (controller, service, store, model, dto, util, vectors, etc.).
   */
  @ArchTest
  val packagesMustBeFreeOfCycles: ArchRule =
    slices()
      .matching("com.adobe.testing.s3mock.(*)..")
      .should()
      .beFreeOfCycles()

  // -----------------------------------------------------------------------
  // API / annotation rules
  // -----------------------------------------------------------------------

  /**
   * All Jackson XML annotations must use the tools.jackson packages (Jackson 3), not the
   * legacy com.fasterxml.jackson.dataformat.xml.annotation package.
   */
  @ArchTest
  val noLegacyJacksonXmlAnnotations: ArchRule =
    noClasses()
      .should()
      .dependOnClassesThat()
      .resideInAPackage("com.fasterxml.jackson.dataformat.xml.annotation..")
      .because("use tools.jackson annotations, not the legacy com.fasterxml packages")
}
