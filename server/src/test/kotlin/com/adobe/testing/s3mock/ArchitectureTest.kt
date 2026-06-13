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

import com.tngtech.archunit.base.DescribedPredicate
import com.tngtech.archunit.core.domain.JavaClass
import com.tngtech.archunit.core.domain.JavaClass.Predicates.resideInAPackage
import com.tngtech.archunit.core.importer.ImportOption
import com.tngtech.archunit.junit.AnalyzeClasses
import com.tngtech.archunit.junit.ArchTest
import com.tngtech.archunit.lang.ArchRule
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noCodeUnits

@AnalyzeClasses(packages = ["com.adobe.testing.s3mock"], importOptions = [ImportOption.DoNotIncludeTests::class])
class ArchitectureTest {
  // Data classes (BucketMetadata, S3ObjectMetadata, etc.) live in the store package and are
  // legitimately used by controllers as service return types. The invariant is that controllers
  // must not call *Store methods directly (bypassing the service layer).
  private val storeOperationClasses: DescribedPredicate<JavaClass> =
    resideInAPackage("..store..").and(
      DescribedPredicate.describe("have simple name ending with 'Store'") { cls ->
        cls.simpleName.endsWith("Store")
      },
    )

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

  @ArchTest
  val noLegacyJacksonXmlAnnotations: ArchRule =
    noCodeUnits()
      .should()
      .beAnnotatedWith("com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement")
      .orShould()
      .beAnnotatedWith("com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty")
      .orShould()
      .beAnnotatedWith("com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper")
      .because("use tools.jackson annotations, not the legacy com.fasterxml packages")

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
      .accessClassesThat(storeOperationClasses)
      .because(
        "controllers must delegate to services, not call store methods directly; " +
          "KmsValidationFilter and ControllerConfiguration are the intentional exceptions",
      )
}
