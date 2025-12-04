/*
 * LAAWS Metadata Extractor Common
 *
 * Common utilities for metadata extraction services.
 */

plugins {
    id("lockss-java-conventions")
}

group = "org.lockss.laaws"
version = "2.11.0-SNAPSHOT"
description = "LOCKSS Metadata Extractor Common Library"

// Export test JAR for other projects
val publishTestJar: Boolean by extra(true)

dependencies {
    // Internal dependencies - lockss-core must be first to avoid MetadataManager shadowing from lockss-plugin-compat
    api(project(":lockss-core"))
    api(project(":lockss-spring-bundle")) {
        exclude(module = "lockss-plugin-compat")
    }

    // Marc4j
    api(libs.marc4j)

    // JBibTeX
    api(libs.jbibtex)

    // Jonix (ONIX)
    api(libs.jonix)

    // Test dependencies
    testImplementation(platform(project(":lockss-pom-bundles:lockss-junit5-bundle")))
    testImplementation(libs.junit.jupiter.engine)
}
