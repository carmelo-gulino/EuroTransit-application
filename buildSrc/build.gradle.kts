plugins {
    `kotlin-dsl`
}

repositories {
    mavenCentral()
    gradlePluginPortal()
}

dependencies {
    implementation("org.jetbrains.kotlin.jvm:org.jetbrains.kotlin.jvm.gradle.plugin:2.2.21")
    implementation("org.jetbrains.kotlin.plugin.spring:org.jetbrains.kotlin.plugin.spring.gradle.plugin:2.2.21")
    implementation("org.springframework.boot:org.springframework.boot.gradle.plugin:4.0.7")
    implementation("io.spring.dependency-management:io.spring.dependency-management.gradle.plugin:1.1.7")
}