plugins {
    kotlin("jvm") version "2.1.0"
}

group = "ru.larkin"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation(kotlin("stdlib"))

    implementation("org.slf4j:slf4j-api:2.0.16")

    testImplementation(kotlin("test"))
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.11.3")

}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(21)
}