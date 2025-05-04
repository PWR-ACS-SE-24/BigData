plugins {
    application

    id("com.gradleup.shadow") version "9.0.0-beta2"
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.hadoop:hadoop-common:3.3.0")
    implementation("org.apache.hadoop:hadoop-mapreduce-client-core:3.3.0")
}

application {
    mainClass = "Main"
}

java.sourceCompatibility = JavaVersion.VERSION_1_8
java.targetCompatibility = JavaVersion.VERSION_1_8
