plugins {
    application

    id("com.diffplug.spotless") version "7.0.3"
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.hadoop:hadoop-common:3.4.1")
    implementation("org.apache.hadoop:hadoop-mapreduce-client-core:3.4.1")
}

application {
    mainClass = "dev.tchojnacki.bigdata.zad7.Main"
}

spotless {
    java {
        googleJavaFormat("1.26.0")
    }
}
