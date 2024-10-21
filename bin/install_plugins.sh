#!/bin/bash

# Create jars directory if it doesn't exist
mkdir -p jars

# Download required JAR files to jars directory
if [[ ! -f "jars/aws-java-sdk-bundle-1.12.262.jar" ]]; then
    curl -o jars/aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
fi
if [[ ! -f "jars/hadoop-aws-3.3.4.jar" ]]; then
    curl -o jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
fi

# Verify the downloaded files
if [[ -f "jars/aws-java-sdk-bundle-1.12.262.jar" && -f "jars/hadoop-aws-3.3.4.jar" ]]; then
    echo "JAR files downloaded successfully."
else
    echo "Failed to download one or more JAR files."
    exit 1
fi
