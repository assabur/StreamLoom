FROM gradle:8.5-jdk17 as builder
WORKDIR /app
COPY build.gradle settings.gradle .
COPY src ./src
RUN gradle clean build -x test

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=builder /app/build/libs/trade-streams-0.1.0.jar app.jar
ENTRYPOINT ["java","-jar","/app/app.jar"]
