# Stage 1: Build the application
FROM gradle:7.6-jdk17 AS build
WORKDIR /app
COPY . .
RUN gradle build --no-daemon -x test

# Stage 2: Run the application
FROM amazoncorretto:17-alpine
WORKDIR /app
COPY --from=build /app/build/libs/*.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-Xmx512m", "-jar", "app.jar"]