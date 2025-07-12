FROM gcr.io/distroless/java21-debian12:nonroot

WORKDIR /appbin
USER 65534
COPY target/*-app.jar app.jar
ENTRYPOINT ["java"]
CMD ["-jar", "./app.jar"]
