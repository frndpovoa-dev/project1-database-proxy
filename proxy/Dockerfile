FROM bellsoft/liberica-runtime-container:jre-24-musl

WORKDIR /appbin
COPY target/*-app.jar app.jar
ENTRYPOINT ["java"]
CMD ["-jar", "./app.jar"]
