# syntax=docker/dockerfile:1

# 1) FROM 태그 치환용
ARG ICEBERG_VERSION=1.9.1
ARG PG_JDBC_VERSION=42.7.3

FROM apache/iceberg-rest-fixture:${ICEBERG_VERSION}

# 2) 스테이지 내부에서 다시 선언해야 ADD·RUN 등에서 사용 가능
ARG ICEBERG_VERSION
ARG PG_JDBC_VERSION

USER root
WORKDIR /usr/lib/iceberg-rest

ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/${PG_JDBC_VERSION}/postgresql-${PG_JDBC_VERSION}.jar .
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar .

RUN chmod 644 postgresql-${PG_JDBC_VERSION}.jar \
               iceberg-aws-bundle-${ICEBERG_VERSION}.jar

ENTRYPOINT ["java","-cp","/usr/lib/iceberg-rest/*","org.apache.iceberg.rest.RESTCatalogServer"]
