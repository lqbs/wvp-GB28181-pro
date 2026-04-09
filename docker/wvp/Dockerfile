FROM maven:3.9.6-eclipse-temurin-21 AS builder

EXPOSE 18978/tcp
EXPOSE 8116/tcp
EXPOSE 8116/udp
EXPOSE 8080/tcp

RUN java -version && javac -version

#RUN sed -i 's/deb.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list.d/debian.sources && \
# RUN apt-get update && \
#    apt-get install -y maven && \
#    rm -rf /var/lib/apt/lists/*
    
COPY . /build
WORKDIR /build
RUN ls && mvn clean package -Dmaven.test.skip=true
WORKDIR /build/target
#确保文件名一致
RUN mv wvp-pro-*.jar wvp.jar

FROM eclipse-temurin:21-jre
RUN mkdir -p /opt/wvp
WORKDIR /opt/wvp
COPY --from=builder /build/target/wvp.jar /opt/wvp/
COPY ./docker/wvp/wvp /opt/wvp
ENTRYPOINT ["java", "-Xms512m", "-Xmx1024m", "-XX:+HeapDumpOnOutOfMemoryError", "-XX:HeapDumpPath=/opt/ylcx/", "-jar", "wvp.jar", "--spring.config.location=/opt/wvp/application.yml"]



#RUN mkdir -p /opt/wvp
#WORKDIR /opt/wvp
#COPY ./wvp /opt/wvp
#
#WORKDIR /home
#RUN cd /home && \
    #git clone https://gitee.com/pan648540858/wvp-GB28181-pro.git
#
#RUN cd /home/wvp-GB28181-pro && \
    #mvn clean package -Dmaven.test.skip=true && \
    #cp /home/wvp-GB28181-pro/target/*.jar /opt/wvp/wvp.jar
#
#WORKDIR /opt/wvp
#ENTRYPOINT ["java", "-Xms512m", "-Xmx1024m", "-XX:+HeapDumpOnOutOfMemoryError", "-XX:HeapDumpPath=/opt/ylcx/", "-jar", "wvp.jar", "--spring.config.location=/opt/ylcx/wvp/application.yml"]