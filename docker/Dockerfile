# 使用 OpenJDK 8 作为基础镜像
FROM openjdk:8-jre-slim-bullseye

# 设置工作目录
WORKDIR /app

# 复制项目文件到容器内
COPY target/data-generator-1.0-jar-with-dependencies.jar /app/data-generator-1.0-jar-with-dependencies.jar

# 创建目录用于存放日志和 dump 文件
RUN mkdir -p /app/logs

# 运行数据生成器，启用 G1 垃圾收集器
#CMD ["java", "-XX:+UseG1GC", "-Xmx8g", "-Xms8g", "-XX:MaxGCPauseMillis=200", "-XX:InitiatingHeapOccupancyPercent=35", "-XX:+HeapDumpOnOutOfMemoryError", "-XX:HeapDumpPath=/app/logs/heapdump.hprof", "-XX:+PrintGCDetails", "-XX:+PrintGCDateStamps", "-Xloggc:/app/logs/gc.log", "-cp", "data-generator-1.0-jar-with-dependencies.jar", "org.apdplat.data.generator.generator.Generator"]
# 运行数据生成器
CMD ["java", "-Xmx8g", "-Xms8g","-XX:NewSize=1g", "-XX:MaxNewSize=2g","-XX:+HeapDumpOnOutOfMemoryError","-XX:HeapDumpPath=/app/logs/heapdump.hprof","-XX:+PrintGCDetails","-XX:+PrintGCDateStamps", "-XX:+PrintHeapAtGC", "-XX:+PrintTenuringDistribution", "-Xloggc:/app/logs/gc.log","-cp", "data-generator-1.0-jar-with-dependencies.jar", "org.apdplat.data.generator.generator.Generator"]
# "-XX:+HeapDumpAfterFullGC"