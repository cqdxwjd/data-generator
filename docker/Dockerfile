# 使用 OpenJDK 8 作为基础镜像
FROM openjdk:8-jre-slim-bullseye

# 设置工作目录
WORKDIR /app

# 复制项目文件到容器内
COPY target/data-generator-1.0-jar-with-dependencies.jar /app/data-generator-1.0-jar-with-dependencies.jar

# 运行数据生成器
CMD ["java", "-Xmx2g", "-Xms2g", "-cp", "data-generator-1.0-jar-with-dependencies.jar", "org.apdplat.data.generator.generator.Generator"]