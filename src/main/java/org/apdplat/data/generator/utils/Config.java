package org.apdplat.data.generator.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

/**
 * Created by ysc on 18/04/2018.
 */
public class Config {
    private static final Logger LOGGER = LoggerFactory.getLogger(Config.class);
    private static final Map<String, String> CONFIG = new HashMap<>();

    static {
        InputStream inputStream = Config.class.getClassLoader().getResourceAsStream("config.txt");
        if (inputStream != null) {
            LOGGER.info("存在配置文件");
            try (Scanner scanner = new Scanner(inputStream)) {
                while (scanner.hasNextLine()) {
                    String line = scanner.nextLine().trim();
                    if (line.isEmpty() || line.startsWith("#")) {
                        continue;
                    }
                    int index = line.indexOf("=");
                    if (index == -1) {
                        LOGGER.error("error config:" + line);
                        continue;
                    }
                    //有K V
                    if (index > 0 && line.length() > index + 1) {
                        String key = line.substring(0, index).trim();
                        String value = line.substring(index + 1).trim();
                        CONFIG.put(key, value);
                        LOGGER.info("读取配置：" + key + "=" + value);
                    }
                    //有K无V
                    else if (index > 0 && line.length() == index + 1) {
                        String key = line.substring(0, index).trim();
                        CONFIG.put(key, "");
                    } else {
                        LOGGER.error("error config:" + line);
                    }
                }
            } catch (Exception e) {
                LOGGER.error("读取配置文件异常", e);
            }
        }
    }

    public static String getStringValue(String key) {
        return CONFIG.get(key);
    }

    public static int getIntValue(String key) {
        String value = CONFIG.get(key);
        if (value == null) {
            return -1;
        }
        return Integer.parseInt(value);
    }
}
