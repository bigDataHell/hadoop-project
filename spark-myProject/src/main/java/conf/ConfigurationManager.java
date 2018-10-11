package conf;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 配置管理组件
 *
 * @author feixue
 */
public class ConfigurationManager {

    private static Properties properties = new Properties();

    /**
     * 静态代码块
     */
    static {

        InputStream resourceAsStream = ConfigurationManager
            .class.getClassLoader()
            .getResourceAsStream("my.properties");
        try {
            properties.load(resourceAsStream);
        } catch (IOException e) {

        }
    }

    /**
     * 获取指定key对应的value
     */
    public  static String getProperties(String key){

        return properties.getProperty(key);

    }
}
