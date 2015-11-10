package it.factbook.semantic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Collection of static methods to use as helpers in this package
 */
public class Util {
    private static final Logger log = LoggerFactory.getLogger(Util.class);

    /**
     * Reads multiple config files
     *
     * @return {@link Properties} holder structure
     */
    public static Properties readProperties(){
        Properties propertiesHolders = new Properties();
        String holdersFileNames = "build.properties";

        Properties configProperties = new Properties();
        try (InputStream inputStream = BuildSemanticIndex.class.getClassLoader().getResourceAsStream(holdersFileNames)){
            propertiesHolders.load(inputStream);
            for (Object propertyFile: propertiesHolders.values()) {
                try (InputStream inputStream2 = BuildSemanticIndex.class.getClassLoader()
                        .getResourceAsStream((String)propertyFile)) {
                    configProperties.load(inputStream2);
                }
            }
        } catch (IOException e){
            log.error("property file {} not found in the classpath", holdersFileNames);
        }
        return configProperties;
    }
}
