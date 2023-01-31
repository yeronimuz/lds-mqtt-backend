package com.lankheet.iot.mqtthandler.config;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test for @link {@link BackendServiceConfig}
 *
 */
public class MqttServiceConfigTest
{
    private static BackendServiceConfig config;

    @BeforeClass
    public static void setupBackendServiceConfigTest() throws IOException {
        config = BackendServiceConfig.loadConfigurationFromFile("src/test/resources/application.yml");
    }

    @Test
    public void testConfigDatabase() throws Exception {
        DatabaseConfig databaseConfig = config.getDatabaseConfig();
        assertThat(databaseConfig, is(notNullValue()));
        assertThat(databaseConfig.getDriver(), is("com.mysql.jdbc.Driver"));
        assertThat(databaseConfig.getUrl(), is("jdbc:mysql://localhost:3306/domotics"));
        assertThat(databaseConfig.getUserName(), is("testuser"));
        assertThat(databaseConfig.getPassword(), is("p@ssW0rd"));
    }

    @Test
    public void testConfigMqtt() {

        MqttConfig mqttConfig = config.getMqttConfig();
        assertThat(mqttConfig, is(notNullValue()));
        assertThat(mqttConfig.getUrl(), is("tcp://localhost:1883"));
        assertThat(mqttConfig.getUserName(), is("johndoe"));
        assertThat(mqttConfig.getPassword(), is("secret"));
    }
}
