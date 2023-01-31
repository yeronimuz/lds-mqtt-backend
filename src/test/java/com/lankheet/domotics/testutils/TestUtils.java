package com.lankheet.domotics.testutils;

import java.util.List;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttSecurityException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.lankheet.iot.datatypes.domotics.SensorValue;
import com.lankheet.iot.datatypes.entities.Measurement;
import com.lankheet.utils.JsonUtil;

public class TestUtils {
    private static final Logger LOG = LoggerFactory.getLogger(TestUtils.class);

    public static MqttClient createMqttClientConnection() throws MqttSecurityException, MqttException {
        MqttClient mqttClient = new MqttClient("tcp://localhost:1883", TestUtils.class.getName() + MqttClient.generateClientId());
        MqttConnectOptions options = new MqttConnectOptions();
        options.setConnectionTimeout(60);
        options.setKeepAliveInterval(60);
        options.setUserName("testuser");
        options.setPassword("testpassword".toCharArray());
        mqttClient.connect(options);
        return mqttClient;
    }

    public static void sendMqttMeasurements(MqttClient mqttClient, List<SensorValue> sensorValues) throws Exception {
         for (SensorValue sensorValue: sensorValues) {
            LOG.info(sensorValue.toString());
            sendMqttSensorValue(mqttClient, sensorValue);
        }
    }

    public static void sendMqttSensorValue(MqttClient mqttClient, SensorValue sensorValue) throws Exception {
        MqttMessage msg = new MqttMessage();
        msg.setPayload(JsonUtil.toJson(sensorValue).getBytes());
        mqttClient.publish("test", msg);
    }
}
