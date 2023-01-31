package com.lankheet.iot.mqtthandler;

import java.util.concurrent.BlockingQueue;

import com.lankheet.iot.mqtthandler.dao.MeasurementDao;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.lankheet.iot.datatypes.domotics.SensorValue;
import com.lankheet.iot.datatypes.entities.Measurement;
import com.lankheet.iot.datatypes.entities.MeasurementType;
import com.lankheet.iot.datatypes.entities.Sensor;
import com.lankheet.iot.datatypes.entities.SensorType;
import com.lankheet.utils.JsonUtil;

import javax.enterprise.inject.spi.CDI;
import javax.inject.Inject;

import static com.lankheet.iot.mqtthandler.MqttReader.MQTT_CONNECT_RETRY_TIMEOUT;

/**
 * Callback class for mqtt comms.
 *
 */
public class BackendMqttCallback implements MqttCallback {
    private static final Logger LOG = LoggerFactory.getLogger(BackendMqttCallback.class);

    @Inject
    private MeasurementDao measurementDao;

    @Override
    public void connectionLost(Throwable cause) {
        CDI.current().select(MqttReader.class).get().connect();
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        LOG.debug("Topic: {}, message: {}", topic, message);
        String payload = new String(message.getPayload());
        LOG.debug("Payload: {}", payload);
        SensorValue sensorValue = null;
        try {
            sensorValue = JsonUtil.fromJson(payload);
        } catch (JsonProcessingException e) {
            LOG.error(e.getMessage());
            return;
        }
        Measurement newMmeasurement = new Measurement(
                new Sensor(SensorType.getType(sensorValue.getSensorNode().getSensorType()),
                        sensorValue.getSensorNode().getSensorMac(), null, null),
                sensorValue.getTimeStamp(), MeasurementType.getType(sensorValue.getMeasurementType()),
                sensorValue.getValue());

        measurementDao.saveNewMeasurement(newMmeasurement);
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        LOG.info("Delivery complete: {}", token);
    }
}
