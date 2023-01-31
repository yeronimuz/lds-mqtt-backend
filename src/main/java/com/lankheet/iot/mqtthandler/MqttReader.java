package com.lankheet.iot.mqtthandler;

import com.lankheet.iot.mqtthandler.config.MqttConfig;
import com.lankheet.iot.mqtthandler.dao.MqttConfigDao;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.inject.Inject;
import javax.ws.rs.core.Context;

/**
 * MQTT client manager that subscribes to the domotics topics.
 */
@Singleton
@Startup
public class MqttReader
{
   private static final Logger LOG = LoggerFactory.getLogger(MqttReader.class);
   protected static final int           MQTT_CONNECT_RETRY_TIMEOUT = 500;
   @Inject
   private                MqttConfigDao mqttConfigDao;

   private       MqttClient         client;
   private final MqttConnectOptions options = new MqttConnectOptions();

   @PostConstruct
   public void init(MqttConfig mqttConfig)
      throws MqttException
   {
      // TODO: Read config from DB
      String userName = mqttConfig.getUserName();
      String password = mqttConfig.getPassword();
      client = new MqttClient(mqttConfig.getUrl(), MqttClient.generateClientId(), new MemoryPersistence());
      client.setCallback(new BackendMqttCallback());

      options.setCleanSession(true);
      options.setConnectionTimeout(60);
      options.setKeepAliveInterval(60);
      options.setUserName(userName);
      options.setPassword(password.toCharArray());

      connect();
   }

   public MqttClient getClient()
   {
      return client;
   }

   public void connect()
   {
      LOG.info("Connecting mqtt broker with options: {}", options);

      do
      {
         try
         {
            client.connect(options);
         }
         catch (MqttException | SecurityException ex)
         {
            LOG.error("Unable to connect to Mqtt broker!");
            try
            {
               Thread.sleep(MQTT_CONNECT_RETRY_TIMEOUT);
               LOG.info("Retrying connection");
            }
            catch (InterruptedException e)
            {
               LOG.error("Interrupted while connecting");
            }
         }
      }
      while (!client.isConnected());
      LOG.info("Mqtt client connected: " + client.getClientId());
      try
      {
         client.subscribe("#", 2);
      }
      catch (MqttException ex)
      {
         LOG.error("Could not subscribe: {}", ex);
      }
      LOG.warn("End of the universe!");
   }
}
