package com.lankheet.iot.mqtthandler.dao;

import com.lankheet.iot.datatypes.entities.Measurement;
import com.lankheet.iot.datatypes.entities.Sensor;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class MeasurementDao
{
   private static final Logger        LOG = LoggerFactory.getLogger(MeasurementDao.class);
   @PersistenceContext
   private              EntityManager entityManager;


   public void saveNewMeasurement(Measurement measurement)
   {
      List<Sensor> sensorList;
      Sensor sensor = measurement.getSensor();
      String query = "SELECT s FROM sensors s WHERE s.macAddress = :mac AND s.sensorType = :type";
      sensorList = entityManager.createQuery(query, Sensor.class)
         .setParameter("mac", sensor.getMacAddress())
         .setParameter("type", sensor.getType())
         .getResultList();
      LOG.debug("Storing: {}", measurement);
      entityManager.getTransaction().begin();
      if (!sensorList.isEmpty())
      {
         sensor = sensorList.get(0);
         measurement.setSensor(sensor);
      }
      else
      {
         entityManager.persist(sensor);
      }

      // TODO: Set reference when sensor already exists;
      entityManager.persist(measurement);
      entityManager.getTransaction().commit();
   }
}
