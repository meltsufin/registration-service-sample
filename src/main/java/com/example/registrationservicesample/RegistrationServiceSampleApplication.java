package com.example.registrationservicesample;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.gcp.pubsub.core.PubSubTemplate;
import org.springframework.cloud.gcp.pubsub.support.converter.ConvertedBasicAcknowledgeablePubsubMessage;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Controller;

@SpringBootApplication
public class RegistrationServiceSampleApplication implements CommandLineRunner {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(RegistrationServiceSampleApplication.class);

  @Autowired
  PubSubTemplate pubSubTemplate;

  @Autowired
  JdbcTemplate jdbcTemplate;

  public static void main(String[] args) {
    SpringApplication.run(RegistrationServiceSampleApplication.class, args);
  }

  @Override
  public void run(String... args) throws Exception {

    pubSubTemplate.subscribeAndConvert("registrations-subscription", this::processNewMessage,
        String.class);
  }

  private void processNewMessage(ConvertedBasicAcknowledgeablePubsubMessage<String> message) {
    String[] registration = message.getPayload().split(";");

    if (registration.length == 3) {
      String email = registration[0];
      String firstName = registration[1];
      String lastName = registration[2];

      saveRegistrationInDb(email, firstName, lastName);

      LOGGER.info("Processed registration for <{}> {} {}.", email, firstName, lastName);

    } else {
      LOGGER.warn("Skipping message '" + message.getPayload()
          + "' because it's not in the format [email];[first-name];[last-name]");
    }

    message.ack();
  }

  private void saveRegistrationInDb(String email, String firstName, String lastName) {
    jdbcTemplate
        .update("INSERT INTO registrants (email, first_name, last_name) VALUES (?, ?, ?)", email,
            firstName, lastName);
  }
}
