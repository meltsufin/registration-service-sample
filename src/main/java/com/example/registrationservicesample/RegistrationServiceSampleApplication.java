package com.example.registrationservicesample;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class RegistrationServiceSampleApplication implements CommandLineRunner {

  private static final Log LOGGER = LogFactory.getLog(RegistrationServiceSampleApplication.class);

  public static void main(String[] args) {
    SpringApplication.run(RegistrationServiceSampleApplication.class, args);
  }

  @Override
  public void run(String... args) {

    // STEP 1: Read messages from Pub/Sub subscription "registrations-subscription".
    //         For each message, acknowledge it, and call processNewMessagePayload.
    System.out.println("STEP 1: Not completed yet.");


  }

  private void processNewMessagePayload(String messagePayload) {
    String[] registration = messagePayload.split(";");

    if (registration.length == 3) {
      String email = registration[0];
      String firstName = registration[1];
      String lastName = registration[2];

      saveRegistrationInDb(email, firstName, lastName);

      LOGGER.info("Processed registration for <" + email + "> " + firstName + " " + lastName + ".");

    } else {
      throw new IllegalArgumentException("Skipping message '" + messagePayload
          + "' because it's not in the format [email];[first-name];[last-name]");
    }


  }

  private void saveRegistrationInDb(String email, String firstName, String lastName) {

    // STEP 2: Save the registration in Cloud SQL - MySQL database.
    // Query might look something like "INSERT INTO registrants (email, first_name, last_name) VALUES (?, ?, ?)"
    System.out.println("STEP 4: Not completed yet.");

  }
}
