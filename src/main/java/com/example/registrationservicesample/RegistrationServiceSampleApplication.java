package com.example.registrationservicesample;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class RegistrationServiceSampleApplication implements CommandLineRunner {

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

      // STEP 2: Log to Stackdriver Logging that you processed the message.
      //         The message should something like:
      //         "Processed registration for <john@doe.com> John Doe."
      System.out.println("STEP 2: Not completed yet.");


    } else {
      throw new IllegalArgumentException("Skipping message '" + messagePayload
          + "' because it's not in the format [email];[first-name];[last-name]");
    }


  }

  private void saveRegistrationInDb(String email, String firstName, String lastName) {

    // STEP 4: Save the registration in Cloud SQL - MySQL database.
    // Query might look something like "INSERT INTO registrants (email, first_name, last_name) VALUES (?, ?, ?)"
    System.out.println("STEP 4: Not completed yet.");

  }
}
