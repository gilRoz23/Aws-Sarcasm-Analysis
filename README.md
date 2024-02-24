# Sarcasm Analysis Application

## Overview
This project implements a real-world application for distributively processing a list of Amazon reviews. It performs sentiment analysis, named-entity recognition (NER), and displays the results on a web page. The goal is to experiment with AWS and detect sarcasm in reviews.

## System Architecture
The system consists of a local application, a manager, and workers. The local application uploads input files to S3, sends messages to an SQS queue, and handles the termination process. The manager distributes tasks to workers, monitors their progress, and creates a summary file. Workers process messages, perform sentiment analysis and NER, and return the results.

## Technologies Used
- Java
- AWS SDK for Java
- Stanford CoreNLP for sentiment analysis and NER

## Dependencies
- Maven
- AWS SDK for Java
- Stanford CoreNLP

## Running the Application
1. Build the project using Maven: `mvn clean package`
2. Run the local application: `java -jar yourjar.jar inputFileName1... inputFileNameN outputFileName1... outputFileNameN n [terminate]`
3. Run the worker: `java -jar yourjar.jar MainWorkerClass`

## Security
- Security credentials are stored securely and not included in plain text in the application.

## Scalability
- The system is designed to handle a large number of clients simultaneously by distributing tasks efficiently among workers.

## Persistence
- The system handles node failures and stalls, ensuring that tasks are completed even in the event of failures.

## Threads
- Threads are used judiciously in the application to improve performance and efficiency.

## Termination Process
- The termination process is managed carefully to ensure that all components are properly closed and no resources are left open.

## Limitations
- AWS limitations are considered in the system design to utilize resources effectively.

## Conclusion
This project demonstrates the implementation of a distributed system for analyzing Amazon reviews for sentiment and sarcasm detection. The system is scalable, efficient, and handles failures gracefully.
