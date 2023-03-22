# Project: STEDI Human Balance Analytics

## Introduction

In this project, you'll act as a data engineer for the STEDI team to build a data lakehouse solution for sensor data that trains a machine learning model.

### Project Details

The STEDI Team has been hard at work developing a hardware STEDI Step Trainer that:

- trains the user to do a STEDI balance exercise;
- and has sensors on the device that collect data to train a machine-learning algorithm to detect steps;
- has a companion mobile app that collects customer data and interacts with the device sensors.
STEDI has heard from millions of early adopters who are willing to purchase the STEDI Step Trainers and use them.

Several customers have already received their Step Trainers, installed the mobile application, and begun using them together to test their balance. The Step Trainer is just a motion sensor that records the distance of the object detected. The app uses a mobile phone accelerometer to detect motion in the X, Y, and Z directions.

The STEDI team wants to use the motion sensor data to train a machine learning model to detect steps accurately in real-time. Privacy will be a primary consideration in deciding what data can be used.

Some of the early adopters have agreed to share their data for research purposes. Only these customers’ Step Trainer and accelerometer data should be used in the training data for the machine learning model.

### Implementation

The python code for glue jobs have been implemented by python file in src folder generated automatically from glue studio
The customer landing result and accelero landing result has been captured and saved into screenshots folder.
The csv result files are the visualized results of the screenshots above, because of the limited screen resolution.

#### ** Glue Tables**
``` plantuml
@startuml
entity "customer_landing" as customer_landing {
  customerName: string,
  email: string,
  phone" string,
  birthDay: string,
  serialNumber: string,
  registrationDate: bigint,
  lastUpdateDate: bigint,
  shareWithResearchAsOfDate: bigint,
  shareWithPublicAsOfDate: bigint
}
entity "customer_trusted" as customer_trusted {
    customerName: string,
    email: string,
    phone" string,
    birthDay: string,
    serialNumber: string,
    registrationDate: bigint,
    lastUpdateDate: bigint,
    shareWithResearchAsOfDate: bigint,
    shareWithPublicAsOfDate: bigint
}
entity "customer_curated" as customer_curated {
    customerName: string,
    email: string,
    phone" string,
    birthDay: string,
    serialNumber: string,
    registrationDate: bigint,
    lastUpdateDate: bigint,
    shareWithResearchAsOfDate: bigint,
    shareWithPublicAsOfDate: bigint
}
entity "accelerometer_landing" as accelerometer_landing {
   user: string,
   timeStamp: bigint,
   x: float,
   y: float,
   z: float
}
entity "accelerometer_trusted" as accelerometer_trusted {
   user: string,
   timeStamp: bigint,
   x: float,
   y: float,
   z: float
}
entity "step_trainer_trusted" as step_trainer_trusted {
  sensorReadingTime: bigint,
  serialNumber: string,
  distanceFromObject: bigint
}
entity "machine_learning_curated" as machine_learning_curated {
  timestamp: bigint,
  x: float,
  y: float,
  z: float,
  sensorreadingtime: bigint,
  serialnumber: string,
  distancefromobject: bigint
}
@enduml
```

