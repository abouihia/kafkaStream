Handle errors in kafka Stream

3 componant in Kafka Stream And each step can throw exception
 Step                       error
    
topology
Serialization

| Step            |                   error Handler | Occur |
|:----------------|--------------------------------:|---------|
| DeSerialization | DeserialisationExceptionHandler |  Occur when incoming bytes cannot be converted into the expected object format. |
| topology        |  StreamUnCaughtExceptionHandler |  Runtime exceptions that happen during the processing logic within your Kafka Streams application, after deserialization|
| Serialization   |      ProductionExceptionHandler | Occur when an object cannot be successfully converted into bytes for sending to a Kafka topic |





Deserialization Errors: Occur when incoming bytes cannot be converted into the expected object format.
Topology Errors: Runtime exceptions that happen during the processing logic within your Kafka Streams application, after deserialization.
Serialization Errors: Occur when an object cannot be successfully converted into bytes for sending to a Kafka topic.

topology errors are runtime exceptions that occur during the processing logic defined in your 
Kafka Streams application (e.g., NullPointerException, NumberFormatException, ArithmeticException). 
They happen when the data, even if correctly deserialized, doesn't conform to the assumptions of your processing code.


A ProductionExceptionHandler specifically deals with errors that occur when the Kafka Streams application is trying to produce (send) a message to an output topic.
This happens after your processing logic has completed and you're ready to write the result back to Kafka.


Serialization is the process of converting an object (like a Java object) into a format that can be easily stored or transmitted, 
typically a sequence of bytes. In Kafka, this means turning your application's data into bytes that can be sent to a Kafka topic.

A serialization error occurs when this conversion process fails. This can happen for several reasons:

Data too large: The object you're trying to serialize might be too large to fit within Kafka's message size limits or the serializer's buffer.
Unsupported data type: The serializer might not know how to convert a particular field or type within your object into bytes.
Transient issues: Sometimes, there might be temporary problems with the Kafka cluster or network that prevent successful serialization and sending of the message.
