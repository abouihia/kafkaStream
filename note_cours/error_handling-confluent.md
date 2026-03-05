# Error Handling

In distributed systems error is always there,  but not every error is a stop-the-world situation.

Same error like  network partition errors, for example usually resolve pretty quickly.

Generally you need to provide a graceful mechanism for recovering form errors, and suitable to recovery to happend automatically if possible, and shut down only if only the situation is truly unrecoverable.

## Errors categories

we distingsh 3 broad categories of errors:

> * Entry consumer errors
> * Processing user logic
> * Exit producer error

### Entry

This type of error happens when records are coming in, and is usually a network or deserialization error.

Handled by
**DeserializationExceptionHandler** interface with default configuration of

* **LogAndFailExceptionHandler** interface( handle the log and then fail)
* **LogAndContinueExceptionHandler** interface( handle the log and then continue to run)

of course you can provide you own.

### Processing

exception related to logic that you provide will eventually bubble up and shut down the application.This could be related to mismatched types, for example.

Handled by
**StreamsUncaughtExceptionHandler**  it return an enum  with 3 option

1. replace the stream thread
2. shutdown the individual kafka stream instance
3. shutdown all kafka streams instances

### Exit

This type of error happend when writing records to a Kafka topics and is usually related to network or serialization errors.

Handled by
**ProductionExceptionHandler** interface  and you can repond by continuing/failing the process, this applies to exception not handled by kafka
