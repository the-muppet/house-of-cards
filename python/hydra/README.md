## Hydra Application Overview
Specifically focused on processing TCG pricing data, the Hydra application is a cloud-native, microservices-based scraper system designed to handle complex workflows quickly and effeciently.
The Hydra leverages a range of Google Cloud Platform (GCP) services, such as Cloud Run, Cloud Functions, Cloud Scheduler, Pub/Sub and BigQuery (BQ) to create a scalable and efficient architecture.

## Functional Workflow
### Workflow Initiation (Controller):
- The controller service, which exposes an API endpoint for initiating the processing workflow requires API key authentication via secured POST request.
- It accepts a payload of comma separated TCG IDs or, in the absence of - fetches all TCG IDs from BQ.
- In either case, the result set is split into batches and published to the processing topic for the dispatcher.

### Task Dispatching (Dispatcher):
- The dispatcher receives the list of TCG IDs, further divides them into smaller batches for processing, and then carries out a staggered batch publishing to Pub/Sub.
- This component ensures that tasks are manageable and evenly distributed accross regions, enhancing processing efficiency.

### Data Processing (Worker):
- Workers subscribe to the Pub/Sub topic, receiving batches of TCG IDs for processing.
- Each ID within the batch is used to construct a URL, to which a POST request is sent.
- These requests are carried out linearly within each Worker until the batch is finished, or a non-200 response is recieved.
- Successful calls are processed and published to the success topic for consumption by the reciever.
- A failed call causes the Worker to cease its POST activity, instead publishing the remainder of its batch to the failure topic, again to the Reciever.

### Data Reception and Storage (Receiver):
- The Receiver acts upon the results from the Worker processes.
- Messages pulled from the success topic are simply batched together and appended to a table in BQ via streaming insert.
- Messages pulled from the failure topic however, have a few effects, and are where the application gets its name:
    + The remaining IDs are sent back to the dispatcher for redistribution.
    + The controller is notified of the failure and sends a kill signal to terminate the failed worker function.
    + The signal to terminate one worker is accompanied by _two_ signals to neighboring regions to each spawn a new Worker process.
    + The url endpoint of each new workers is passed to the dispatcher so that they can be assigned batches to process.

### Repeat until all batches are finished.
  Sounds like a good time, right?
