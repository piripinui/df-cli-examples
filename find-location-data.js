const { Kafka } = require('kafkajs'),
fs = require('fs'),
uuidv1 = require('uuid/v1'),
commandLineArgs = require('command-line-args'),
commandLineUsage = require('command-line-usage');

// Set up command line arguments and help.
const sections = [
  {
    header: 'find-data',
    content: 'Finds location records using UUID values in Digital Enterprise Data Fabric'
  },
  {
    header: 'Options',
    optionList: [
      {
        name: 'locationUUID',
        alias: 'i',
        typeLabel: '{underline id}',
        type: String,
        description: 'REQUIRED: A string representing the UUID of the location you wish to search for'
      },
      {
        name: 'listTopics',
        alias: 'l',
        type: Boolean,
        description: 'List the existing Kafka topics.'
      },
      {
        name: 'kafkaHost',
        alias: 'k',
        type: String,
        description: 'REQUIRED: The name of the Kafka host.'
      },
      {
        name: 'kafkaPort',
        alias: 'p',
        type: Number,
        description: 'REQUIRED: The Kafka port number.'
      },
      {
        name: 'help',
        alias: 'h',
        type: Boolean,
        description: 'Print this usage guide.'
      }
    ]
  }
],
usage = commandLineUsage(sections),
options = commandLineArgs(sections[1].optionList),
// Define a JSON template for the request (in DEIM format default.datafabric.storage.api.request.location-1.0.0.json format).
findTemplate = {
  query: "findByUUID",
  locationUUID: "xxxxxxxx-yyyy-zzzz-xxxx-xxxxxxxxxxxx"
};

// Define globals.
var kafka, producer, consumer, topics = {};

// The Kafka topic for making find requests.
topics['findTopic'] = 'default.datafabric.storage.api.request.location';
// The Kafka topic for receiving the response to a find request.
topics['findResponseTopic'] = 'default.datafabric.storage.api.response.location';

function setupKafka() {
  var existingTopics = [], kafkaHost, kafkaPort;

  if (options.kafkaHost) {
     kafkaHost = options.kafkaHost;
  }
  else {
    console.log("Error: The Kafka host name must be specified.");
    console.log(usage);
    return;
  }

  if (options.kafkaPort) {
    kafkaPort = options.kafkaPort;
  }
  else {
    console.log("Error: The Kafka port must be specified.");
    console.log(usage);
    return;
  }

  if (!options.locationUUID) {
    console.log("Error: The location UUID must be specified.");
    console.log(usage);
    return;
  }

  kafka = new Kafka({
    clientId: 'find-data',
    brokers: [kafkaHost + ':' + kafkaPort]
  });
  producer = kafka.producer();
  consumer = kafka.consumer({ groupId: 'test-group' });

  const run = async () => {
    await producer.connect()

    await consumer.connect()
    await consumer.subscribe({ topic: topics['findResponseTopic'], fromBeginning: false })

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        var msg;

        try {
          msg = message.value;
          var myLocation = JSON.parse(msg);

          if (myLocation.locationUUID = options.locationUUID) {
            console.log("Received response to find request.", JSON.stringify(myLocation));
          }
        }
        catch(error) {
          console.log("Error: Received message value was not JSON", message.value);
        }
      },
    })
  }

  run().then(function() { sendFindRequest() }).catch(console.error);
}

function sendFindRequest() {
  // Update the UUID according to the supplied value and make a find request.
  findTemplate.locationUUID = options.locationUUID;
  sendMessage(findTemplate, 'findTopic');
}

function sendMessage(data, topicName) {
  // Send the provided data to the provide topic.
  producer.send({
    topic: topics[topicName],
    messages: [
      {
        value: JSON.stringify(data),
        // Need to specify headers to set the name of the topic to respond on.
        headers: {
            'kafka_replyTopic': topics['findResponseTopic']
        }
      }
    ],
  });
  console.log("Sent location update message to " + topics[topicName] + ": ", data);
}

(function init() {
  if (options.help) {
    console.log(usage);
  }
  else {
    setupKafka();
  };
})();
