const { Kafka } = require('kafkajs'),
fs = require('fs'),
uuidv1 = require('uuid/v1'),
commandLineArgs = require('command-line-args'),
commandLineUsage = require('command-line-usage');

// Set up command line arguments and help.
const sections = [
  {
    header: 'find-workorder-data',
    content: 'Finds work order records in Digital Enterprise Data Fabric'
  },
  {
    header: 'Options',
    optionList: [
      {
        name: 'workorderUUID',
        alias: 'i',
        typeLabel: '{underline uuid}',
        type: String,
        description: 'A string representing the UUID of the work order you wish to search for'
      },
      {
        name: 'all',
        alias: 'a',
        type: Boolean,
        description: 'Returns all work orders. Overrides any other query criteria.'
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
// Define a JSON template for the request (in DEIM format de.workorder.api.retrieval.request-1.0.0.json format).
findIdTemplate = {
  "query":"findByWorkOrderIds",
  "ids":[]
},
findAllTemplate = {
  "query": "findAll"
};

// Define globals.
var kafka, producer, consumer, topics = {};

// The Kafka topic for making find requests.
topics['findTopic'] = 'default.datafabric.storage.api.request.work.order';
// The Kafka topic for receiving the response to a find request.
topics['findResponseTopic'] = 'default.datafabric.storage.api.response.work.order';

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

  if (!options.workorderUUID && !options.all) {
    console.log("Error: Must specify either a work order id or all.");
    console.log(usage);
    return;
  }

  kafka = new Kafka({
    clientId: 'find-workorder-data',
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
          msg = message.value.toString();
          var results = JSON.parse(msg);

          if (options.all) {
            console.log("Received response from " + topics['findResponseTopic'] + " to find all request.", JSON.stringify(results));
          }
          if (results.id = options.workorderUUID) {
            console.log("Received response from " + topics['findResponseTopic'] + " to find-by-id request.", JSON.stringify(results));
          }
        }
        catch(error) {
          console.log("Error: Received message value from " + topics['findResponseTopic'] + " was not JSON", msg, error);
        }
      },
    })
  }

  run().then(function() { sendFindRequest() }).catch(console.error);
}

function sendFindRequest() {
  // Update the search parameters according to the supplied values and make a find request.
  if (options.all) {
    sendMessage(findAllTemplate, 'findTopic');
  }
  else if (options.workorderUUID) {
    findIdTemplate.ids.push(options.workorderUUID.toString());
    sendMessage(findIdTemplate, 'findTopic');
  }
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
  console.log("Sent work order find message to " + topics[topicName] + ": ", data);
}

(function init() {
  if (options.help) {
    console.log(usage);
  }
  else {
    setupKafka();
  };
})();
