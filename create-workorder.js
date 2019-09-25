const { Kafka } = require('kafkajs'),
fs = require('fs'),
uuidv1 = require('uuid/v1'),
commandLineArgs = require('command-line-args'),
commandLineUsage = require('command-line-usage');

// Set up command line arguments and help.
const sections = [
  {
    header: 'create-workorder',
    content: 'Inserts or updates a work order into the Digital Enterprise Data Fabric'
  },
  {
    header: 'Options',
    optionList: [
      {
        name: 'workorderId',
        alias: 'i',
        typeLabel: '{underline id}',
        type: String,
        description: 'A string representing the id of the work order'
      },
      {
        name: 'workorderDescription',
        alias: 'd',
        typeLabel: '{underline description}',
        type: String,
        description: 'A string describing the work order'
      },
      {
        name: 'filename',
        alias: 'f',
        type: String,
        description: 'The path of a JSON document defining the message to send. If not specified defaults to an example document.'
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
options = commandLineArgs(sections[1].optionList);

// Used to "remember" which record identity we inserted.
const insertedEntitys = new Set();

// Load in the default message content and define globals.
var testData = require('./simple-workorder-create.json');
var kafka, producer, consumer, topics = {};

// The Kafka topic for creating new work orders.
topics['createTopic'] = 'default.datafabric.transform.work.order';
// Ingestion complete topic.
topics['completeTopic'] = 'default.datafabric.storage.work.order';

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

  kafka = new Kafka({
    clientId: 'create-workorder',
    brokers: [kafkaHost + ':' + kafkaPort]
  });
  producer = kafka.producer();
  consumer = kafka.consumer({ groupId: 'test-group' });

  const run = async () => {
    await producer.connect()

    await consumer.connect()
    await consumer.subscribe({ topic: topics['completeTopic'], fromBeginning: true })

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        var msg;

        try {
          msg = JSON.parse(message.value);
          if (insertedEntitys.has(msg.event.data.workOrderId)) {
            console.log("Got completion response for work order " + msg.event.data.workOrderId);
            console.log({
              partition,
              offset: message.offset,
              value: JSON.stringify(msg),
            })
          }
        }
        catch(error) {
          console.log("Error: Received message value was not JSON", message.value);
        }
      },
    })
  }

  run().then(function() { sendCreateRequest() }).catch(console.error);
}

function sendCreateRequest() {
  // Modify the template data according to the command line parameters and send
  // a message request to insert/update.
  if (options.filename) {
    // Load the specified file and use it as the message content.
    fs.readFile(options.filename, function(err, data) {
      if (err) {
        console.log(err);
      }
      else {
        var msgData = JSON.parse(data);
        var myData = modifyMessageContent(msgData);
        sendMessage(myData, 'createTopic');
      }
    })
  }
  else {
    // Use the default message content.
    var myData = modifyMessageContent(testData);
    sendMessage(myData, 'createTopic');
  }
}

function modifyMessageContent(data) {

  if (options.workorderId) {
    // If specified at the command line, set the workorderId.

    data.event.data.workOrders.forEach(function(workorderEvent) {
      workorderEvent.event.data.workOrderId = String(options.workorderId);
    });
  }

  if (options.workorderDescription) {
    // Set the work order description if supplied on the command line.
    data.event.data.workOrders.forEach(function(workorderEvent) {
      workorderEvent.event.data.description = String(options.workorderDescription);
    });
  }

  // Set the raised date/time.
  data.event.data.workOrders.forEach(function(workorderEvent) {
    workorderEvent.event.data.raisedDateTime = Date.now();
  });

  return data;
}

function sendMessage(data, topicName) {
  // Send the provided data to the provide topic.
  producer.send({
    topic: topics[topicName],
    messages: [
      { value: JSON.stringify(data) },
    ],
  });

  console.log("Sent work order update message: ", data);
  data.event.data.workOrders.forEach(function(workorderEvent) {
    insertedEntitys.add(workorderEvent.event.data.workOrderId);
  });
}

(function init() {
  if (options.help) {
    console.log(usage);
  }
  else {
    setupKafka();
  };
})();
