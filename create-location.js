const { Kafka } = require('kafkajs'),
fs = require('fs'),
uuidv1 = require('uuid/v1'),
commandLineArgs = require('command-line-args'),
commandLineUsage = require('command-line-usage');

// Set up command line arguments and help.
const sections = [
  {
    header: 'location-cli',
    content: 'Inserts and reads a location into the Digital Enterprise Data Fabric'
  },
  {
    header: 'Options',
    optionList: [
      {
        name: 'locationId',
        alias: 'i',
        typeLabel: '{underline id}',
        type: String,
        description: 'A string representing the id of the location'
      },
      {
        name: 'locationDescription',
        alias: 'd',
        typeLabel: '{underline description}',
        type: String,
        description: 'A string describing the location'
      },
      {
        name: 'createNew',
        alias: 'n',
        type: Boolean,
        description: 'Create a new location record.'
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
var testData = require('./simple-create-location.json');
var kafka, producer, consumer, topics = {}, locationId = 1;

// The Kafka topic for creating new locations.
topics['createTopic'] = 'default.datafabric.transform.location';
// Ingestion complete topic.
topics['locationCompleteTopic'] = 'default.datafabric.storage.location';

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
    clientId: 'listener-cli',
    brokers: [kafkaHost + ':' + kafkaPort]
  });
  producer = kafka.producer();
  consumer = kafka.consumer({ groupId: 'test-group' });

  const run = async () => {
    await producer.connect()

    await consumer.connect()
    await consumer.subscribe({ topic: topics['locationCompleteTopic'], fromBeginning: true })

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        var msg;

        try {
          msg = JSON.parse(message.value);
          if (insertedEntitys.has(msg.event.data.entityUUID)) {
            console.log("Got completion response for location creation " + msg.event.data.entityUUID);
            console.log({
              partition,
              offset: message.offset,
              value: JSON.stringify(msg),
            })
          }
        }
        catch(error) {
          console.log("Error: ", error, message.value);
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
  // Generate a uuid for the breadcrumb so we can track the message for debugging if we want to.
  data.event.breadcrumbId = uuidv1();
  data.event.data.locations.event.breadcrumbId = uuidv1();

  if (options.createNew) {
    // By assigning a unique value to the entity UUID we will create a new location record.
    // Otherwise if the entity UUID already exists, the existing record will be overwritten.
    data.event.data.locations.event.data.entityUUID = uuidv1();
    // Shouldn't set a location UUID if creating a new record.
    data.event.data.locations.event.data.location.locationUUID = "";
    console.log("Entity UUID set to " + data.event.data.locations.event.data.entityUUID);
  }

  if (options.locationId) {
    // If specified at the command line, set the locationId.
    data.event.data.locations.event.data.location.locationId = String(options.locationId);
  }

  if (options.locationDescription) {
    // Set the location description if supplied on the command line.
    data.event.data.locations.event.data.location.locationDescription = String(options.locationDescription);
  }

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
  if (options.createNew) {
    console.log("Sent message to create new location: " + data.event.data.locations.event.data.entityUUID);
    insertedEntitys.add(data.event.data.locations.event.data.entityUUID);
  }
  else {
    console.log("Sent location update message: ");
  }
}

(function init() {
  if (options.help) {
    console.log(usage);
  }
  else {
    setupKafka();
  };
})();
