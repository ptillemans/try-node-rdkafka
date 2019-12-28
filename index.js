const Kafka = require('node-rdkafka');
console.log(Kafka.features);
console.log(Kafka.librdkafkaVersion);
var producer = new Kafka.Producer({
    'metadata.broker.list': 'nas.snamellit.com:9092'
});
// Connect to the broker manually
producer.connect();

// Wait for the ready event before proceeding
producer.on('ready', function() {
  try {
    producer.produce(
      // Topic to send the message to
      'testTopic',
      // optionally we can manually specify a partition for the message
      // this defaults to -1 - which will use librdkafka's default partitioner (consistent random for keyed messages, random for unkeyed messages)
      null,
      // Message to send. Must be a buffer
      Buffer.from('Awesome message'),
      // for keyed messages, we also specify the key - note that this field is optional
      'Stormwind',
      // you can send a timestamp here. If your broker version supports it,
      // it will get added. Otherwise, we default to 0
      Date.now(),
      // you can send an opaque token here, which gets passed along
      // to your delivery reports
    );
  } catch (err) {
    console.error('A problem occurred when sending our message');
    console.error(err);
  }
});

// Any errors we encounter, including connection errors
producer.on('event.error', function(err) {
  console.error('Error from producer');
  console.error(err);
})

// Print out delivery reports
producer.on('delivery-report', function(err, report) {
    // Report of delivery statistics here:
    //
    console.log(report);
});


//==================== Consuming ===========================

var consumer = new Kafka.KafkaConsumer({
    'group.id': 'kafka',
    'metadata.broker.list': 'nas.snamellit.com:9092',
    'offset_commit_cb': function(err, topicPartitions) {

        if (err) {
            // There was an error committing
            console.error(err);
        } else {
            // Commit went through. Let's log the topic partitions
            console.log(topicPartitions);
        }

    }}, {});

// Flowing mode
consumer.connect();

consumer
    .on('ready', function() {
        console.log('subscriber became ready.')
        consumer.subscribe(['testTopic']);

        // Consume from the librdtesting-01 topic. This is what determines
        // the mode we are running in. By not specifying a callback (or specifying
        // only a callback) we get messages as soon as they are available.
        consumer.consume();
    })
    .on('data', function(data) {
        // Output the actual message contents
        console.log(data.value.toString());
    });
