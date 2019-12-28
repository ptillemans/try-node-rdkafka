const Kafka = require('node-rdkafka');


const globalOptions = {
    'group.id': 'kafka',
    'metadata.broker.list': 'nas.snamellit.com:9092'
};

var stream = Kafka.Producer.createWriteStream(
    globalOptions, {}, {
        topic: 'testTopic'
    });

// Writes a message to the stream
var queuedSuccess = stream.write(Buffer.from('Awesome message'));

if (queuedSuccess) {
    console.log('We queued our message!');
} else {
    // Note that this only tells us if the stream's queue is full,
    // it does NOT tell us if the message got to Kafka!  See below...
    console.log('Too many messages in our queue already');
}

// NOTE: MAKE SURE TO LISTEN TO THIS IF YOU WANT THE STREAM TO BE DURABLE
// Otherwise, any error will bubble up as an uncaught exception.
stream.on('error', function (err) {
    // Here's where we'll know if something went wrong sending to Kafka
    console.error('Error in our kafka stream');
    console.error(err);
});

stream.on('close', function() {
    console.log('stream closed.');
});

setTimeout(function() {
    stream.close();
}, 5000);
