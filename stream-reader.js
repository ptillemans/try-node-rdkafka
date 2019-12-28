const Kafka = require('node-rdkafka');


const globalOptions = {
    'group.id': 'kafka',
    'metadata.broker.list': 'nas.snamellit.com:9092'
};

const topicOptions = {};

var stream = Kafka.Consumer.createReadStream(globalOptions, topicOptions, {
    topics: ['testTopic']
});

stream.on('data', function(message) {
    console.log('Got message');
    console.log(message.value.toString());
});

stream.on('error', function(err) {
    console.log('ERROR:' + err);
});

stream.on('close', () => console.log('stream closed.'));

setTimeout(() => stream.close(), 10000);
