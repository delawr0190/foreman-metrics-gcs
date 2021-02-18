var moment = require('moment');
const { Storage } = require('@google-cloud/storage');

const storage = new Storage({projectId: process.env.PROJECT_ID || "silent-caster-188619"});
const bucket = storage.bucket(process.env.BUCKET || "foreman-dev-metrics");

async function uploadMetrics(payload) {
    const publishedTime = moment(new Date(payload.published));

    const year = publishedTime.get('year');
    const month = publishedTime.get('month');
    const day = publishedTime.get('date');
    const hour = publishedTime.get('hour');

    // Upload aggregates
    console.log(`Storing aggregates for ${payload.client}`);
    await bucket
      .file(`${payload.client}/aggregates/${year}/${month}/${day}/${hour}/${payload.pickaxe.id}/${publishedTime}.json`)
      .save(JSON.stringify(payload));

    for (const key in payload.miners) {
      const miner = payload.miners[key].miner;
      const metrics = payload.miners[key].metrics;

      const toStore = {
        'pickaxe': payload.pickaxe.id,
        'metrics': metrics,
        'published': payload.published,
        'received': payload.received
      };

      const identifier = miner.mac ? miner.mac : miner.id;

      // Upload individual miner metrics
      console.log(`Storage metrics for ${payload.client} - ${identifier}`);

      const file = bucket.file(`${payload.client}/split/${year}/${month}/${day}/${hour}/${identifier}/${publishedTime}.json`);
      await file.save(JSON.stringify(toStore));
      await file.setMetadata({
        metadata: {
          minerId: miner.id,
          minerType: miner.type
        }
      });
    }
}

exports.runPubSub = (event, context) => {
  if (event.data) {
    const message = Buffer.from(event.data, 'base64').toString();
    console.log(`Processing message: ${message}`);

    const payload = JSON.parse(message);
    uploadMetrics(payload).catch(console.error);
  } else {
    console.log('Message contained no data');
  }
};
