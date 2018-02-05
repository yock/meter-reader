#!/usr/bin/env node

import { MongoClient } from 'mongodb';
import { curry } from 'lodash';
import { startOfDay, getHours, getMinutes } from 'date-fns';

let firstChunk = true;
let lastConsumptionValue = 0;

const onError = (error) => { 
  if (error) console.error(error);
}

const insertDocument =  (collection, document) => {
  collection.insertOne(document, onError);
}

const recordTimeSeries = (collection, document) => {
  const incrementValue = document['Message']['Consumption'] - lastConsumptionValue;
  const ts = document['Time'];
  collection.update(
    {date: startOfDay(ts)},
    {
      $inc: {
        consumption: incrementValue,
        "hours.$[hour].consumption": incrementValue,
        "hours.$[hour].minutes.$[minute].consumption": incrementValue
      }
    },
    {
      arrayFilters: [{ hour: getHours(ts), minute: getMinutes(ts) }],
      upsert: true,
    }
  )
}

MongoClient.connect('mongodb://localhost:27017', (err, client) => {
  const db = client.db('electric_meter_readings');
  const logDocument = curry(insertDocument)(db.collection('log'));
  process.stdin.on('data', (chunk) => {
    const document = JSON.parse(chunk)
    const readingDate = startOfDay(document['Time']);
    if (firstChunk) {
      firstChunk = false;
      lastConsumptionValue = document['Message']['Consumption'];
    }
    logDocument(document);
    recordTimeSeries(db.collection('time_series'), document);
    lastConsumptionValue = document['Message']['Consumption'];
  });
  process.stdin.on('end', client.close);
});
