import Redis from "ioredis";
import mysql from "mysql2/promise";
import fs from 'fs';
import axios from "axios";

let mysqlPool;

const replayProductWithoutPusher = true;
const replayProductWithPusher = false;
const killProduct = false;
const killExchange = false;
const nukeAllInstabid = false;

const product = "prod";
const exchange = "Insta";
const apiEndpoint = 'https://api.instabid.io/order';

// Declare the Redis client
let client;

if (typeof client === 'undefined') {
  console.log('Reconnecting to Redis');
  client = new Redis("redis://default:SuP8KPQNWrb336crYxlaZed5PitswCJR@redis-11245.c98.us-east-1-4.ec2.cloud.redislabs.com:11245");
  console.log('connected to redis');
  client.defineCommand("nuke", {
    numberOfKeys: 0,
    lua: fs.readFileSync('./lua/nuke.lua'),
  });
  client.defineCommand("killProduct", {
    numberOfKeys: 0,
    lua: fs.readFileSync('./lua/killProduct.lua'),
  });
  client.defineCommand("orderExecute", {
    numberOfKeys: 1,
    lua: fs.readFileSync('./lua/order.lua'),
  });

}

const pool = mysql.createPool({
    host: "instabid.cf0trhlypqg1.us-east-2.rds.amazonaws.com",
    user: "admin",
    password: "Instabid123!!!",
    database: "Instabid",
});

async function killProductDb(exchange, product) {
  const query = `
  DELETE FROM orders WHERE exchange = '` + exchange + `' and product = '` + product + `'
`;
try {
  await mysqlPool.query(query);
  console.log("successfully nuked " + exchange + "/" + product + " db");
} catch(eDb) {
  console.error("DB Nuke error: ", eDb);
}
}

async function nukeWholeDb() {
    const query = `
      DELETE FROM orders
    `;
    try {
      await mysqlPool.query(query);
      console.log("successfully nuked db");
    } catch(eDb) {
      console.error("DB Nuke error: ", eDb);
    }
}

async function processOrder(order) {
  try {
    console.log("Sending order to Redis");
    const timestampOld = new Date().getTime();
    console.log(`order right before: qty: ${order.qty} price: ${order.price}`)
    let result = await client.orderExecute(0, order.exchange, order.product, order.side, order.qty, order.price, order.timestamp, order.user);
    const timestampNew = new Date().getTime();
    console.log("LATENCY REDIS ", timestampNew - timestampOld)
    console.log('order processed correctly in Redis');
    console.log('Attempting to store in Mysql');
    let orderResponse = JSON.parse(result[0]);
    let depthResponse = JSON.parse(result[1]);
    return [orderResponse,depthResponse];
  } catch (err) {
    console.error("couldn't process the order. Likely an error with Redis or the lua script:", err);
  }
}


async function replayWithoutPusher(mysqlPool, exchange, product) {
  console.log("in replay");
  try {
    // Create a connection to the MySQL pool
    

    // Execute SELECT query
    const [rows] = await mysqlPool.query(`
      SELECT orderNumber, exchange, product, side, qty, price, timestamp, user
      FROM orders WHERE exchange = '` + exchange + `' and product = '` + product +  `'
    `);

    // POST each order to the API
    for (const row of rows) {
      const order = {
        orderNumber: row.orderNumber,
        exchange: row.exchange,
        product: row.product,
        side: row.side,
        qty: parseInt(row.qty),
        price: parseFloat(row.price),
        timestamp: row.timestamp,
        user: row.user
      };
      console.log("Here's the submitted order: " + JSON.stringify(order));
      try {
        let response = await processOrder(order);
        console.log(`Order ${order.orderNumber} posted to Redis successfully: `, JSON.stringify(response));
      } catch (error) {
        console.error(`Error posting to Redis order ${order.orderNumber}: `, error.message);
      }
    }

    // Close the connection
    pool.end();
  } catch (error) {
    console.error('Error: ', error.message);
  }
}


async function replay(mysqlPool, exchange, product) {
  console.log("in replay");
  try {
    // Create a connection to the MySQL pool
    

    // Execute SELECT query
    const [rows] = await mysqlPool.query(`
      SELECT orderNumber, exchange, product, side, qty, price, timestamp, user
      FROM orders WHERE exchange = '` + exchange + `' and product = '` + product +  `'
    `);

    // POST each order to the API
    for (const row of rows) {
      const order = {
        orderNumber: row.orderNumber,
        exchange: row.exchange,
        product: row.product,
        side: row.side,
        qty: row.qty,
        price: row.price,
        timestamp: row.timestamp,
        user: row.user
      };

      try {
        const response = await axios.post(apiEndpoint, order);
        console.log(`Order ${order.orderNumber} posted successfully: `, response.data);
      } catch (error) {
        console.error(`Error posting order ${order.orderNumber}: `, error.message);
      }
    }

    // Close the connection
    pool.end();
  } catch (error) {
    console.error('Error: ', error.message);
  }
}

export const handler = async(event) => {
  let result = "";

    if (typeof mysqlPool === 'undefined') {
      console.log("had to reconnect to mysql! Yikes! That takes time!")
      mysqlPool = mysql.createPool({
        host: "instabid.cf0trhlypqg1.us-east-2.rds.amazonaws.com",
        user: "admin",
        password: "Instabid123!!!",
        database: "Instabid",
      });
    }

    if (replayProductWithoutPusher) {
      result = await client.killProduct(exchange, product);
      await replayWithoutPusher(mysqlPool, exchange, product);
      
    }

    if (replayProductWithPusher && !replayProductWithoutPusher) {
      result = await client.killProduct(exchange, product);
      await replay(mysqlPool, exchange, product);
      
    }

    if (killProduct && !replayProduct) {
      console.log("Time to kill the Product: " + exchange + ":" + product);
      result = await client.killProduct(exchange, product);
      result = await killProductDb(exchange, product);
    }

    if (nukeAllInstabid && !killProduct && !killExchange) {
      console.log("Time to nuke everything!")
      result = await client.nuke();
      result = await nukeWholeDb();
    }

    console.log(result);
    return result;
};
