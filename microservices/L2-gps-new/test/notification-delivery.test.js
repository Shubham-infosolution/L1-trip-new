'use strict';
const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

function load(failDetails = false) {
  const calls = [], published = [];
  let committed = false;
  const keys = new Map();
  const client = {
    async set(key,token) { if(keys.has(key)) return null; keys.set(key,token); return 'OK'; },
    async eval(script,count,key,token) { if(keys.get(key)===token) keys.delete(key); },
    async publish(channel,raw) { assert.equal(committed,true);published.push({channel,payload:JSON.parse(raw)}); }
  };
  const db = {async withTransaction(work) {
    const result = await work({async query(sql, values) {
      calls.push({sql, values});
      if (failDetails === true && sql.includes('tbl_notification_details')) throw Object.assign(new Error('details failed'), {transactionRolledBack:true});
      return {insertId: 42, affectedRows: 1};
    }});
    if (failDetails === 'commit') throw Object.assign(new Error('commit outcome unknown'), {transactionRolledBack:false});
    committed = true;
    return result;
  }};
  const context = {module:{exports:{}}, console, require(name) {
    if (name === '../model/dbquery') return db;
    if (name === 'node:crypto') return require(name);
    if (name === '../db/settings') return require('../src/db/settings');
    return {getClient:() => client};
  }};
  vm.runInNewContext(fs.readFileSync('src/services/notificationDelivery.js','utf8'), context);
  return {deliver:context.module.exports, calls, published, keys};
}

test('one shared notification, distinct recipient details, then full socket payloads', async () => {
  const {deliver, calls, published} = load();
  const row = {user_id:7, title:'Overspeed', message:'Speed 80', vehicle_no:'JH01', district_id:3};
  const result = await deliver([row, {...row, user_id:8}, row]);
  assert.equal(result.affectedRows, 1);
  assert.equal(calls.length, 2);
  for (const call of calls) {
    assert.match(call.sql, /CONVERT_TZ\(NOW\(\), @@session.time_zone, '\+05:30'\)/);
  }
  const columns = calls[0].sql.split('(')[1].split(')')[0].split(',');
  for (const name of ['user_id','is_read','read_at','notify_date_time']) assert.ok(!columns.includes(name));
  assert.deepEqual(Array.from(calls[1].values), [7,42,null,8,42,null]);
  assert.equal(published.length, 2);
  assert.equal(published[0].channel, 'jsfc:notification:new');
  assert.equal(published[0].payload.userId, 7);
  assert.equal(published[0].payload.message, 'Speed 80');
});

test('details failure prevents commit and socket publish', async () => {
  const {deliver, published} = load(true);
  await assert.rejects(deliver([{user_id:7,title:'Alert'}]), /details failed/);
  assert.equal(published.length, 0);
});

test('master alert name is used for each occurrence and shared among its recipients', async () => {
  const deliveries = [];
  let lookups = 0;
  const context = {module:{exports:{}}, require(name) {
    if (name === '../utils/vehicleAlertIdentity') return require('../src/utils/vehicleAlertIdentity');
    if (name === '../repositories/NotificationRepository') return {
      async getAlertNames() { lookups++; return [{id:2, alert_name:'Long Halt'}]; },
      async getStoreUsers() { return [{user_id:7},{user_id:8}]; }
    };
    return {async queueNotifications(data) { deliveries.push(data); }};
  }};
  vm.runInNewContext(fs.readFileSync('src/services/alertNotifications.js','utf8'), context);
  await context.module.exports([
    {alert_id:2,district_id:3,vehicle_no:'JH01',message:'First halt'},
    {alert_id:2,district_id:3,vehicle_no:'JH01',message:'Later halt'}
  ]);
  assert.equal(lookups, 1);
  assert.equal(deliveries.length, 2);
  for (const delivery of deliveries) {
    assert.equal(delivery.title, 'Long Halt');
    assert.equal(delivery.users.length, 2);
  }
});

test('replayed and concurrent deliveries save and publish once per event/user', async () => {
 const {deliver,calls,published}=load();
 const row={user_id:7,title:'Overspeed',vehicle_no:'JH01',notification_type:9,event_key:'event-1'};
 await Promise.all([deliver([row]),deliver([row])]);await deliver([row]);
 assert.equal(calls.length,2);assert.equal(published.length,1);
 await deliver([{...row,event_key:'event-2'}]);assert.equal(published.length,2);
 await deliver([{...row,user_id:8}]);assert.equal(published.length,3);
});
test('trip retry ignores formatting changes but preserves other trips', async () => {
 const {deliver,published}=load();
 const row={user_id:7,title:'Trip Started',vehicle_no:'JH01',notification_type:3,reference_id:42,message:'First'};
 await deliver([row]);await deliver([{...row,message:'Retry'}]);assert.equal(published.length,1);
 await deliver([{...row,reference_id:43}]);assert.equal(published.length,2);
});
test('confirmed rollback releases claims and publishes nothing', async () => {
 const {deliver,keys,published}=load(true);
 await assert.rejects(deliver([{user_id:7,title:'Alert',event_key:'event'}]),/details failed/);
 assert.equal(keys.size,0);assert.equal(published.length,0);
});

test('unknown commit outcome retains duplicate protection', async () => {
 const {deliver,keys,calls,published}=load('commit');
 const row={user_id:7,title:'Alert',event_key:'event'};
 await assert.rejects(deliver([row]),/commit outcome unknown/);
 assert.equal(keys.size,1);await deliver([row]);
 assert.equal(calls.length,2);assert.equal(published.length,0);
});
