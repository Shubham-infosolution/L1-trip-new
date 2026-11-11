'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const Module = require('node:module');
const { WriteBatch } = require('../src/db/writeBatch');
const calls = [], redisCalls = [];
let selectedRows = [{id:5}];
const fakeDb = { async query(sql,values) { calls.push({sql,values}); return {affectedRows: sql.includes('INSERT INTO tbl_vehicle_alert_details') ? values.length/14 : 1,insertId:123}; } };
const batch = new WriteBatch(fakeDb, {size:200,delay:1,limit:1000});
class Database {
  query(sql,values) { return fakeDb.query(sql,values); }
  async joinSelect(sql,values) { calls.push({sql,values}); return selectedRows; }
  withTransaction(work) { return work(this); }
}
const Redis = new Proxy({}, {get:(_,key)=>async (...args)=>{ redisCalls.push([key,...args]); return key.startsWith('get') ? null : true; }});
const originalLoad = Module._load;
Module._load = function(name,...args) {
  if (name === '../model/dbquery') return new Database();
  if (name === '../db/writeBatch') return batch;
  if (/RedisRepository$/.test(name)) return Redis;
  if (/geoHelper$/.test(name)) return {reverseGeocode:async()=>'',formatServerTime:async()=> '2026-09-16 12:00:00'};
  return originalLoad.call(this,name,...args);
};
const Events = require('../src/repositories/AlertEventRepository');
const Trips = require('../src/repositories/TripRepository');
const Notifications = require('../src/repositories/NotificationRepository');
Module._load = originalLoad;
test('all opted-in event update templates compile, bind values, and preserve guards', async () => {
  for (const name of ['updateAlertEvent','updateIdleEvent']) {
    const start=calls.length;
    await Promise.all([1,2].map(id=>Events[name](id, {end_lat:1,end_lng:2,lat:1,lng:2,off_route_in_km:3,alert_path:[],duration_minutes:5,end_date_time:'2026-09-16 12:00:00'})));
    assert.equal(calls.length,start+1,name);
    const call=calls.at(-1);
    assert.match(call.sql,/JOIN \(SELECT/); assert.match(call.sql,/AND status = 1/);
    assert.doesNotMatch(call.sql,/alert_path/i);
    assert.equal((call.sql.match(/\?/g)||[]).length, call.values.length,name);
  }
});
test('event creation remains immediate and retains exact insertId', async () => {
  const result=await Events.savealertsEventLog({trip_id:1,vehicle_no:'JH01',alert_id:2,start_lat:1,start_lng:2});
  assert.equal(result.insertId,123);
  assert.doesNotMatch(calls.at(-1).sql,/alert_path/i);
  assert.doesNotMatch(calls.at(-1).sql,/alert_path/i);
  assert.equal((calls.at(-1).sql.match(/\?/g)||[]).length,calls.at(-1).values.length);
  assert.doesNotMatch(calls.at(-1).sql,/UNION ALL/);
});
test('trip caches use correct level on reads and saves', async () => {
  await Trips.getActiveTrip('JH01');
  for (const name of ['getActiveTrip','saveActiveTrip']) {
    const call=redisCalls.find(c=>c[0]===name); assert.equal(call[2],2,name);
    if(name.startsWith('save')) assert.deepEqual(call[3],[{id:5}]);
  }
});
test('notification history inserts use valid multi-value SQL', async () => {
  const v={vehicle_no:'JH01',district_id:1}, t={id:1,trip_id:1,trip_unique_id:'T1'}, p={latitude:1,longitude:2,gps_timestamp:1};
  const operations = [
    ()=>Notifications.savegeo_fence_historyNotification({vehicle_no:'JH01'})
  ];
  for (const fn of operations) {
    const start=calls.length;
    await Promise.all([fn(),fn()]);
    assert.equal(calls.length,start+1);
    assert.equal((calls.at(-1).sql.match(/\?/g)||[]).length,calls.at(-1).values.length);
  }
  await batch.close();
});

test('notification recipient lookup parameterizes IDs and preserves recipient groups', async () => {
  await Notifications.getStoreUsers(5,12,'2,3,4',31,30);
  const call=calls.at(-1);
  assert.deepEqual(call.values,[5,5,12,2,3,4,31,30]);
  assert.equal((call.sql.match(/\?/g)||[]).length,call.values.length);
  assert.match(call.sql,/um.user_type = 2/);
  assert.match(call.sql,/uda.store_id IS NULL/);
  assert.match(call.sql,/um.user_type = 6/);
  assert.match(call.sql,/um.user_type = 5/);
});
test('missing district does not become an undefined SQL column', async () => {
  await Notifications.getStoreUsers(undefined,12,'2,3,4');
  const call=calls.at(-1);
  assert.doesNotMatch(call.sql,/undefined/); assert.deepEqual(call.values,[null,null,12,2,3,4]);
  await assert.rejects(Notifications.getStoreUsers(5,12,'2,invalid'),/Invalid notification userTypes/);
});

test('common alert batches preserve trip ownership and comma-separated locations', async () => {
  selectedRows = [40371,40372].map((id,i)=>({id,vehicle_no:i?'JH01':'JH13D9661',trip_status:1,status:1,level:2}));
  await Trips.bulkInsertAlertDetails([
    {trip_id:40371,vehicle_no:'JH13D9661',alert_id:2,last_location:'23.3,85.3'},
    {trip_id:40372,vehicle_no:'JH01',alert_id:7,speed:65,last_location:'23.4,85.4'},
    {trip_id:40373,vehicle_no:'JH02',alert_id:7,last_location:null}
  ]);
  const call=calls.at(-1);
  assert.equal((call.sql.match(/\?/g)||[]).length,call.values.length);
  assert.equal(call.values.length,28);
  assert.equal(call.values[0],40371);
  assert.equal(call.values[14],40372);
  assert.equal(call.values[5],'23.3,85.3');
  assert.equal(call.values[19],'23.4,85.4');
  assert.equal(call.values[22],65);
});

test('alerts without both valid coordinates do not execute an insert', async () => {
  const start = calls.length;
  const result = await Trips.bulkInsertAlertDetails(
    [null, undefined, '', '23.3,', ',85.3', 'null,null', '91,85', '23,85,1'].map(last_location => ({last_location}))
  );
  assert.equal(result.affectedRows, 0);
  assert.equal(calls.length, start);
});



test('final insert locks live trips and drops manually completed, missing and wrong-vehicle alerts', async () => {
 selectedRows = [
  {id:10,vehicle_no:'JH01',trip_status:2,status:1,level:2},
  {id:11,vehicle_no:'JH02',trip_status:1,status:1,level:2},
  {id:12,vehicle_no:'JH03',trip_status:1,status:0,level:2},
  {id:13,vehicle_no:'JH04',trip_status:1,status:1,level:1}
 ];
 const start=calls.length;
 const rows=[{trip_id:10,vehicle_no:'JH01'},{trip_id:11,vehicle_no:'JH02'},
 {trip_id:11,vehicle_no:'WRONG'},{trip_id:12,vehicle_no:'JH03'},
 {trip_id:13,vehicle_no:'JH04'},{trip_id:999,vehicle_no:'JH05'}].map(row=>({...row,last_location:'23,85'}));
 const result=await Trips.bulkInsertAlertDetails(rows);
 assert.equal(result.affectedRows,1);assert.equal(result.skippedCount,5);
 assert.deepEqual(result.insertedAlerts,[rows[1]]);
 assert.match(calls[start].sql,/ORDER BY id FOR UPDATE/);
 assert.deepEqual(calls[start].values,[10,11,12,13,999]);
 assert.equal(calls.at(-1).values[0],11);
});
test('completed-only batch performs no alert INSERT', async () => {
 selectedRows=[{id:10,vehicle_no:'JH01',trip_status:2,status:1,level:2}];
 const start=calls.length;
 const result=await Trips.bulkInsertAlertDetails([{trip_id:10,vehicle_no:'JH01',last_location:'23,85'}]);
 assert.equal(result.affectedRows,0);assert.equal(result.skippedCount,1);
 assert.equal(calls.length,start+1);assert.match(calls[start].sql,/FOR UPDATE/);
});
