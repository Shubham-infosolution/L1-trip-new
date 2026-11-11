'use strict';
const { test, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const Module = require('node:module');
const queues = new Map(), processing = new Map(), inserted = [];
let users, insertError, enqueueError, recipientArgs, cronExpression, schedules = 0;
const Redis = {
 async pushNotifications(type, rows) {
  if(enqueueError) throw enqueueError;
  queues.set(type, [...(queues.get(type)||[]), ...rows]); return rows.length;
 },
 async getNotificationQueueLength(type) {return (queues.get(type)||[]).length;},
 async moveNotificationBatch(type, limit) {
  const rows=(queues.get(type)||[]).splice(0,limit), key=`notification:${type}:processing:test`;
  processing.set(key,rows); return {key,notifications:rows};
 },
 async deleteNotificationProcessingQueue(key) {processing.delete(key);}
};
const Repository={
 async getStoreUsers(...args) {recipientArgs=args;return users;},
 async bulkInsertNotifications(rows) {
  if(insertError) throw insertError;
  inserted.push(...rows);return {affectedRows:1,notifications:rows};
 }
};
const load=Module._load;
Module._load=function(name,...args) {
 if(name==='../repositories/RedisRepository') return Redis;
 if(name==='../repositories/NotificationRepository') return Repository;
 if(name==='../utils/managedCron') return {schedule(expression,callback) {cronExpression=expression;schedules++;return {callback};}};
 if(name==='../helpers/geoHelper') return {formatServerTime:async()=> '2026-09-16 12:00:00'};
 return load.call(this,name,...args);
};
const Service=require('../src/services/notification.service');
Module._load=load;
Service.getPacketReceiveTime=async()=> '2026-09-16 12:00:00';
beforeEach(()=>{queues.clear();processing.clear();inserted.length=0;users=[{user_id:7},{user_id:8}];insertError=null;enqueueError=null;});

function input(event='TRIP_STARTED') {
 return {vehicle:{id:1,vehicle_no:'JH01',level:2,district_id:5,transporter_id:30},payload:{gps_timestamp:100},tripResult:{event,trip:{id:40,trip_id:'T40',driver_id:31,source_store_id:12,destination_store_id:13},subTrip:{id:41,destination_store_id:14}},distance:0.03};
}
for (const [event,title,type,store] of [
 ['TRIP_STARTED','Trip Started',3,12],
 ['SUBTRIP_COMPLETED','Sub Trip Completed',6,14],
 ['TRIP_COMPLETED','Trip Completed',4,13]
]) test(event+' delivers immediately without Redis queue',async()=>{
 assert.equal(await Service.process(input(event)),true);
 assert.equal(inserted.length,2);
 assert.equal(queues.size,0);
 assert.deepEqual(recipientArgs,[5,store,event==='SUBTRIP_COMPLETED'?'4,7':'2,3,4',31,30]);
 assert.ok(inserted.every(row=>row.title===title && row.notification_type===type));
});
test('running trip does not duplicate notifications',async()=>{
 await Service.process(input('TRIP_RUNNING'));assert.equal(inserted.length,0);
});
test('delivery errors propagate from queueNotifications',async()=>{
 insertError=new Error('DB unavailable');
 await assert.rejects(Service.queueNotifications({type:'trip_started',users,vehicle:input().vehicle,title:'Trip Started'}),/DB unavailable/);
});
test('legacy queued rows are delivered and removed after success',async()=>{
 await Redis.pushNotifications('trip_started',[{user_id:7,title:'Trip Started'}]);
 await Service.processNotificationBatch('trip_started');assert.equal(inserted.length,1);assert.equal(processing.size,0);
});
test('legacy queued rows remain on delivery failure',async()=>{
 await Redis.pushNotifications('trip_started',[{user_id:7,title:'Trip Started'}]);insertError=new Error('DB unavailable');
 await Service.processNotificationBatch('trip_started');assert.equal(inserted.length,0);assert.equal(processing.size,1);
});
test('legacy queue processor starts once',()=>{
 assert.equal(cronExpression,'*/30 * * * * *');Service.startNotificationQueueProcessor();assert.equal(schedules,1);
});
