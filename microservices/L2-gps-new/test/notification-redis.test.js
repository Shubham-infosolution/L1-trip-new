'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const Module = require('node:module');
let failPush=false,rows=[],pushCalls=[],deleted=0;
const load=Module._load;
Module._load=function(name,...args) {
 if(name==='ioredis') return class {
  on() { return this; }
  async rpush(...args) {pushCalls.push(args);if(failPush)throw new Error('WRONGTYPE');return args.length-1;}
  async eval() {return rows;}
  async del(){deleted++;}
 };
 return load.call(this,name,...args);
};
const Redis=require('../src/repositories/RedisRepository');Module._load=load;
test('notification enqueue uses one RPUSH and rejects Redis command errors',async()=>{
 assert.equal(await Redis.pushNotifications('geofence',[{user_id:1},{user_id:2}]),2);
 assert.equal(pushCalls.length,1);assert.equal(pushCalls[0][0],'notification:geofence');assert.equal(pushCalls[0].length,3);
 failPush=true;await assert.rejects(Redis.pushNotifications('geofence',[{user_id:1}]),/WRONGTYPE/);failPush=false;
});
test('malformed claimed notification is retained with processing key in error',async()=>{
 rows=[JSON.stringify({user_id:1}),'{bad-json'];
 await assert.rejects(Redis.moveNotificationBatch('geofence'),err=>Boolean(err.processingKey?.startsWith('notification:geofence:processing:')));
 assert.equal(deleted,0);
 rows=[JSON.stringify({user_id:0})];await assert.rejects(Redis.moveNotificationBatch('geofence'),/Invalid notification/);
});
