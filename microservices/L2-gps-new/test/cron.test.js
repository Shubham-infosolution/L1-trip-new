'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const Module = require('node:module');
let tick,stops=0,destroys=0;
const load=Module._load;
Module._load=function(name,...args) {
 if(name==='node-cron') return {schedule(_expression,callback) {tick=callback;return {stop(){stops++;},destroy(){destroys++;}};}};
 return load.call(this,name,...args);
};
const cron=require('../src/utils/managedCron'); Module._load=load;
test('scheduled ticks never overlap and stopAll waits for active work',async()=>{
 let release,runs=0;
 cron.schedule('* * * * *',async()=>{runs++;await new Promise(resolve=>{release=resolve;});});
 const first=tick();await Promise.resolve();await tick();assert.equal(runs,1);
 let done=false; const stopping=cron.stopAll().then(()=>{done=true;});
 await Promise.resolve();assert.equal(done,false);release();await first;await stopping;
 assert.equal(stops,1);assert.equal(destroys,1);await tick();assert.equal(runs,1);
});
