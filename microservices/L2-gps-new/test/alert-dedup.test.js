'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const { createRequire } = require('node:module');
const identity = require('../src/utils/vehicleAlertIdentity');

function load(file, mocks) {
    const localRequire=createRequire(path.resolve(file));
    const context={module:{exports:{}},process:{env:{}},console,
        require(name){return Object.hasOwn(mocks,name)?mocks[name]:localRequire(name);}};
    vm.runInNewContext(fs.readFileSync(file,'utf8'),context,{filename:file});
    return context.module.exports;
}

test('event identity suppresses retries but preserves other trips, times, and halt milestones', () => {
    const row={vehicle_no:'JH13D9661',trip_id:40371,alert_id:13,last_date_time:'2026-10-23 12:00:00'};
    assert.equal(identity(row),identity({...row,vehicle_no:' jh13d9661 ',trip_id:'40371',speed:4,message:'changed duration'}));
    for(const change of [{trip_id:40372},{subtrip_id:5},{alert_id:14},{last_date_time:'2026-10-23 12:01:00'}]) {
        assert.notEqual(identity(row),identity({...row,...change}));
    }
    assert.notEqual(identity({...row,event_key:'halt:start:1'}),identity({...row,event_key:'halt:start:2'}));
    assert.equal(identity({vehicle_no:'JH01',alert_id:2}),null);
});

test('enqueue uses the atomic dedup path and treats a duplicate as successful delivery', async () => {
    const calls=[];
    const Redis=load('src/repositories/RedisRepository.js',{ioredis:class{
        on(){}
        async eval(script,count,queue,key,payload){calls.push({script,count,queue,key,payload});return calls.length===1?1:0;}
        async rpush(){throw Error('Unprotected enqueue must not be used');}
    }});
    const row={vehicle_no:'JH13D9661',trip_id:40371,alert_id:13,last_date_time:'2026-10-23 12:00:00'};
    assert.equal(await Redis.addVehicleAlert({...row}),true);
    assert.equal(await Redis.addVehicleAlert({...row}),true);
    assert.equal(calls[0].key,calls[1].key);
    assert.equal(calls[0].queue,'vehicle_alert_queue');
    assert.equal(calls[0].count,2);
});

test('continuous ignition OFF updates do not close the event or create another alert', async () => {
    let active={id:7}, inserts=0, queued=0, updates=0;
    const Service=load('src/services/ignitionOff.service.js',{
        '../repositories/TripRepository':{async getAlertConfig(){return {alert_time:5};}},
        '../repositories/RedisRepository':{
            async getIgnitionOffStart(){return '2026-10-23 12:00:00';},
            async addVehicleAlert(){queued++;return true;}
        },
        '../repositories/AlertEventRepository':{
            async getActiveAlertEvent(){return active;},
            async updateIdleEvent(){updates++;},
            async updateAlertEvent(){active=null;},
            async savealertsEventLog(){inserts++;active={id:8};}
        },
        '../helpers/geoHelper':{}
    });
    const trip={trip_id:40371,trip_unique_id:'TRIP-1'};
    for(const time of ['12:10:00','12:11:00','12:12:00']) {
        await Service.handleIgnitionOff({vehicle_no:'JH13D9661'}, {},trip,'JH13D9661',`2026-10-23 ${time}`,false,null,null);
    }
    assert.equal(updates,3);
    assert.equal(inserts,0);
    assert.equal(queued,0);
    assert.equal(active.id,7);
    await Service.closeOffEvent(7,12,false,null,null);
    assert.equal(active,null);
});
