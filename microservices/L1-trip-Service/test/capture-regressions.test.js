'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const fixtures=require('./fixtures/capture-20261002-082854.json');
const {dbFixture}=require('../scripts/simulation-fixtures.cjs');
for(const item of fixtures){
 test(`13:58 IST capture ${item.row.vehicle_no}/${item.row.id} reports its actual blocker without invented writes`,async()=>{
  const f=dbFixture([item.row]),p=item.latest_packet;
  const packet={vehicle_no:item.row.vehicle_no,lat:p.latitude,lng:p.longitude,ts:p.gps_timestamp,packet_type:p.packet_type};
  const r=await f.repo.transitionBatch([packet],{save:async()=>{},geofences:{[item.row.id]:item.redis_state}});
  assert.equal(f.state.writes.length,0);
  assert.equal(r.intent.decisions[0].reason,Number(item.row.trip_status)===0?'SOURCE_EXIT_NOT_OBSERVED':'INVALID_DESTINATION_GEOFENCE');
 });
}
