'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
const {createRequire}=require('node:module');
const filename=path.resolve(__dirname,'../src/repository.js');
const original=fs.readFileSync(filename,'utf8');
// Exercise the actual transaction query path after normal SQL formatting edits.
// These changes reproduce the previous indexOf(-1)/slice(0,-1) production risk.
for(const [name,sqlWhere]of [
 ['default formatting',' WHERE tp.level'],
 ['WHERE at start of line','WHERE tp.level'],
 ['lowercase where and tab','\twhere tp.level']
]){
 test('transaction details SQL remains valid with '+name,async()=>{
  const module={exports:{}};
  const source=original.replace(/\n WHERE tp\.level/,'\n'+sqlWhere);
  vm.runInNewContext(source,{module,exports:module.exports,require:createRequire(filename),Buffer,structuredClone},{filename});
  let detailsQueries=0;
  const db={withTransaction:async work=>work(db),writeExecute:async(sql)=>{
   if(sql.includes('tp.*')&&sql.includes('tp.vehicle_no = ?')&&sql.includes('((tp.level')){
    detailsQueries++;
    assert.equal((sql.match(/\bwhere\b/gi)||[]).length,1,'transaction details query must have one WHERE');
    let depth=0;for(const c of sql){if(c==='(')depth++;if(c===')')depth--;assert.ok(depth>=0);}
    assert.equal(depth,0,'transaction query must retain closing parentheses');
    assert.match(sql,/WHERE tp\.vehicle_no = \?/);
   }
   return [];
  }};
  const repo=new module.exports.Repository(db);
  await repo.transitionBatch([{vehicle_no:'JH02AK7064',lat:24.29,lng:86.63,ts:1790910190000}],{save:async()=>{}});
  assert.equal(detailsQueries,1);
 });
}
