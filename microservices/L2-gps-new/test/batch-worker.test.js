'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

function setup() {
    const children = [], timers = new Set();
    let spawnOptions;
    const context = {
        module: { exports: {} }, __dirname: path.resolve('src/utils'),
        process: { execArgv: ['--inspect=9229'] }, console: { log() {}, error() {} },
        setTimeout(fn, ms) { const t = {fn, ms}; timers.add(t); return t; },
        clearTimeout(t) { timers.delete(t); },
        require(name) {
            if (name !== 'node:child_process') return require(name);
            return { fork(file, args, options) {
                spawnOptions = options;
                assert.ok(file.endsWith(path.join('workers','alerts.js')));
                const child = new EventEmitter();
                Object.assign(child, { pid: children.length + 1, connected: true, exitCode: null,
                    send(message) { assert.equal(message.type, 'shutdown'); queueMicrotask(() => child.emit('exit', 0)); },
                    kill() { child.emit('exit', 1); } });
                children.push(child); return child;
            } };
        }
    };
    vm.runInNewContext(fs.readFileSync('src/utils/batchWorker.js', 'utf8'), context);
    return { start: context.module.exports, children, timers, options: () => spawnOptions };
}

test('worker is isolated, restarts on failure, and stops without restarting', async () => {
    const s = setup(), worker = s.start();
    assert.equal(s.children.length, 1);
    assert.equal(s.options().windowsHide, true);
    assert.equal(s.options().execArgv.length, 0);
    s.children[0].emit('exit', 1);
    const restart = [...s.timers][0];
    assert.equal(restart.ms, 1000);
    s.timers.delete(restart); restart.fn();
    assert.equal(s.children.length, 2);
    await worker.stop();
    assert.equal(s.timers.size, 0);
});

test('shutdown cancels a pending worker restart', async () => {
    const s = setup(), worker = s.start();
    s.children[0].emit('exit', 1);
    await worker.stop();
    assert.equal(s.timers.size, 0);
    assert.equal(s.children.length, 1);
});
