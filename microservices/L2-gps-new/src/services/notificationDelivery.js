'use strict';

const { createHash, randomUUID } = require('node:crypto');
const { integer } = require('../db/settings');
const db = require('../model/dbquery');
const RedisRepository = require('../repositories/RedisRepository');
const IST_NOW = "CONVERT_TZ(NOW(), @@session.time_zone, '+05:30')";

// Persist shared content once; per-user state belongs only to the details table.
module.exports = async function deliverNotifications(notifications) {
  if (!Array.isArray(notifications)) throw new TypeError('Expected notifications array');
  if (!notifications.length) return {affectedRows: 0, notifications: []};
  const columns = ['level','title','message','reference_id','notification_type','link','c_by','district_id','vehicle_no'];
  const groups = new Map();
  const ttl = integer('NOTIFICATION_DEDUP_TTL_SECONDS', 604800, 1, 2592000);
  for (const row of notifications) {
    const userId = Number(row?.user_id);
    if (!Number.isSafeInteger(userId) || userId <= 0 || !row.title) {
      throw new Error('Notification requires title and positive user_id');
    }
    const values = columns.map(column => row[column] ?? null);
    // Event identity distinguishes separate occurrences with identical content.
    const key = JSON.stringify([row.event_key ?? null, values]);
    if (!groups.has(key)) groups.set(key, {row, values, users: new Set()});
    groups.get(key).users.add(userId);
  }
  const client = RedisRepository.getClient();
  const token = randomUUID();
  const claimed = [];
  let started = false;
  let saved;
  let affectedRows = 0;
  try {
    // Atomic Redis claims prevent concurrent workers from saving/publishing duplicates.
    for (const group of groups.values()) {
      const {row, values, users} = group;
      const tripEvent = [3,4,6].includes(Number(row.notification_type)) && row.reference_id != null;
      const identity = row.event_key != null
        ? ['event', row.event_key, row.notification_type, row.vehicle_no]
        : tripEvent
          ? ['trip', row.notification_type, row.reference_id, row.vehicle_no]
          : ['content', values];
      const digest = createHash('sha256').update(JSON.stringify(identity)).digest('hex');
      const accepted = new Set();
      for (const userId of users) {
        const key = 'notification:dedup:' + digest + ':' + userId;
        const result = await client.set(key, token, 'EX', row.event_key != null || tripEvent ? ttl : 60, 'NX');
        if (result === 'OK') { claimed.push(key); accepted.add(userId); }
      }
      group.users = accepted;
    }
    if (!claimed.length) return {affectedRows: 0, notifications: []};
    started = true;
    saved = await db.withTransaction(async tx => {
    const payloads = [];
    for (const {row, values, users} of groups.values()) {
      if (!users.size) continue;
      affectedRows++;
      const result = await tx.query(
        `INSERT INTO tbl_notifications (${columns.join(',')},c_date,status)
         VALUES (${columns.map(() => '?').join(',')},${IST_NOW},1)`, values);
      if (!result.insertId) throw new Error('Notification INSERT returned no ID');
      const recipients = [...users];
      await tx.query(`INSERT INTO tbl_notification_details
        (user_id,notification_id,is_read,c_by,c_date,status)
        VALUES ${recipients.map(() => `(?,?,0,?,${IST_NOW},1)`).join(',')}`,
        recipients.flatMap(userId => [userId, result.insertId, row.c_by ?? null]));
      for (const userId of recipients) payloads.push({
        ...row, userId, user_id: userId, id: result.insertId, notification_id: result.insertId
      });
    }
    return payloads;
  });
  } catch (error) {
    // An unknown COMMIT outcome must retain claims to prevent duplicate inserts.
    if (!started || error.transactionRolledBack === true) {
      for (const key of claimed) {
        try {
          await client.eval("if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end", 1, key, token);
        } catch (cleanupError) { console.error('[Notification] Dedup cleanup failed', key, cleanupError); }
      }
    }
    throw error;
  }
  // Publish only after COMMIT. No SELECT/read-back is needed by SocketService.
  for (const payload of saved) {
    try {
      await RedisRepository.getClient().publish('jsfc:notification:new', JSON.stringify(payload));
    } catch (error) {
      // Replaying SQL here would duplicate already committed notifications.
      console.error('[Notification] Saved but socket publish failed', payload.id, error);
    }
  }
  return {affectedRows, notifications: saved};
};
