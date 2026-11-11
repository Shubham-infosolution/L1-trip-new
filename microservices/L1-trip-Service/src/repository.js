'use strict';
const cfg = require('./config');
const { observeEvent, canStart, journeyState } = require('./geofence-state');
const { decisionsFor } = require('./transition-decisions');
const { createHash, randomUUID } = require('node:crypto');
const { plan, sqlTime, dbTime } = require('./domain');
// tp.* preserves every trip column; aliases add route and geofence information.
// Keep the join projection independent of WHERE. Never derive it by cutting an
// already-filtered query: harmless whitespace/case edits used to create two WHEREs.
const DETAILS_SELECT = `SELECT tp.*, tp.trip_id AS trip_unique_id, tp.c_date AS lifting_date,
 rt.source_store_id, rt.destination_store_id, rt.time_duration,
 src.lat AS source_lat, src.lng AS source_lng, src.radius AS source_radius,
 dest.lat AS destination_lat, dest.lng AS destination_lng, dest.radius AS destination_radius
 FROM tbl_trip tp
 LEFT JOIN tbl_route_master rt ON tp.route_id = rt.id AND rt.status = 1
 LEFT JOIN tbl_store_location_master src ON src.id = rt.source_store_id AND src.status = 1
 LEFT JOIN tbl_store_location_master dest ON dest.id = rt.destination_store_id AND dest.status = 1`;
const SELECT = `${DETAILS_SELECT}
 WHERE tp.level = 1 AND tp.status = 1 AND tp.trip_status IN (0, 1)`;
const START = `UPDATE tbl_trip SET trip_status = 1, trip_date = ?,
 expected_arrival_time = DATE_ADD(?, INTERVAL ? MINUTE), start_lat = ?, start_lng = ?
 WHERE id = ? AND vehicle_no = ? AND level = 1 AND status = 1
 AND trip_status = 0 AND c_date < ?`;
const END = `UPDATE tbl_trip SET trip_status = 2, end_lat = ?, end_lng = ?, completion_date = ?
 WHERE id = ? AND vehicle_no = ? AND level = 1 AND status = 1
 AND trip_status = 1 AND trip_date < ?`;
class Repository {
  constructor(database) { this.db = database; }
  async activeForVehicle(v) {
    return this.db.readExecute(`${SELECT} AND tp.vehicle_no = ? ORDER BY tp.id`, [v], { primary: true });
  }
  async refresh(shard) {
    const predicate = this.db.useShardColumn
      ? 'tp.trip_worker_shard = ?' : 'MOD(CRC32(UPPER(TRIM(tp.vehicle_no))), 8) = ?';
    const rows = await this.db.readExecute(`${SELECT} AND ${predicate} ORDER BY tp.id`, [shard]);
    // Replica discovery, followed by a small primary-state projection. Never cache
    // completed/cancelled trips just because a secondary has not applied their update.
    const current = new Map();
    for (let i = 0; i < rows.length; i += 500) {
      const ids = rows.slice(i, i + 500).map(t => t.id);
      const states = await this.db.readExecute(`SELECT id, vehicle_no, route_id, trip_status,
        c_date, trip_date, expected_arrival_time, start_lat, start_lng, end_lat, end_lng, completion_date
        FROM tbl_trip WHERE id IN (${ids.map(() => '?').join(',')})
        AND level = 1 AND status = 1 AND trip_status IN (0,1)`, ids, { primary: true });
      for (const state of states) current.set(String(state.id), state);
    }
    return rows.flatMap(row => {
      const state = current.get(String(row.id));
      // Changed assignment/route must be rediscovered with matching joined data.
      if (!state || state.vehicle_no !== row.vehicle_no || String(state.route_id) !== String(row.route_id)) return [];
      return [{ ...row, ...state, lifting_date: state.c_date }];
    });
  }
  async transitionBatch(packets, journal) {
    if (!packets.length) return { rows: [], events: [], intent: null };
    if (!journal?.save) throw new Error('Redis write journal is required');
    const v = packets[0].vehicle_no;
    if (packets.some(p => p.vehicle_no !== v)) throw new Error('Mixed vehicle batch');
    return this.db.withTransaction(async db => {
      let intent = journal.intent;
      const targets = [...new Set((intent?.operations || []).map(o => o.trip_id))];
      const extra = targets.length ? ` OR id IN (${targets.map(() => '?').join(',')})` : '';
      await db.writeExecute(`SELECT id FROM tbl_trip WHERE vehicle_no = ?
        AND ((level = 1 AND status = 1 AND trip_status IN (0,1))${extra}) ORDER BY id FOR UPDATE`, [v, ...targets]);
      const targetFilter = targets.length ? ` OR tp.id IN (${targets.map(() => '?').join(',')})` : '';
      let rows = await db.writeExecute(`${DETAILS_SELECT} WHERE tp.vehicle_no = ?
        AND ((tp.level = 1 AND tp.status = 1 AND tp.trip_status IN (0,1))${targetFilter}) ORDER BY tp.id`, [v, ...targets]);
      if (!intent) {
        if (journal.archiveCoverage && !require('./archive-recovery').covers(journal.archiveCoverage, rows, cfg)) {
          throw Object.assign(new Error('Primary trip configuration requires historical recovery before processing'),
            { code: 'ARCHIVE_RECOVERY_REQUIRED', rows });
        }
        const simulated = structuredClone(rows), operations = [];
        const geofences = structuredClone(journal.loadGeofences
          ? await journal.loadGeofences(rows.map(t => String(t.id))) : journal.geofences || {});
        const decisions = {};
        const ordered = [...packets].sort((a, b) => a.ts - b.ts);
        // Markers are scoped to vehicle + trip + action + GPS time. Legacy
        // vehicle-wide markers cannot suppress another trip's valid departure.
        const ids = rows.flatMap(t => ordered.flatMap(p =>
          ['start','end'].map(kind => transitionId(p, t.id, kind))));
        const done = journal.completed ? await journal.completed(ids) : journal.done || new Set();
        for (const p of ordered) {
          const eventGeofences = {};
          for (const t of simulated.filter(t => Number(t.trip_status) !== 2)) {
            // Prefer a recovered actual source visit over assuming an earlier outside point was departure.
            const observeCfg = journal.archiveCoverage?.sourceInside?.[String(t.id)]
              ? { ...cfg, allowMissingStateAssumption: false } : cfg;
            const observed = observeEvent(geofences[String(t.id)], t, p, observeCfg);
            geofences[String(t.id)] = observed.snapshot;
            eventGeofences[String(t.id)] = observed.current;
          }
          const duplicate = (t, kind) => done.has(transitionId(p, t.id, kind));
          const action = plan(simulated, p, cfg, (t, kind) => !duplicate(t, kind) &&
            (kind === 'end' || (!(journal.archiveCoverage?.sourceInside?.[String(t.id)] &&
              eventGeofences[String(t.id)]?.source_exit_basis === 'MISSING_REDIS_STATE') &&
              canStart(eventGeofences[String(t.id)], t, p, cfg))));
          Object.assign(decisions, decisionsFor(simulated, p, eventGeofences, action, duplicate, cfg));
          for (const t of action.end) {
            operations.push(makeOperation('end', t, p));
            t.trip_status = 2; t.completion_date = sqlTime(p.ts, cfg.offset);
          }
          for (const t of action.start) {
            operations.push(makeOperation('start', t, p, eventGeofences[String(t.id)]));
            t.trip_status = 1; t.trip_date = sqlTime(p.ts, cfg.offset);
          }
        }
        for (const t of simulated) {
          const state = geofences[String(t.id)];
          if (state) {
            state.trip_status = Number(t.trip_status);
            state.journey_state = journeyState(t, state);
            state.last_decision = decisions[String(t.id)] || null;
          }
        }
        intent = { id: randomUUID(), policy: 'independent-trips-v3-missing-state', vehicle_no: v,
          packets: ordered, operations, geofences, decisions: Object.values(decisions) };
        // Persist exact target IDs and values BEFORE issuing any business UPDATE.
        await journal.save(intent);
      }
      const events = [];
      for (const op of intent.operations) {
        const t = rows.find(row => String(row.id) === op.trip_id);
        if (!t) throw conflict(op, 'target row missing or reassigned');
        const when = sqlTime(op.packet.ts, cfg.offset);
        const status = Number(t.trip_status);
        const dateField = op.kind === 'start' ? t.trip_date : t.completion_date;
        const lat = op.kind === 'start' ? t.start_lat : t.end_lat;
        const lng = op.kind === 'start' ? t.start_lng : t.end_lng;
        const reached = op.kind === 'start' ? status === 1 || status === 2 : status === 2;
        if (reached && dbTime(dateField, cfg.offset) === op.packet.ts &&
            sameCoordinate(lat, op.packet.lat) && sameCoordinate(lng, op.packet.lng)) {
          continue; // Commit already applied: never choose another trip for this event.
        }
        if (status !== (op.kind === 'start' ? 0 : 1) || Number(t.status ?? 1) !== 1 ||
            Number(t.level ?? 1) !== 1 || fingerprint(t) !== op.expected) {
          throw conflict(op, 'state or route changed; retained for review');
        }
        if (op.kind === 'start') {
          const r = await db.writeExecute(START, [when, when, op.duration, op.packet.lat, op.packet.lng, op.trip_id, v, when]);
          if (r.affectedRows !== 1) throw conflict(op, 'start precondition changed');
          t.trip_status = 1; t.trip_date = when; t.start_lat = op.packet.lat; t.start_lng = op.packet.lng;
        } else {
          const r = await db.writeExecute(END, [op.packet.lat, op.packet.lng, when, op.trip_id, v, when]);
          if (r.affectedRows !== 1) throw conflict(op, 'end precondition changed');
          t.trip_status = 2; t.completion_date = when; t.end_lat = op.packet.lat; t.end_lng = op.packet.lng;
        }
        events.push({ kind: op.kind === 'start' ? 'trip_started' : 'trip_ended', trip_id: op.trip_id,
          vehicle_no: v, packet_datetime: when, gps_timestamp: op.packet.ts,
          latitude: op.packet.lat, longitude: op.packet.lng, packet_type: op.packet.packet_type,
          ...(op.kind === 'start' ? { start_basis: op.start_basis || 'OBSERVED_SOURCE_EXIT',
            assumed_source_in_at: op.assumed_source_in_at ?? null } : {}),
          expected_arrival_time: op.kind === 'start' && op.duration !== null
            ? sqlTime(op.packet.ts + op.duration * 60000, cfg.offset) : null,
          eta_warning: op.kind === 'start' && op.duration === null ? 'ROUTE_DURATION_MISSING_OR_INVALID' : null });
      }
      const after = await db.writeExecute(`${SELECT} AND tp.vehicle_no = ? ORDER BY tp.id`, [v]);
      return { rows: after, evaluatedRows: rows, events, intent };
    }, { isolationLevel: 'READ COMMITTED' });
  }
}
function fingerprint(t) {
  return JSON.stringify([String(t.route_id ?? ''), dbTime(t.lifting_date, cfg.offset),
    Number(t.trip_status) === 1 ? dbTime(t.trip_date, cfg.offset) : null,
    ...['source_lat','source_lng','source_radius','destination_lat','destination_lng','destination_radius','time_duration']
      .map(k => t[k] === null || t[k] === undefined ? null : Number(t[k]))]);
}
function makeOperation(kind, t, p, sourceState) {
  const value = t.time_duration;
  const duration = value !== null && value !== undefined && value !== '' &&
    Number.isFinite(Number(value)) && Number(value) >= 0 ? Number(value) : null;
  return { kind, trip_id: String(t.id), packet: p, duration, expected: fingerprint(t),
    ...(kind === 'start' ? { start_basis: sourceState?.source_exit_basis || 'OBSERVED_SOURCE_EXIT',
      assumed_source_in_at: sourceState?.source_assumed_in_at ?? null } : {}) };
}
function sameCoordinate(a, b) { return a !== null && a !== undefined && Math.abs(Number(a) - Number(b)) <= 0.000001; }
function conflict(op, reason) { return Object.assign(new Error(`Trip ${op.trip_id}: ${reason}`), { code: 'TRIP_INTENT_CONFLICT' }); }
function eventId(p) { return createHash('sha256').update(`jsfc-trip-event-v1:${p.vehicle_no}:${p.ts}`).digest('hex'); }
function transitionId(p, tripId, kind) {
  return createHash('sha256').update(JSON.stringify(['jsfc-trip-transition-v2', p.vehicle_no, String(tripId), kind, p.ts])).digest('hex');
}
module.exports = { Repository, DETAILS_SELECT, SELECT, START, END, eventId, transitionId };
