# L2 GPS service — reviewed batching update

See **docs/REVIEW_AND_DEPLOYMENT.md** for changes, service coverage, configuration, tests and rollout limits.

```bash
npm ci
npm test
npm start
```

Merge `.env.batching.example` into your existing `.env` first. Production credentials remain in your existing environment. Use staging to validate the actual MySQL schema and Redis/MQTT flows before rollout.

`src/services/packet.service.js` is unchanged. All repositories share one MySQL pool per process (default limit 10). Frequent event updates and compatible append writes use bounded batches; generated-ID inserts and conditional trip transitions retain immediate results.

Latest fix: see **docs/GEOFENCE_NOTIFICATION_FIX.md** for the missing GeoFence IN notification cause, patch and server verification steps.

Latest location update: see **docs/ALERT_LOCATION_UPDATE.md**. Event-table path writes are removed; geofence direction is persisted; last_location uses a JSON array.


Manual trip completion guard
----------------------------
Before inserting queued L2 vehicle alert details, bulkInsertAlertDetails locks
current tbl_trip rows with SELECT ... FOR UPDATE in the insert transaction.
Only trip_status=1, status=1, level=2 and matching vehicle ownership are eligible.
A manual trip_status=2 update committed before this lock excludes the alert,
even when Redis has an active-trip snapshot or pending alerts. If insertion
holds the lock first, manual completion waits for that insertion to commit.

Completed/missing/inactive trips are skipped and the queue claim is acknowledged;
only actually inserted alerts produce notifications. All SQL chunks in the
claim share the transaction. Existing historical alerts are retained.
