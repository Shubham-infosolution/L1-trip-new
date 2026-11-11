L2 notification delivery
========================

Matches GpsService: shared content is saved once in tbl_notifications, with
recipient/read state in tbl_notification_details. After commit the full payload
is published on jsfc:notification:new, including userId and notification_id.
SocketService does not need a notification SELECT or frontend polling.
Recipient and alert-name lookups and notification history INSERTs still use DB.

GPS alerts generate type 9 notifications after alert batch commit and acknowledgement.
Titles come from tbl_alert_master.alert_name; recipients are active state users
and district users assigned without a store. Invalid district IDs are skipped.
Trip start, trip completion and subtrip completion deliver immediately; the
notification cron only drains legacy Redis notification queues.

GpsService, L2 and SocketService must use the same Redis instance.
The existing GpsService notification/details schema is required.
Pub/Sub is best effort: offline clients need stored history. Publish errors are
logged without repeating committed INSERTs. There is no durable outbox/retry.

Duplicate protection
--------------------
Atomic Redis SET NX claims restrict each event/user before notification INSERTs
and socket publication, including concurrent worker calls. Alert occurrence keys
preserve distinct events even with identical message text. Trip notifications use
type, trip/subtrip reference and vehicle. Claims default to seven days, configurable
with NOTIFICATION_DEDUP_TTL_SECONDS (1 to 2592000). Legacy rows without an event
identity use an identical-content window of 60 seconds.
Confirmed DB rollback releases owned claims. Unknown commit outcomes and publish
failures retain claims to avoid duplicate history. This is bounded best-effort
protection, not a durable exactly-once guarantee: Redis data loss/expiry can allow
replays; a process crash after claiming can suppress that event until expiry.
No historical duplicates are deleted. c_date SQL remains unchanged.
