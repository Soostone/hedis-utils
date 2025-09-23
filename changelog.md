0.5.0.0
* **BREAKING CHANGE**: Updated to be compatible with hedis master branch (commit 4b4e69e from 2025-06-08)
* Updated API calls to use NonEmpty lists where required by newer hedis
* Updated SetOpts usage to include new fields (setUnixSeconds, setUnixMilliseconds, setKeepTTL)
* Removed deprecated renewable lock functions as mentioned in 0.4.0

0.4.0.1
* Round negative or 0 ttls for locks up to 1ms so they are actually acquired.

0.4.0
* Switch to native locking mechanism using SET with PX and NX
  options. This should provide better locking performance and avoids
  some edge cases. **Migration Note** This version changes the
  prefixes for locks. Any locks taken out under this version will
  start with "_lock2:" instead of "_lock:". Previous versions of locks
  never expired within the database, so if a process shut down
  uncleanly and did not clear a lock, that lock would always appear
  locked after this update.
* `acquireRenewableLock`, `blockRenewableLock`, and
  `releaseRenewableLock` have been deprecated and are now aliased to
  their non-renewable versions, as all locks now support renew. These
  will be dropped in 0.5.
