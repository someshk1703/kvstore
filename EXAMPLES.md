# Dashboard Testing Examples

Live instance: https://kvstore-production.up.railway.app/dashboard.html  
Local: http://localhost:8080/dashboard.html (run `java -jar kvstore/target/kvstore-0.1.0-SNAPSHOT.jar --http`)

The console supports **5 commands**: `SET key value [EX sec]` · `GET key` · `DEL key` · `KEYS pattern` · `PING`  
Every command goes through the REST API. Stats panel polls `/api/info` every 3 s automatically.

---

## 1. Connectivity check

```
PING
```
**Expect:** `+UP`  
**Watch:** status dot turns green and shows "online". If you see "unreachable" the server is down.

---

## 2. Basic write and read

```
SET name Somesh
```
**Expect:** `+OK`  
**Watch:** "total keys" increments by 1 in the stats panel; one cell in the grid pulses amber then settles warm.

```
GET name
```
**Expect:** `"Somesh"`

```
SET name SomeshKannan
```
**Expect:** `+OK` (overwrite — key count does NOT change, same key)

```
GET name
```
**Expect:** `"SomeshKannan"` (confirms overwrite took effect)

---

## 3. Delete

```
SET temp deleteMe
GET temp
```
**Expect (GET):** `"deleteMe"`

```
DEL temp
```
**Expect:** `:1` (1 key deleted)  
**Watch:** key count decrements by 1; cell grid loses one warm cell.

```
GET temp
```
**Expect:** `$-1 (nil)` — key is gone.

```
DEL temp
```
**Expect:** `:0 (key not found)` — deleting a non-existent key is a no-op with count 0.

---

## 4. TTL (EX) — expiry in real time

```
SET session:demo tokenXYZ EX 20
```
**Expect:** `+OK`

```
GET session:demo
```
**Expect:** `"tokenXYZ"` — key is live.

Wait 20+ seconds, then:

```
GET session:demo
```
**Expect:** `$-1 (nil)` — key expired. The stats panel key count will also drop by 1 on the next 3 s poll.  
**Watch:** if key count drops between two polls with no explicit DEL, the grid briefly shows an "evicted" (outlined red) cell.

---

## 5. Short TTL — observe in real time

```
SET flash boom EX 5
GET flash
```
**Expect (GET):** `"boom"`

Wait 6 seconds:

```
GET flash
```
**Expect:** `$-1 (nil)`

---

## 6. KEYS pattern matching

First, seed several keys:

```
SET user:1 alice
SET user:2 bob
SET user:3 carol
SET product:1 widget
SET product:2 gadget
SET config:debug true
```

Then list by namespace:

```
KEYS user:*
```
**Expect:** three lines — `1) "user:1"` · `2) "user:2"` · `3) "user:3"`

```
KEYS product:*
```
**Expect:** `1) "product:1"` · `2) "product:2"`

```
KEYS *:1
```
**Expect:** `1) "product:1"` · `2) "user:1"` (both keys ending in `:1`)

```
KEYS *
```
**Expect:** all live keys (order is not guaranteed — the backend sorts them).

```
KEYS config:*
```
**Expect:** `1) "config:debug"`

```
KEYS nonexistent:*
```
**Expect:** `(empty list)`

---

## 7. Single-character wildcard (?)

```
SET ab 1
SET ac 2
SET abc 3
KEYS a?
```
**Expect:** `1) "ab"` · `2) "ac"` — two characters, starting with `a`. `abc` (3 chars) is excluded.

```
KEYS a??
```
**Expect:** `1) "abc"` — exactly 3 characters.

---

## 8. Bulk key population (copy-paste block)

Paste each line one at a time (the console is single-command-per-Enter):

```
SET counter:hits 0
SET counter:errors 0
SET counter:logins 0
SET cache:homepage "<html>..."
SET cache:api "{\"status\":\"ok\"}"
SET lock:job:42 worker-1 EX 30
SET queue:pending item-001
SET queue:pending item-002
SET flag:maintenance false
```

Then inspect:

```
KEYS counter:*
```
**Expect:** 3 keys

```
KEYS cache:*
```
**Expect:** 2 keys

```
KEYS lock:*
```
**Expect:** `1) "lock:job:42"` — disappears after 30 s

```
KEYS *
```
**Expect:** 9 keys total (key count in stats panel should show 9 + whatever you had before)

---

## 9. Value with spaces

```
SET greeting "hello world"
GET greeting
```
**Expect (GET):** `"hello"` — **not** `"hello world"`

**Why:** the console tokenizes on whitespace. `"hello` becomes the value; `world"` is silently dropped. This is a known limitation of the dashboard parser (not the backend). The backend itself is fine — this is purely the JS split-on-whitespace in `dispatch()`.  
**Implication for AOF interview question:** the AOF log format uses the same inline text parser, so values with unquoted spaces would also break on replay. A production system converts to multi-bulk RESP before logging.

---

## 10. Numeric values (INCR equivalent via SET)

The dashboard has no `INCR` command. You can simulate the read-modify-write manually:

```
SET score 0
GET score
SET score 1
GET score
SET score 100
GET score
```

To see what a real `INCR` response looks like, the TCP protocol returns `:newvalue` (integer). Over HTTP, the REST API (`KeyValueController`) only has SET/GET/DEL — there is no `/api/incr` endpoint. INCR is TCP-only.

---

## 11. FLUSHALL equivalent — wipe everything

No `FLUSHALL` in the dashboard. Workaround:

```
KEYS *
```
Then DEL each key individually. For a full wipe during testing, restart the server (kills all in-memory state if persistence is disabled).

---

## 12. Stats panel — what updates and what doesn't

| Field | Updates via dashboard? | Source |
|---|---|---|
| total keys | Yes — every SET/DEL, confirmed on next 3 s poll | `/api/info` `totalKeys` |
| memory used | Yes — approximate JVM heap, not per-key | `/api/info` `usedMemoryBytes` |
| memory capacity | Yes | `/api/info` `maxMemoryBytes` (JVM `-Xmx`) |
| uptime | Yes — counts up every poll | `/api/info` `uptimeSeconds` |
| version | Yes — static `"0.1.0"` | `/api/info` `version` |
| AOF status | No — "not exposed yet" | Not in `/api/info` |
| hit/miss ratio | No — "not exposed yet" | Not in `/api/info` |
| evictions | No — inferred from key-count drop only | Not in `/api/info` |
| replica sync | No — "not exposed yet" | Not in `/api/info` |

---

## 13. Cell grid behaviour

The grid has **96 cells**. Each cell represents a slot (not a 1:1 key mapping).

| State | Trigger |
|---|---|
| **Hot** (bright amber, glowing) | Key count just increased (SET on new key) — lasts ~900 ms |
| **Warm** (dim amber) | Slot occupied after hot animation fades |
| **Empty** (dark) | Slot count above current key count |
| **Evicted** (outlined red) | Key count dropped between two polls (DEL or TTL expiry) — lasts 2.5 s |

To see the evicted state deliberately:

```
SET evictme value EX 4
```
Wait 4–7 seconds (next poll catches the drop) → one cell briefly outlines red.

---

## 14. Rapid-fire write sequence — watch key count climb

Type these quickly in succession (or paste one per Enter):

```
SET k1 v1
SET k2 v2
SET k3 v3
SET k4 v4
SET k5 v5
```

**Watch:** the stats panel key count climbs with each poll. The grid flashes amber cells as new keys arrive.

Then delete all at once:

```
DEL k1
DEL k2
DEL k3
DEL k4
DEL k5
```

**Watch:** key count drops; evicted cells flash briefly.

---

## 15. Confirm server identity

```
PING
GET __probe__
KEYS __*
```

`PING` → `+UP` confirms the server is the real running instance, not a cached response.  
`GET __probe__` → `$-1 (nil)` confirms an unset key returns nil cleanly.  
`KEYS __*` → `(empty list)` confirms the keyspace is clean.


to set:
SET foo bar
SET session:1 alice EX 30
DEL foo
EXPIRE counter 3600
# SNAPSHOT 1720000000000
SET newkey val

Here's a broader set covering the patterns you'll actually hit in an AOF log, grouped by what each is testing:

**Basic writes/updates**
```
SET user:100 "John Doe"
SET user:100 "John Smith"        # overwrite — same key, new value
INCR counter                     # counter now 1 (if key didn't exist, starts at 0)
INCR counter                     # counter now 2
APPEND log:events "user_login"   # if you support string append ops
```

**Expiry (TTL) handling**
```
SET session:42 "token_abc" EX 60      # expires 60s after this write
EXPIRE session:42 120                 # separately extends TTL on an existing key
PERSIST session:42                    # removes TTL, makes key permanent again
```
Worth knowing cold: does your AOF log expiry as a **relative** TTL (`EX 60`) or does it get converted to an **absolute** expiry timestamp before writing? This matters a lot for replay correctness — if you log `EX 60` and replay the AOF five minutes later on restart, a naive implementation would give the key a *fresh* 60 seconds instead of recognizing it should've already expired. Most real implementations (Redis included) convert relative TTLs to absolute timestamps before persisting, specifically to avoid this bug. Good interview question to have a real answer for either way.

**Deletion**
```
DEL foo
DEL nonexistent_key      # still logged even if it's a no-op — worth deciding whether you skip logging no-ops or not
```

**What does NOT appear in the AOF**
```
GET foo          # reads are never written — AOF only logs mutations
EXISTS foo        # also read-only, not logged
TTL session:42     # read-only, not logged
```
This is actually a good thing to state explicitly in your interview notes — it shows you understand *why* AOF works the way it does: replaying only mutations reconstructs the exact same end-state, replaying reads would be pointless and would bloat the file for no reason.

**Snapshot / compaction markers**
```
# SNAPSHOT 1720000000000
SET newkey val
```
This is the mechanism that keeps your AOF file from growing forever. A few things worth understanding about your own `# SNAPSHOT` line specifically:
- Does it mean "truncate everything before this line because a full snapshot of state was written to disk separately," or is it purely an informational marker with no truncation behavior yet?
- If it does trigger truncation, what's the trigger — file size threshold, time interval, or manual command? Redis calls this "AOF rewrite" and it's a classic interview follow-up ("how do you keep the AOF from growing unbounded forever").

**Edge cases worth testing/understanding in your own log format**
```
SET "key with spaces" "value with spaces"     # how does your parser handle quoting/escaping?
SET binary:key "\x00\x01\x02"                 # binary-safe values, if you support them
DEL "key with spaces"
```

If your parser only splits on whitespace without quote-awareness, values with spaces will silently corrupt on replay — that's a real gap worth knowing about (or fixing) before an interviewer asks "what happens if I SET a value containing a space."

Want me to write out the actual AOF replay pseudocode (reading the file top-to-bottom, applying each line, handling the `# SNAPSHOT` truncation logic) so you have a clean mental model to explain verbally in an interview?