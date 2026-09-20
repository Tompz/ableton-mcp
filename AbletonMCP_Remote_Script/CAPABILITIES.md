# AbletonMCP capability map

Living audit of what this Remote Script (`__init__.py`, currently v1.13.0)
can and can't do. Started 2026-09-19 after a session of live-testing,
patching, and root-causing real bugs against Live 12.4.6 — not a spec, a
record of what was actually verified working, kept honest about the
difference between "implemented" and "confirmed."

That session ended with Live hanging (right after loading a Simpler) and
then crashing badly enough to need a full laptop reboot — see
[[abletonmcp-patching-caution]] in memory. The script file itself survived
completely intact (confirmed via `get_script_info` post-reboot: v1.13.0,
all 43 capabilities present). Several fixes made in the final stretch were
never re-tested before the crash — those are called out explicitly below
rather than folded into "confirmed working."

Three standing architectural facts that shape everything below:

- **Code changes need a full Live restart to load.** Toggling the Control
  Surface off/on in Preferences → Link/Tempo/MIDI preserves the open Live Set
  but does *not* re-import the Python module — confirmed by a stale
  `__pycache__` timestamp after a toggle-reload. Only a full quit/reopen
  guarantees fresh code.
- **There is no save API.** Live deliberately does not expose a save method
  to Remote Scripts. Saving is handled outside this script entirely — via
  AppleScript activating Live and sending ⌘S — and the *first* save on a
  never-saved Set still needs a human to type the filename into the dialog.
- **Restart pacing is now a real constraint, not just an inconvenience.**
  ~16 restarts in one sitting preceded the crash. Batch fixes, but don't
  chain many restarts back-to-back — see the memory note.

## Confirmed working

Actually exercised against a live Ableton instance and stable afterward.

**Session / structure**
- Read: full session/track/clip/device snapshot (`get_session_snapshot`),
  individual track/clip/device introspection, arrangement clip listing
- Create: MIDI track, audio track, scene, MIDI clip, audio clip (from a file
  path)
- Delete: track, clip, device, scene
- Tempo set, playback start/stop, individual clip fire/stop, scene fire
- Duplicate a Session clip into the Arrangement at a given beat position
- Switch to Arrangement view, move the arrangement playhead
- Undo / redo (`song.undo()` / `song.redo()` — re-confirmed after the
  reboot, the one thing verified in the recovery session)

**Notes / MIDI**
- Add notes to a clip, clear all notes from a clip
- Read notes back with full fidelity (pitch, time, duration, velocity, mute,
  probability, velocity_deviation, release_velocity, note_id)

**Devices / instruments**
- Load an instrument or effect by browser URI onto a regular track
  (reliable)
- Load onto the **Master** track via an explicit `target="master"` param —
  confirmed live (loaded Wavetable onto "Main", Live 12's internal name for
  Master)
- Read and set any device parameter by index
- Delete a device from a track's chain
- Write an automation envelope onto a device parameter within a Session
  clip — `clip.create_automation_envelope(param)` is the real creation call
  (`automation_envelope()` only *reads* an existing one, a genuinely
  non-obvious API split); ramps are approximated as many short constant
  steps since the LOM's envelope API is step-based, not true curves

**Mixer**
- Set track volume, panning, and individual send levels
- Set track mute, solo, and record-arm (`can_be_armed` guarded)

**Audio clips**
- Set gain, pitch (coarse/fine), and warping on an audio clip

**Groove**
- List grooves in the Groove Pool, assign one to a clip — real engine
  swing, not hand-written note-timing offsets

**Colour**
- Set colour on a track or a clip (Live snaps arbitrary RGB to its nearest
  palette colour — expected behaviour, not a bug)

**Clip launch settings**
- `legato` confirmed set successfully. `launch_quantization`,
  `follow_action_a/b`, `follow_action_time` are implemented the same way
  but weren't individually exercised — same risk profile as anything
  else untested below.

**Browser / locators**
- `get_browser_items_at_path` tolerates a missing `.adg` extension
  (`"Foo Kit"` matches `"Foo Kit.adg"`) and correctly routes a raw URI
  (`"query:Drums#FileId_..."`) to the URI-based lookup instead of
  mis-parsing it as a slash-path
- `create_locator` works reliably for both creating a new locator and
  renaming an existing one. Root cause of the original bug: setting
  `song.current_song_time` and reading it back (or toggling a cue) within
  the *same* main-thread tick did not reflect the change. Fixed by
  splitting into two scheduled phases (set position, then — one tick later
  — toggle and verify)

**Routing**
- `get_track_routing` / `set_track_routing` — read/set a track's input or
  output routing type by matching display name. Real options surfaced
  (hardware inputs, MODX ports, other tracks); set + restore round-tripped
  cleanly both directions

## Confirmed working (verified in this session, post-reboot)

- **`update_notes`** (targeted per-note edit) — took five restart cycles
  and three wrong theories to land properly; worth recording precisely
  since the real cause is genuinely non-obvious. `apply_note_modifications`
  binds to a fixed C++ vector type (`std::vector<NClipApi::TNoteInfo>`)
  and — confirmed by direct experiment, not inference — **never** accepts
  a Python-constructed `list` or `tuple`, no matter what's inside it: not
  a plain dict (with or without all 9 fields), not a real `Clip.MidiNote`
  object (mutated or completely untouched). It only accepts its own native
  container type, `Clip.MidiNoteVector` — specifically, **a slice of the
  vector `get_notes_extended` itself returned** (proven by passing an
  untouched `raw_notes[0:1]` straight back successfully, before any other
  part of the fix was in place). The working implementation: fetch notes
  via `get_notes_extended(from_pitch, pitch_span, from_time, time_span)`,
  keep that native vector intact (never call `list()` on it, which
  silently discards the type information needed for the write side),
  mutate target notes' attributes in place by indexing into it, then hand
  back a full-range slice (`raw_notes[0:len(raw_notes)]`) of that same
  vector. Verified live: changed one note's pitch and velocity, the other
  two notes in the clip came back byte-for-byte unchanged.

- **`set_session_record`** — same class of bug as the locator: writing
  `song.session_record` and reading it back in the same tick showed the
  *previous* call's value, not this one's. Fixed by no longer reading back
  (trust the write, like any transport toggle). Re-tested clean.
- **`remove_notes_range`** — `remove_notes_extended`'s real signature
  (confirmed via a boost::python type error) is `(from_pitch: int,
  pitch_span: int, from_time: double, time_span: double)` — pitch args
  first and grouped together, not interleaved with time as originally
  guessed. Re-tested: removed exactly the targeted note, left the other
  two untouched, confirmed by reading the clip back.
- **`investigate_advanced_editing`'s track-reorder check** — filter was
  buggy (`"move" in a.lower()` also matches every `remove_*_listener`
  method, since "re**move**" contains "move"), drowning the real signal in
  noise. Fixed to exclude `add_`/`remove_` prefixed names, then re-run —
  see "Confirmed absent" below for what the clean result actually showed.

## Implemented, never tested at all

Brand new in the final patch batch before the crash — zero live
verification, good or bad.

- **`add_warp_marker` / `remove_warp_marker` / `move_warp_marker`** — the
  methods are confirmed to exist (`dir(clip)` introspection found exactly
  these three names), but the call signatures are a best-effort guess.
  Given tonight's track record, expect at least one to need a
  signature correction from its first real error.
- **Return-track device loading** (`target="return"`) — only Master was
  tested; the code path is identical but return-track indexing is
  unverified.

## Genuinely inconclusive — needs a clean re-test

- **Simpler/Sampler slice/reverse control.** Zero real data — the one
  attempt to get it (loading a Simpler to inspect) is what coincided with
  the hang that led to the crash. Per [[abletonmcp-patching-caution]],
  treat re-attempting this as a soft risk: test it in isolation, first
  thing after a fresh restart, not bundled with anything else.

## Confirmed absent — not a Remote Script capability at all

- **Freeze / render / export / bounce.** `investigate_render_capability`
  filtered `dir()` on Track, Song, and Application for any attribute
  containing freeze/render/export/bounce/flatten/consolidate — all three
  came back empty. Not a naming mismatch: none of those operations are
  exposed to Remote Scripts anywhere on the object surface. It's a
  File → Export Audio/Video (or right-click → Freeze Track) UI-only
  operation with no LOM hook. Would need a different mechanism entirely
  (UI automation via AppleScript/System Events, the same class of
  workaround used for saving) — not fixable from this file.
- **Track reordering.** Re-checked with the filter bug fixed: `song`'s
  only reorder-shaped attributes are `find_device_position` and
  `move_device` — both for reordering *devices within a chain*, nothing
  track-level. The track object itself has nothing matching reorder/move/
  position/index beyond unrelated properties (`color_index`,
  `fired_slot_index`, etc. — caught by the "index" keyword but irrelevant).
  Clean negative both sides, not fixable from this file.
- **Mid-arrangement time signature changes.** `song`'s only
  signature-related attributes are the two global properties
  (`signature_numerator`/`signature_denominator`), already read elsewhere.
  No per-position marker API exists. Confirmed global-only, not a naming
  issue.
- **Save.** By design — see above.

## Still genuinely unexplored

Not yet investigated in either direction — no dir() check, no attempt.

- **Project file management** — no read of the current project's file
  path or "has unsaved changes" state; no open/new-project control (the
  latter is almost certainly impossible for the same structural reason as
  freeze/render — a Remote Script lives inside one already-open document's
  lifetime, it doesn't get invoked to open a different one)
- **Note selection state** — Live's UI concept of "these notes are
  selected" (as opposed to just editing by note_id) — unexplored
- **A dedicated quantize command** — not impossible so much as
  unnecessary: `update_notes` can already snap arbitrary notes to a grid
  by computing and writing new `start_time` values, so this is a
  convenience gap, not a hard blocker
- **Take-lane / comping visibility or control** (Live 12-specific) —
  never investigated

## Where the ceiling actually is

Two different kinds of "missing," worth keeping separate:

1. **Fixable in this file, likely quickly** — the "unverified fix" and
   "never tested" sections above. Every wrong guess tonight was corrected
   within one live attempt because boost::python's type errors hand back
   the real signature. That pattern held all night; no reason to expect
   otherwise for warp markers or return-track loading.
2. **Outside this file's reach** — the `load_drum_kit` convenience
   wrapper's remaining flakiness lives in the separate MCP server process
   (fetched at runtime, not found on disk despite a real search), not this
   Remote Script. The direct-URI workaround via `load_instrument_or_effect`
   is reliable. Freeze/render/export and time-signature changes are
   confirmed absent from the entire object model, not just this file —
   fixing those would mean UI automation, not Python API calls.
