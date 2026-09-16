# Exposure scenarios

Drives `sps expose` end to end with no hub, no detectors and no spectrograph, to check what
the actor does when an exposure goes well and when it is interrupted.

## Running

    setup -r .                                  # or however spsActor is set up
    python test/runTests.py                     # everything
    python test/runTests.py -v backgrounded     # one family, with tracebacks

A run takes about a minute. Every scenario builds its own spectrograph, so they are
independent and can be run in any order.

Three outcomes are reported:

- `ok` — the scenario passed.
- `FAIL` — a regression. The runner exits non-zero.
- `gap` — behaviour the actor does not have yet, marked with `@knownGap` and its reason.
  Expected to fail; if it starts passing the runner says `FIXED` and exits non-zero, so a
  fix cannot land while the mark still claims otherwise.

## What is real and what is not

The exposure objects, the threading, the state machines, the shutter callbacks, the command
vocabulary and its argument parsing are the actor's own. `SpsConfig` is built from
`pfs_instdata`, so light sources and shutter sets are the ones the summit uses.

Simulated: the devices behind the commands. An enu opens its shutters, holds them for the
exposure time unless finished early, closes them and publishes `shutterTimings`; a detector
announces every state it passes through; a lamp controller records whether its lamps were
fired and whether anything released them. Timings are scaled to fractions of a second.

Not simulated: the NIR arm, whose ramp timing and file handling need a different device, and
the twisted reactor, so anything deferred with `callLater` — the `fiberIllumination` keyword
— does not fire.

## Injecting an interruption

Injections land at a named point rather than after a delay, so a scenario tests a timing
rather than a race. `failAt` fails a device at a point it declares, `abortAt` and `finishAt`
deliver a user command just before a given command reaches its device:

    expose(f'expose arc exptime=0.4 cams=b1 visit=1 doLamps isLast',
           prepare=dict(pfilamps=dict(neon=0.4)),
           inject=abortAt('ccd_b1', 'wipe'))

The points a device can be failed at are the strings it passes to `failIfRequested`:
`wipe`, `read`, `waitForReadySignal`, `go`, `shutters expose, before opening` and
`shutters expose, after opening`.

## Asserting

`Result` answers from outside the actor: `fileIds` for what came back, `stopped(actor)` for
how many times an illuminator was released, `isLit(actor)` for whether a lamp run was started
and never released, and `sent(actor, cmdHead)` / `firstAt(actor, cmdHead)` for the command
transcript. `sim.printTranscript()` prints the whole exchange when a scenario needs reading
rather than asserting.
