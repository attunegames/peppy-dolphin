# Peppy fork of Slippi Dolphin

A fork of [project-slippi/Ishiiruka](https://github.com/project-slippi/Ishiiruka) that
replaces Slippi's matchmaking with our own, so Peppy can run persistent rooms
where a queue of players rotates through matches without relaunching Dolphin.

Everything below the introduction is untouched. Rollback, the ENet connection,
the character select handshake and the game itself are Slippi's and stay that
way — the only thing being replaced is how two clients learn about each other.

## Why this exists

Peppy today automates the stock Slippi client from outside: it writes gecko
codes, scripts the menus, and relaunches Dolphin for every new opponent because
the opponent's connect code is baked into config at boot.

Slippi itself has no such limit — ranked and unranked change opponents without
relaunching. The relaunch is an artifact of driving direct mode from outside.
Owning the matchmaking step removes it, and most of Peppy's fragile automation
along with it.

## What changes

`Source/Core/Core/Slippi/SlippiMatchmaking.cpp`, and only two functions in it:

- `startMatchmaking()` — instead of an ENet connection to `mm.slippi.gg:43113`,
  send a STUN binding request from the same socket and publish the resulting
  endpoint to Supabase.
- `handleMatchmaking()` — instead of reading `get-ticket-resp` off that ENet
  connection, poll Supabase and synthesise the same JSON.

Everything downstream consumes the parsed result, so nothing else needs to know.

### Why STUN

Slippi's matchmaking connection is deliberately sent *from the netplay port* —
their own comment says so — because that outbound packet opens the NAT mapping
the opponent will later connect to. The server is not just a directory; it is an
accomplice in the hole punch.

A STUN request from that same socket does the identical thing without being a
server we have to run. Public STUN servers are free, so there is nothing to host
and nothing to pay for.

The limitation is symmetric NAT, where the router assigns a different mapping per
destination and the observed endpoint is not the one the peer will reach. Slippi
has exactly the same limitation for exactly the same reason; those players need
port forwarding either way.

### Identity

No Slippi account. Players choose a name in Peppy on first launch, and Supabase
assigns the disambiguating number. `user.json` and the `playKey` in it are never
read, which resolves a constraint that has shaped Peppy since the beginning.

## Build

Do not build this locally. `.github/workflows/pr-build.yml` builds a Windows
Netplay artifact on every push, on GitHub's runners.

The workflow is trimmed relative to upstream, because private repos have a
limited free minute budget and Windows bills at 2x, macOS at 10x:

- Windows matrix reduced to `Netplay` (no Playback)
- `linux` and `macOS` jobs disabled
- `check_rust_commit` disabled — it enforces upstream's merge policy, not ours

Re-enable them if this ever goes public, where minutes are unlimited.

## Status

Milestone 1: baseline build from unmodified Slippi source. In progress.
Milestone 2: STUN + Supabase matchmaking.
Milestone 3: two players, one room, a real game.
