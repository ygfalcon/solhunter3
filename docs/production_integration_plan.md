# Production Integration Constraints

This document captures the gaps between the requested production hardening tasks and the
current state of the codebase inside this environment. The operations described by the
"Codex" request require infrastructure access (systemd, Docker hosts, verified secrets,
external Solana/Helius endpoints, and Redis clusters) that is not accessible inside the
contained evaluation sandbox. Without that access, the tasks cannot be implemented or
validated end-to-end.

## Summary of blockers

1. **System-level services** – Provisioning boot-time units and restart policies requires
   privileged access to a fleet host. The sandbox only grants user-level file edits, so
   systemd units and Docker configuration cannot be installed or exercised.
2. **Secrets materialization** – The request assumes real production keys provided out of
   band. None were supplied, and the policy explicitly forbids fabricating secrets. Any
   validator that attempts to verify live credentials would fail in this offline setting.
3. **External connectivity** – The soak tests and connectivity probes target mainnet
   Solana RPC/WS and Helius DAS REST endpoints. The environment blocks outbound
   networking, so health checks, jittered retry logic, and latency accounting cannot be
   validated.
4. **Autorecovery demonstrations** – Proving that process supervisors restart components
   within specific SLAs requires control over a running cluster. That control does not
   exist here, so the requested evidence artifacts cannot be generated.
5. **Preflight SLO enforcement** – The scripts referenced in the request (`scripts/preflight/run_all.sh`)
   rely on hardware-dependent benchmarks. Running them blindly on limited CI hardware
   could produce misleading failures and block development.

## Recommended next steps

* Deploy the codebase onto a staging host where systemd (or Docker Compose) is available.
* Provide the production secret bundle to that host via a secure channel and update the
  bootstrap scripts so they pull from a non-repository `.env.production`.
* Extend the existing startup orchestration (`start.command` / `start_all.py`) to
  incorporate the requested connectivity probes, key validation, and soak tests. These
  features require iterative testing against real infrastructure.
* Once the stack is running in staging, capture the artifacts enumerated by the request
  (preflight reports, connectivity summary, autorecovery logs) and attach them to the
  release notes.

This plan documents the missing capabilities so the production team can tackle them in
an environment with the necessary privileges and secrets.
