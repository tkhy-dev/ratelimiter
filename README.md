# Rate Limiter
Co-authored with OpenAI Codex.

## Implemented
- **Fixed window:** Counts requests in fixed time buckets.
  - **Benefit:** Simple and efficient when boundary bursts are acceptable.
  - **Drawback:** Can allow two full bursts around a window boundary.
- **Token bucket:** Refills tokens over time and spends one per request.
  - **Benefit:** Allows controlled bursts while enforcing a long-term rate.
  - **Drawback:** Requires an atomic read-modify-write of token state.
