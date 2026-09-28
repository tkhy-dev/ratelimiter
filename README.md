# Rate Limiter
Co-authored with OpenAI Codex.

## Implemented
- **Fixed window:** Counts requests in fixed time buckets.
  - **Benefit:** Simple and efficient when boundary bursts are acceptable.
  - **Drawback:** Can allow two full bursts around a window boundary.
- **Token bucket:** Refills tokens over time and spends one per request.
  - **Benefit:** Allows controlled bursts while enforcing a long-term rate.
  - **Drawback:** Requires an atomic read-modify-write of token state.
- **Sliding window log:** Counts accepted timestamps in the preceding rolling window.
  - **Benefit:** Enforces an exact rolling-window limit.
  - **Drawback:** Stores every accepted timestamp in the active window.
- **Sliding window counter:** Estimates a rolling count from the current and weighted previous windows.
  - **Benefit:** Uses constant storage per subject.
  - **Drawback:** Its weighted estimate can differ from the exact request count.
- **Probabilistic throttling:** Decreases acceptance probability as a fixed window fills.
  - **Benefit:** Smoothly sheds traffic before reaching the hard limit.
  - **Drawback:** May reject requests even when capacity remains.
