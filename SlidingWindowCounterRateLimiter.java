import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;

public class SlidingWindowCounterRateLimiter {
    static class Redis {
        private final ConcurrentHashMap<String, CounterState> counters = new ConcurrentHashMap<>();

        CounterState update(String key, CounterState initial,
                UnaryOperator<CounterState> operation) {
            // A real Redis key would atomically rotate and update both counters.
            return counters.compute(key, (ignored, current) ->
                    operation.apply(current == null ? initial : current));
        }
    }

    static class CounterState {
        final long windowStart;
        final int currentCount;
        final int previousCount;
        final long lastTimestamp;

        CounterState(long windowStart, int currentCount, int previousCount, long lastTimestamp) {
            this.windowStart = windowStart;
            this.currentCount = currentCount;
            this.previousCount = previousCount;
            this.lastTimestamp = lastTimestamp;
        }
    }

    static class SlidingWindowCounter {
        private final Redis redis;
        private final String subject;
        private final long windowSizeMillis;
        private final int requestLimit;
        private double estimatedCount;

        SlidingWindowCounter(Redis redis, String subject, long windowSizeMillis,
                int requestLimit) {
            if (windowSizeMillis <= 0 || requestLimit <= 0) {
                throw new IllegalArgumentException("window size and request limit must be positive");
            }
            this.redis = redis;
            this.subject = subject;
            this.windowSizeMillis = windowSizeMillis;
            this.requestLimit = requestLimit;
        }

        boolean allow(long timestamp) {
            AtomicReference<Boolean> allowed = new AtomicReference<>(false);
            AtomicReference<Double> estimate = new AtomicReference<>();
            long requestedWindowStart = timestamp / windowSizeMillis * windowSizeMillis;

            // One Redis key stores the current and previous counters per subject.
            String key = "sliding-window-counter:" + subject;
            CounterState initial = new CounterState(requestedWindowStart, 0, 0, Long.MIN_VALUE);
            redis.update(key, initial, state -> {
                if (timestamp < state.lastTimestamp) {
                    estimate.set(weightedCount(state, timestamp));
                    return state;
                }

                CounterState rotated = rotate(state, requestedWindowStart);
                double count = weightedCount(rotated, timestamp);
                if (count + 1 <= requestLimit) {
                    allowed.set(true);
                    count++;
                    rotated = new CounterState(rotated.windowStart,
                            rotated.currentCount + 1, rotated.previousCount, timestamp);
                } else {
                    rotated = new CounterState(rotated.windowStart,
                            rotated.currentCount, rotated.previousCount, timestamp);
                }
                estimate.set(count);
                return rotated;
            });

            estimatedCount = estimate.get();
            return allowed.get();
        }

        private CounterState rotate(CounterState state, long newWindowStart) {
            if (newWindowStart == state.windowStart) {
                return state;
            }
            // Keep the old current count only when rotating exactly one window.
            int previousCount = newWindowStart - state.windowStart == windowSizeMillis
                    ? state.currentCount : 0;
            return new CounterState(newWindowStart, 0, previousCount, state.lastTimestamp);
        }

        private double weightedCount(CounterState state, long timestamp) {
            // The previous count's weight is the unelapsed fraction of this window.
            double weight = 1.0 - (double) (timestamp - state.windowStart) / windowSizeMillis;
            return state.currentCount + state.previousCount * weight;
        }
    }

    /*
     * Output:
     * 0 ALLOW estimatedCount=1.00
     * 100 ALLOW estimatedCount=2.00
     * 200 ALLOW estimatedCount=3.00
     * 999 DENY estimatedCount=3.00
     * 1000 DENY estimatedCount=3.00
     * 1334 ALLOW estimatedCount=3.00
     * 1667 ALLOW estimatedCount=3.00
     * 1500 DENY estimatedCount=3.50
     */
    public static void main(String[] args) {
        Redis redis = new Redis();
        SlidingWindowCounter rateLimiter = new SlidingWindowCounter(redis, "demo", 1000, 3);
        long[] timestamps = {0, 100, 200, 999, 1000, 1334, 1667, 1500};

        for (long timestamp : timestamps) {
            boolean allowed = rateLimiter.allow(timestamp);
            System.out.printf("%d %s estimatedCount=%.2f%n",
                    timestamp, allowed ? "ALLOW" : "DENY", rateLimiter.estimatedCount);
        }
    }
}
