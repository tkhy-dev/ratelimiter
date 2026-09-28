import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;

public class SlidingWindowCounterRateLimiter {
    static class Redis {
        private final ConcurrentHashMap<String, CounterState> counters =
                new ConcurrentHashMap<>();

        CounterState update(String key, CounterState initial,
                UnaryOperator<CounterState> operation) {
            // Redis only applies one atomic update. The limiter defines the policy.
            return counters.compute(key, (ignored, current) ->
                    operation.apply(current == null ? initial : current));
        }
    }

    static class CounterState {
        final long windowNumber;
        final int previousCount;
        final int currentCount;
        final long previousTimestamp;

        CounterState(long windowNumber, int previousCount, int currentCount,
                long previousTimestamp) {
            this.windowNumber = windowNumber;
            this.previousCount = previousCount;
            this.currentCount = currentCount;
            this.previousTimestamp = previousTimestamp;
        }
    }

    static class Result {
        final boolean allowed;
        final double estimatedCount;

        Result(boolean allowed, double estimatedCount) {
            this.allowed = allowed;
            this.estimatedCount = estimatedCount;
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
            AtomicReference<Result> result = new AtomicReference<>();
            long requestedWindow = timestamp / windowSizeMillis;

            // A real Redis implementation would atomically rotate and update counters.
            redis.update("sliding-counter:" + subject,
                    new CounterState(requestedWindow, 0, 0, Long.MIN_VALUE), state -> {
                        if (timestamp < state.previousTimestamp) {
                            result.set(new Result(false, state.currentCount));
                            return state;
                        }

                        int previousCount = state.previousCount;
                        int currentCount = state.currentCount;
                        if (requestedWindow > state.windowNumber) {
                            previousCount = requestedWindow == state.windowNumber + 1
                                    ? currentCount : 0;
                            currentCount = 0;
                        }

                        long elapsed = timestamp % windowSizeMillis;
                        double previousWeight = 1.0
                                - (double) elapsed / windowSizeMillis;
                        double estimate = currentCount + 1
                                + previousCount * previousWeight;
                        boolean allowed = estimate <= requestLimit;
                        if (allowed) {
                            currentCount++;
                        }

                        result.set(new Result(allowed, estimate));
                        return new CounterState(requestedWindow, previousCount,
                                currentCount, timestamp);
                    });

            Result current = result.get();
            estimatedCount = current.estimatedCount;
            return current.allowed;
        }
    }

    /*
     * Output:
     * 0 ALLOW estimatedCount=1.00
     * 100 ALLOW estimatedCount=2.00
     * 200 ALLOW estimatedCount=3.00
     * 300 DENY estimatedCount=4.00
     * 999 DENY estimatedCount=4.00
     * 1000 DENY estimatedCount=4.00
     * 1100 DENY estimatedCount=3.70
     * 1200 DENY estimatedCount=3.40
     * 1340 ALLOW estimatedCount=2.98
     * 1400 DENY estimatedCount=3.80
     * 2000 ALLOW estimatedCount=2.00
     */
    public static void main(String[] args) {
        Redis redis = new Redis();
        SlidingWindowCounter rateLimiter =
                new SlidingWindowCounter(redis, "demo", 1000, 3);
        long[] timestamps = {0, 100, 200, 300, 999, 1000, 1100, 1200, 1340, 1400, 2000};

        for (long timestamp : timestamps) {
            boolean allowed = rateLimiter.allow(timestamp);
            System.out.printf("%d %s estimatedCount=%.2f%n",
                    timestamp, allowed ? "ALLOW" : "DENY", rateLimiter.estimatedCount);
        }
    }
}
