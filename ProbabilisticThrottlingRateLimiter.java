import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;

public class ProbabilisticThrottlingRateLimiter {
    static class Redis {
        private final ConcurrentHashMap<String, WindowState> windows = new ConcurrentHashMap<>();

        WindowState update(String key, WindowState initial,
                UnaryOperator<WindowState> operation) {
            // compute models Redis atomically updating all state stored at one key.
            return windows.compute(key, (ignored, current) ->
                    operation.apply(current == null ? initial : current));
        }
    }

    static class WindowState {
        final long windowStart;
        final int acceptedCount;
        final long lastTimestamp;

        WindowState(long windowStart, int acceptedCount, long lastTimestamp) {
            this.windowStart = windowStart;
            this.acceptedCount = acceptedCount;
            this.lastTimestamp = lastTimestamp;
        }
    }

    static class AllowResult {
        final boolean allowed;
        final WindowState state;

        AllowResult(boolean allowed, WindowState state) {
            this.allowed = allowed;
            this.state = state;
        }
    }

    static class ProbabilisticThrottling {
        private final Redis redis;
        private final String subject;
        private final long windowSizeMillis;
        private final int requestLimit;
        private final Random random;
        private long windowStart;
        private int acceptedCount;

        ProbabilisticThrottling(Redis redis, String subject, long windowSizeMillis,
                int requestLimit, Random random) {
            if (windowSizeMillis <= 0 || requestLimit <= 0) {
                throw new IllegalArgumentException("window size and request limit must be positive");
            }
            this.redis = redis;
            this.subject = subject;
            this.windowSizeMillis = windowSizeMillis;
            this.requestLimit = requestLimit;
            this.random = random;
        }

        boolean allow(long timestamp) {
            AtomicReference<AllowResult> result = new AtomicReference<>();
            long requestWindowStart = Math.floorDiv(timestamp, windowSizeMillis)
                    * windowSizeMillis;
            // One Redis key holds the active window, count, and latest timestamp.
            String key = "probabilistic-throttling:" + subject;

            redis.update(key, new WindowState(requestWindowStart, 0, timestamp), state -> {
                if (timestamp < state.lastTimestamp) {
                    result.set(new AllowResult(false, state));
                    return state;
                }

                int count = state.windowStart == requestWindowStart
                        ? state.acceptedCount : 0;
                // The hard limit prevents any window from accepting more than its limit.
                boolean allowed = count < requestLimit
                        // Acceptance becomes less likely as accepted requests accumulate.
                        && random.nextDouble() < 1.0 - (double) count / requestLimit;
                int updatedCount = allowed ? count + 1 : count;
                WindowState updated = new WindowState(
                        requestWindowStart, updatedCount, timestamp);
                result.set(new AllowResult(allowed, updated));
                return updated;
            });

            AllowResult allowResult = result.get();
            windowStart = allowResult.state.windowStart;
            acceptedCount = allowResult.state.acceptedCount;
            return allowResult.allowed;
        }
    }

    /*
     * Output:
     * 0 ALLOW windowStart=0 acceptedCount=1
     * 100 DENY windowStart=0 acceptedCount=1
     * 200 ALLOW windowStart=0 acceptedCount=2
     * 300 DENY windowStart=0 acceptedCount=2
     * 999 DENY windowStart=0 acceptedCount=2
     * 1000 ALLOW windowStart=1000 acceptedCount=1
     * 1100 ALLOW windowStart=1000 acceptedCount=2
     * 1200 DENY windowStart=1000 acceptedCount=2
     * 1300 ALLOW windowStart=1000 acceptedCount=3
     */
    public static void main(String[] args) {
        Redis redis = new Redis();
        ProbabilisticThrottling rateLimiter = new ProbabilisticThrottling(
                redis, "demo", 1000, 3, new Random(7));
        long[] timestamps = {0, 100, 200, 300, 999, 1000, 1100, 1200, 1300};

        for (long timestamp : timestamps) {
            boolean allowed = rateLimiter.allow(timestamp);
            System.out.printf("%d %s windowStart=%d acceptedCount=%d%n",
                    timestamp,
                    allowed ? "ALLOW" : "DENY",
                    rateLimiter.windowStart,
                    rateLimiter.acceptedCount);
        }
    }
}
