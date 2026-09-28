import java.util.ArrayDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;

public class SlidingWindowLogRateLimiter {
    static class Redis {
        private final ConcurrentHashMap<String, LogState> logs = new ConcurrentHashMap<>();

        LogState update(String key, LogState initial, UnaryOperator<LogState> operation) {
            // Redis only applies one atomic update. The limiter defines the policy.
            return logs.compute(key, (ignored, current) ->
                    operation.apply(current == null ? initial : current));
        }
    }

    static class LogState {
        final ArrayDeque<Long> timestamps;
        long previousTimestamp = Long.MIN_VALUE;

        LogState() {
            timestamps = new ArrayDeque<>();
        }
    }

    static class Result {
        final boolean allowed;
        final int requestCount;

        Result(boolean allowed, int requestCount) {
            this.allowed = allowed;
            this.requestCount = requestCount;
        }
    }

    static class SlidingWindowLog {
        private final Redis redis;
        private final String subject;
        private final long windowSizeMillis;
        private final int requestLimit;
        private int requestCount;

        SlidingWindowLog(Redis redis, String subject, long windowSizeMillis,
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

            // A real Redis implementation would atomically trim and update a sorted set.
            redis.update("sliding-log:" + subject, new LogState(), state -> {
                if (timestamp < state.previousTimestamp) {
                    result.set(new Result(false, state.timestamps.size()));
                    return state;
                }
                state.previousTimestamp = timestamp;

                long cutoff = timestamp - windowSizeMillis;
                while (!state.timestamps.isEmpty()
                        && state.timestamps.peekFirst() <= cutoff) {
                    state.timestamps.removeFirst();
                }

                boolean allowed = state.timestamps.size() < requestLimit;
                if (allowed) {
                    state.timestamps.addLast(timestamp);
                }
                result.set(new Result(allowed, state.timestamps.size()));
                return state;
            });

            Result current = result.get();
            requestCount = current.requestCount;
            return current.allowed;
        }
    }

    /*
     * Output:
     * 0 ALLOW requestCount=1
     * 100 ALLOW requestCount=2
     * 200 ALLOW requestCount=3
     * 300 DENY requestCount=3
     * 999 DENY requestCount=3
     * 1000 ALLOW requestCount=3
     * 1100 ALLOW requestCount=3
     * 1200 ALLOW requestCount=3
     * 1300 DENY requestCount=3
     */
    public static void main(String[] args) {
        Redis redis = new Redis();
        SlidingWindowLog rateLimiter = new SlidingWindowLog(redis, "demo", 1000, 3);
        long[] timestamps = {0, 100, 200, 300, 999, 1000, 1100, 1200, 1300};

        for (long timestamp : timestamps) {
            boolean allowed = rateLimiter.allow(timestamp);
            System.out.printf("%d %s requestCount=%d%n",
                    timestamp, allowed ? "ALLOW" : "DENY", rateLimiter.requestCount);
        }
    }
}
