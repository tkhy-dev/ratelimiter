import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;

public class SlidingWindowLogRateLimiter {
    static class Redis {
        private final ConcurrentHashMap<String, LogState> logs = new ConcurrentHashMap<>();

        LogState update(String key, LogState initial, UnaryOperator<LogState> operation) {
            // A real Redis sorted set would atomically trim, count, and add by score.
            return logs.compute(key, (ignored, current) ->
                    operation.apply(current == null ? initial : current));
        }
    }

    static class LogState {
        final Deque<Long> acceptedTimestamps;
        final long lastTimestamp;

        LogState(Deque<Long> acceptedTimestamps, long lastTimestamp) {
            this.acceptedTimestamps = acceptedTimestamps;
            this.lastTimestamp = lastTimestamp;
        }
    }

    static class SlidingWindowLog {
        private final Redis redis;
        private final String subject;
        private final long windowSizeMillis;
        private final int requestLimit;
        private int requestCount;

        SlidingWindowLog(Redis redis, String subject, long windowSizeMillis, int requestLimit) {
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
            AtomicReference<Integer> count = new AtomicReference<>();

            // One sorted-set key holds the accepted timestamps for each subject.
            String key = "sliding-window-log:" + subject;
            redis.update(key, new LogState(new ArrayDeque<>(), Long.MIN_VALUE), state -> {
                Deque<Long> timestamps = new ArrayDeque<>(state.acceptedTimestamps);
                if (timestamp < state.lastTimestamp) {
                    count.set(timestamps.size());
                    return state;
                }

                long cutoff = timestamp - windowSizeMillis;
                while (!timestamps.isEmpty() && timestamps.peekFirst() <= cutoff) {
                    timestamps.removeFirst();
                }
                if (timestamps.size() < requestLimit) {
                    timestamps.addLast(timestamp);
                    allowed.set(true);
                }
                count.set(timestamps.size());
                return new LogState(timestamps, timestamp);
            });

            requestCount = count.get();
            return allowed.get();
        }
    }

    /*
     * Output:
     * 0 ALLOW requestCount=1
     * 100 ALLOW requestCount=2
     * 200 ALLOW requestCount=3
     * 999 DENY requestCount=3
     * 1000 ALLOW requestCount=3
     * 1099 DENY requestCount=3
     * 1100 ALLOW requestCount=3
     * 1000 DENY requestCount=3
     */
    public static void main(String[] args) {
        Redis redis = new Redis();
        SlidingWindowLog rateLimiter = new SlidingWindowLog(redis, "demo", 1000, 3);
        long[] timestamps = {0, 100, 200, 999, 1000, 1099, 1100, 1000};

        for (long timestamp : timestamps) {
            boolean allowed = rateLimiter.allow(timestamp);
            System.out.printf("%d %s requestCount=%d%n",
                    timestamp, allowed ? "ALLOW" : "DENY", rateLimiter.requestCount);
        }
    }
}
