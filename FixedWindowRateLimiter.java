import java.util.concurrent.ConcurrentHashMap;

public class FixedWindowRateLimiter {
    static class Redis {
        private final ConcurrentHashMap<String, Integer> counts = new ConcurrentHashMap<>();

        int increment(String key) {
            return counts.merge(key, 1, Integer::sum);
        }
    }

    static class FixedWindow {
        private final Redis redis;
        private final String subject;
        private final long windowSizeMillis;
        private final int requestLimit;
        private long previousTimestamp = Long.MIN_VALUE;
        private long windowStart;
        private int requestCount;

        FixedWindow(Redis redis, String subject, long windowSizeMillis, int requestLimit) {
            this.redis = redis;
            this.subject = subject;
            this.windowSizeMillis = windowSizeMillis;
            this.requestLimit = requestLimit;
        }

        boolean allow(long timestamp) {
            if (timestamp < previousTimestamp) {
                return false;
            }
            previousTimestamp = timestamp;

            long windowNumber = timestamp / windowSizeMillis;
            windowStart = windowNumber * windowSizeMillis;

            // Each subject and time window gets an independent Redis counter.
            // For example: rate-limit:demo:0, then rate-limit:demo:1.
            // A real Redis key should expire shortly after its window ends.
            String counterKey = "rate-limit:" + subject + ":" + windowNumber;
            requestCount = redis.increment(counterKey);

            // Redis only counts attempts; the rate limiter owns the policy decision.
            return requestCount <= requestLimit;
        }
    }

    /*
     * Output:
     * 0 ALLOW windowStart=0 requestCount=1
     * 100 ALLOW windowStart=0 requestCount=2
     * 200 ALLOW windowStart=0 requestCount=3
     * 300 DENY windowStart=0 requestCount=4
     * 999 DENY windowStart=0 requestCount=5
     * 1000 ALLOW windowStart=1000 requestCount=1
     * 1100 ALLOW windowStart=1000 requestCount=2
     * 1200 ALLOW windowStart=1000 requestCount=3
     * 1300 DENY windowStart=1000 requestCount=4
     */
    public static void main(String[] args) {
        Redis redis = new Redis();
        FixedWindow rateLimiter = new FixedWindow(redis, "demo", 1000, 3);
        long[] timestamps = {0, 100, 200, 300, 999, 1000, 1100, 1200, 1300};

        for (long timestamp : timestamps) {
            boolean allowed = rateLimiter.allow(timestamp);
            System.out.printf("%d %s windowStart=%d requestCount=%d%n",
                    timestamp,
                    allowed ? "ALLOW" : "DENY",
                    rateLimiter.windowStart,
                    rateLimiter.requestCount);
        }
    }
}
