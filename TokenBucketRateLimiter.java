import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;

public class TokenBucketRateLimiter {
    static class Redis {
        private final ConcurrentHashMap<String, BucketState> buckets = new ConcurrentHashMap<>();

        BucketState update(String key, BucketState initial,
                UnaryOperator<BucketState> operation) {
            // Redis only provides an atomic update for one key. It does not know
            // how token refill, consumption, or the allow decision works.
            return buckets.compute(key, (ignored, current) ->
                    operation.apply(current == null ? initial : current));
        }
    }

    static class BucketState {
        final long tokens;
        final long lastRefillTimestamp;

        BucketState(long tokens, long lastRefillTimestamp) {
            this.tokens = tokens;
            this.lastRefillTimestamp = lastRefillTimestamp;
        }
    }

    static class ConsumeResult {
        final boolean consumed;
        final long remainingTokens;

        ConsumeResult(boolean consumed, long remainingTokens) {
            this.consumed = consumed;
            this.remainingTokens = remainingTokens;
        }
    }

    static class TokenBucket {
        private final Redis redis;
        private final String subject;
        private final int capacity;
        private final long refillIntervalMillis;
        private long tokens;

        TokenBucket(Redis redis, String subject, int capacity, long refillIntervalMillis) {
            if (capacity <= 0 || refillIntervalMillis <= 0) {
                throw new IllegalArgumentException("capacity and refill interval must be positive");
            }
            this.redis = redis;
            this.subject = subject;
            this.capacity = capacity;
            this.refillIntervalMillis = refillIntervalMillis;
        }

        boolean allow(long timestamp) {
            AtomicReference<ConsumeResult> result = new AtomicReference<>();
            String key = "token-bucket:" + subject;

            // TokenBucket defines the state transition; Redis only runs it atomically.
            // In real Redis, the same transition would be a Lua script or Function.
            redis.update(key, new BucketState(capacity, timestamp), bucket -> {
                if (timestamp < bucket.lastRefillTimestamp) {
                    result.set(new ConsumeResult(false, bucket.tokens));
                    return bucket;
                }

                long elapsed = timestamp - bucket.lastRefillTimestamp;
                long refilled = elapsed / refillIntervalMillis;
                long availableTokens = Math.min(capacity, bucket.tokens + refilled);
                long lastRefillTimestamp = bucket.lastRefillTimestamp
                        + refilled * refillIntervalMillis;

                boolean consumed = availableTokens > 0;
                long remainingTokens = consumed ? availableTokens - 1 : availableTokens;
                result.set(new ConsumeResult(consumed, remainingTokens));
                return new BucketState(remainingTokens, lastRefillTimestamp);
            });

            ConsumeResult consumeResult = result.get();
            tokens = consumeResult.remainingTokens;
            return consumeResult.consumed;
        }
    }

    /*
     * Output:
     * 0 ALLOW tokens=2
     * 0 ALLOW tokens=1
     * 0 ALLOW tokens=0
     * 0 DENY tokens=0
     * 999 DENY tokens=0
     * 1000 ALLOW tokens=0
     * 2000 ALLOW tokens=0
     * 2000 DENY tokens=0
     * 3000 ALLOW tokens=0
     */
    public static void main(String[] args) {
        Redis redis = new Redis();
        TokenBucket rateLimiter = new TokenBucket(redis, "demo", 3, 1000);
        long[] timestamps = {0, 0, 0, 0, 999, 1000, 2000, 2000, 3000};

        for (long timestamp : timestamps) {
            boolean allowed = rateLimiter.allow(timestamp);
            System.out.printf("%d %s tokens=%d%n",
                    timestamp, allowed ? "ALLOW" : "DENY", rateLimiter.tokens);
        }
    }
}
