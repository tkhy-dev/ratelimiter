import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class RateLimiterTest {
    private static int passed;

    public static void main(String[] args) throws Exception {
        run("fixed window limit and rollover", RateLimiterTest::fixedWindowLimitAndRollover);
        run("fixed window repeated and backward timestamps", RateLimiterTest::fixedWindowTimestamps);
        run("fixed window shared counter and subject isolation", RateLimiterTest::fixedWindowSharedState);
        run("fixed window concurrent quota", RateLimiterTest::fixedWindowConcurrency);
        run("token bucket limit and refill", RateLimiterTest::tokenBucketLimitAndRefill);
        run("token bucket repeated and backward timestamps", RateLimiterTest::tokenBucketTimestamps);
        run("token bucket shared state and subject isolation", RateLimiterTest::tokenBucketSharedState);
        run("token bucket concurrent quota", RateLimiterTest::tokenBucketConcurrency);
        System.out.println("PASS: " + passed + " tests");
    }

    private static void fixedWindowLimitAndRollover() {
        var limiter = new FixedWindowRateLimiter.FixedWindow(
                new FixedWindowRateLimiter.Redis(), "alice", 1000, 2);
        check(limiter.allow(0), "first request");
        check(limiter.allow(999), "second request at end of window");
        check(!limiter.allow(999), "limit exceeded");
        check(limiter.allow(1000), "new window at exact boundary");
        check(limiter.allow(1999), "second request in new window");
        check(!limiter.allow(1999), "new window limit exceeded");
        check(limiter.allow(2000), "third window starts clean");
    }

    private static void fixedWindowTimestamps() {
        var limiter = new FixedWindowRateLimiter.FixedWindow(
                new FixedWindowRateLimiter.Redis(), "alice", 1000, 2);
        check(limiter.allow(100), "first request");
        check(limiter.allow(100), "repeated timestamp counts as another request");
        check(!limiter.allow(99), "backward timestamp rejected");
        check(!limiter.allow(100), "backward rejection did not reset quota");
        check(limiter.allow(1000), "forward boundary still rolls over");
        check(!limiter.allow(999), "old window cannot be revisited");
    }

    private static void fixedWindowSharedState() {
        var redis = new FixedWindowRateLimiter.Redis();
        var first = new FixedWindowRateLimiter.FixedWindow(redis, "alice", 1000, 1);
        var second = new FixedWindowRateLimiter.FixedWindow(redis, "alice", 1000, 1);
        var other = new FixedWindowRateLimiter.FixedWindow(redis, "bob", 1000, 1);
        check(first.allow(0), "first instance consumes shared quota");
        check(!second.allow(0), "second instance sees shared quota");
        check(other.allow(0), "different subject has independent quota");
    }

    private static void fixedWindowConcurrency() throws Exception {
        var redis = new FixedWindowRateLimiter.Redis();
        var one = new FixedWindowRateLimiter.FixedWindow(redis, "alice", 1000, 12);
        var two = new FixedWindowRateLimiter.FixedWindow(redis, "alice", 1000, 12);
        check(parallelAllowed(128, i -> (i % 2 == 0 ? one : two).allow(0)) == 12,
                "atomic shared counter must allow exactly 12 requests");
        check(!one.allow(-1), "backward timestamp remains rejected after concurrent calls");
    }

    private static void tokenBucketLimitAndRefill() {
        var bucket = new TokenBucketRateLimiter.TokenBucket(
                new TokenBucketRateLimiter.Redis(), "alice", 3, 1000);
        check(bucket.allow(0), "first token");
        check(bucket.allow(0), "second token");
        check(bucket.allow(0), "third token");
        check(!bucket.allow(0), "empty bucket");
        check(!bucket.allow(999), "no fractional token before interval");
        check(bucket.allow(1000), "one token at exact interval");
        check(!bucket.allow(1500), "partial interval is retained, not rounded up");
        check(bucket.allow(2000), "next full interval");
        check(!bucket.allow(2000), "same timestamp has no new token");
        check(bucket.allow(10000), "long pause refills up to capacity");
        check(bucket.allow(10000), "second stored token");
        check(bucket.allow(10000), "third stored token");
        check(!bucket.allow(10000), "capacity never exceeds three");
    }

    private static void tokenBucketTimestamps() {
        var bucket = new TokenBucketRateLimiter.TokenBucket(
                new TokenBucketRateLimiter.Redis(), "alice", 1, 1000);
        check(bucket.allow(1000), "initial token");
        check(!bucket.allow(1000), "repeat does not refill");
        check(!bucket.allow(999), "backward timestamp rejected");
        check(!bucket.allow(1000), "backward call did not restore token");
        check(bucket.allow(2000), "refill remains anchored at original timestamp");
    }

    private static void tokenBucketSharedState() {
        var redis = new TokenBucketRateLimiter.Redis();
        var first = new TokenBucketRateLimiter.TokenBucket(redis, "alice", 1, 1000);
        var second = new TokenBucketRateLimiter.TokenBucket(redis, "alice", 1, 1000);
        var other = new TokenBucketRateLimiter.TokenBucket(redis, "bob", 1, 1000);
        check(first.allow(0), "first instance consumes token");
        check(!second.allow(0), "second instance sees shared empty bucket");
        check(other.allow(0), "different subject has independent bucket");
    }

    private static void tokenBucketConcurrency() throws Exception {
        var redis = new TokenBucketRateLimiter.Redis();
        var one = new TokenBucketRateLimiter.TokenBucket(redis, "alice", 12, 1000);
        var two = new TokenBucketRateLimiter.TokenBucket(redis, "alice", 12, 1000);
        check(parallelAllowed(128, i -> (i % 2 == 0 ? one : two).allow(0)) == 12,
                "atomic shared bucket must allow exactly 12 requests");
        check(!one.allow(-1), "backward timestamp remains rejected after concurrent calls");
    }

    private interface Request {
        boolean allow(int index);
    }

    private static int parallelAllowed(int requests, Request request) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(16);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<Boolean>> results = new ArrayList<>();
            for (int i = 0; i < requests; i++) {
                final int index = i;
                results.add(pool.submit((Callable<Boolean>) () -> {
                    if (!start.await(10, TimeUnit.SECONDS)) {
                        throw new AssertionError("workers did not start");
                    }
                    return request.allow(index);
                }));
            }
            start.countDown();
            int allowed = 0;
            for (Future<Boolean> result : results) {
                if (result.get(10, TimeUnit.SECONDS)) {
                    allowed++;
                }
            }
            return allowed;
        } finally {
            pool.shutdownNow();
        }
    }

    private interface TestCase {
        void run() throws Exception;
    }

    private static void run(String name, TestCase test) throws Exception {
        test.run();
        passed++;
        System.out.println("PASS: " + name);
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
