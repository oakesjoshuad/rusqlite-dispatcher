//! Time primitives for metrics and observability.
//!
//! This module provides type-safe timestamp and duration types with automatic
//! unit scaling for human-readable output. All values are stored internally
//! as microseconds for maximum precision.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Absolute timestamp (microseconds since UNIX epoch).
///
/// Provides type safety to prevent mixing timestamps with durations.
/// Internally stores microseconds for maximum precision while supporting
/// lossy conversions to milliseconds and seconds.
///
/// # Examples
///
/// ```ignore
/// use crate::Timestamp;
///
/// let now = Timestamp::now();
/// let micros = now.as_micros();
/// let millis = now.as_millis();
///
/// // Calculate elapsed time
/// # std::thread::sleep(std::time::Duration::from_millis(10));
/// let duration = now.elapsed();
/// println!("Elapsed: {}", duration); // Auto-scales: "10.23ms"
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Timestamp(u64);

/// Relative duration (microseconds).
///
/// Provides type safety to prevent mixing durations with timestamps.
/// Supports automatic human-readable formatting based on magnitude,
/// automatically choosing the most appropriate unit (μs, ms, s, m).
///
/// # Examples
///
/// ```ignore
/// use crate::Duration;
///
/// // Measure operation duration
/// let (result, duration) = Duration::measure(|| {
///     // expensive operation
///     42
/// });
///
/// println!("Took: {}", duration); // Auto-scales: "5.43ms" or "1.23s"
/// assert_eq!(result, 42);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Duration(u64);

impl Timestamp {
    /// Get current timestamp (microseconds since UNIX epoch).
    ///
    /// Uses saturating conversion to handle overflow gracefully.
    /// Overflow would only occur in year 586,524 AD.
    #[inline]
    pub(crate) fn now() -> Self {
        let micros = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("System time before UNIX epoch")
            .as_micros();

        // Saturate instead of panic on overflow (should never happen)
        Timestamp(micros.try_into().unwrap_or(u64::MAX))
    }

    /// Get raw microseconds value.
    #[inline]
    pub(crate) const fn as_micros(self) -> u64 {
        self.0
    }

    /// Get milliseconds (lossy conversion).
    #[inline]
    pub(crate) const fn as_millis(self) -> u64 {
        self.0 / 1_000
    }

    /// Get seconds (lossy conversion).
    #[inline]
    pub(crate) const fn as_secs(self) -> u64 {
        self.0 / 1_000_000
    }

    /// Calculate duration since this timestamp.
    #[inline]
    pub(crate) fn elapsed(self) -> Duration {
        let now = Timestamp::now();
        Duration(now.0.saturating_sub(self.0))
    }

    /// Load from atomic storage.
    #[inline]
    pub(crate) fn load_atomic(atomic: &AtomicU64, ordering: Ordering) -> Self {
        Timestamp(atomic.load(ordering))
    }

    /// Store to atomic storage.
    #[inline]
    pub(crate) fn store_atomic(self, atomic: &AtomicU64, ordering: Ordering) {
        atomic.store(self.0, ordering);
    }
}

impl Duration {
    /// Zero duration constant.
    pub(crate) const ZERO: Self = Duration(0);

    /// Create from microseconds.
    #[inline]
    pub(crate) const fn from_micros(micros: u64) -> Self {
        Duration(micros)
    }

    /// Create from std::time::Duration (safe conversion).
    ///
    /// Uses saturating conversion to handle overflow gracefully.
    #[inline]
    pub(crate) fn from_std(duration: std::time::Duration) -> Self {
        Duration(duration.as_micros().try_into().unwrap_or(u64::MAX))
    }

    /// Measure duration of a closure execution.
    ///
    /// Returns both the closure result and the measured duration.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use crate::Duration;
    ///
    /// let (result, duration) = Duration::measure(|| {
    ///     // expensive operation
    ///     42 * 2
    /// });
    ///
    /// assert_eq!(result, 84);
    /// assert!(duration.as_micros() > 0);
    /// ```
    #[inline]
    pub(crate) fn measure<F, R>(f: F) -> (R, Duration)
    where
        F: FnOnce() -> R,
    {
        let start = Instant::now();
        let result = f();
        let duration = Duration::from_std(start.elapsed());
        (result, duration)
    }

    /// Get raw microseconds value.
    #[inline]
    pub(crate) const fn as_micros(self) -> u64 {
        self.0
    }

    /// Get milliseconds (lossy conversion).
    #[inline]
    pub(crate) const fn as_millis(self) -> u64 {
        self.0 / 1_000
    }

    /// Get seconds (lossy conversion).
    #[inline]
    pub(crate) const fn as_secs(self) -> u64 {
        self.0 / 1_000_000
    }

    /// Get fractional seconds (high precision).
    #[inline]
    pub(crate) fn as_secs_f64(self) -> f64 {
        self.0 as f64 / 1_000_000.0
    }

    /// Load from atomic storage.
    #[inline]
    pub(crate) fn load_atomic(atomic: &AtomicU64, ordering: Ordering) -> Self {
        Duration(atomic.load(ordering))
    }

    /// Store to atomic storage.
    #[inline]
    pub(crate) fn store_atomic(self, atomic: &AtomicU64, ordering: Ordering) {
        atomic.store(self.0, ordering);
    }

    /// Check if duration is zero.
    #[inline]
    pub(crate) const fn is_zero(self) -> bool {
        self.0 == 0
    }
}

/// Display implementation with automatic unit scaling.
///
/// Chooses the most appropriate unit based on magnitude:
/// - 0: "0μs"
/// - < 1ms: "500μs"
/// - < 1s: "5.43ms"
/// - < 1min: "12.34s"
/// - >= 1min: "5.2m"
impl std::fmt::Display for Duration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            0 => write!(f, "0μs"),
            micros if micros < 1_000 => write!(f, "{}μs", micros),
            micros if micros < 1_000_000 => write!(f, "{:.2}ms", micros as f64 / 1_000.0),
            micros if micros < 60_000_000 => write!(f, "{:.2}s", micros as f64 / 1_000_000.0),
            micros => write!(f, "{:.1}m", micros as f64 / 60_000_000.0),
        }
    }
}

impl std::fmt::Display for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}μs", self.0)
    }
}

// Arithmetic operations

impl std::ops::Add for Duration {
    type Output = Duration;

    #[inline]
    fn add(self, rhs: Duration) -> Duration {
        Duration(self.0.saturating_add(rhs.0))
    }
}

impl std::ops::Sub for Duration {
    type Output = Duration;

    #[inline]
    fn sub(self, rhs: Duration) -> Duration {
        Duration(self.0.saturating_sub(rhs.0))
    }
}

impl std::ops::Add<Duration> for Timestamp {
    type Output = Timestamp;

    #[inline]
    fn add(self, rhs: Duration) -> Timestamp {
        Timestamp(self.0.saturating_add(rhs.0))
    }
}

impl std::ops::Sub<Duration> for Timestamp {
    type Output = Timestamp;

    #[inline]
    fn sub(self, rhs: Duration) -> Timestamp {
        Timestamp(self.0.saturating_sub(rhs.0))
    }
}

impl std::ops::Sub for Timestamp {
    type Output = Duration;

    #[inline]
    fn sub(self, rhs: Timestamp) -> Duration {
        Duration(self.0.saturating_sub(rhs.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_now_returns_reasonable_value() {
        let ts = Timestamp::now();
        // Should be somewhere around 2025 (> 1.7 trillion microseconds since epoch)
        assert!(ts.as_micros() > 1_700_000_000_000_000);
    }

    #[test]
    fn timestamp_conversions() {
        let ts = Timestamp(1_234_567_890_000); // microseconds
        assert_eq!(ts.as_micros(), 1_234_567_890_000);
        assert_eq!(ts.as_millis(), 1_234_567_890);
        assert_eq!(ts.as_secs(), 1_234_567);
    }

    #[test]
    fn duration_from_micros() {
        let d = Duration::from_micros(1_500);
        assert_eq!(d.as_micros(), 1_500);
        assert_eq!(d.as_millis(), 1);
    }

    #[test]
    fn duration_display_zero() {
        let d = Duration::ZERO;
        assert_eq!(d.to_string(), "0μs");
    }

    #[test]
    fn duration_display_microseconds() {
        let d = Duration::from_micros(500);
        assert_eq!(d.to_string(), "500μs");
    }

    #[test]
    fn duration_display_milliseconds() {
        let d = Duration::from_micros(1_500);
        assert_eq!(d.to_string(), "1.50ms");

        let d = Duration::from_micros(999_999);
        assert_eq!(d.to_string(), "1000.00ms");
    }

    #[test]
    fn duration_display_seconds() {
        let d = Duration::from_micros(5_432_100);
        assert_eq!(d.to_string(), "5.43s");

        let d = Duration::from_micros(30_000_000);
        assert_eq!(d.to_string(), "30.00s");
    }

    #[test]
    fn duration_display_minutes() {
        let d = Duration::from_micros(65_000_000);
        assert_eq!(d.to_string(), "1.1m");

        let d = Duration::from_micros(300_000_000);
        assert_eq!(d.to_string(), "5.0m");
    }

    #[test]
    fn duration_measure_closure() {
        let (result, duration) = Duration::measure(|| {
            std::thread::sleep(std::time::Duration::from_millis(10));
            42
        });

        assert_eq!(result, 42);
        assert!(duration.as_millis() >= 10);
    }

    #[test]
    fn duration_addition() {
        let d1 = Duration::from_micros(100_000);
        let d2 = Duration::from_micros(50_000);
        let sum = d1 + d2;
        assert_eq!(sum.as_micros(), 150_000);
    }

    #[test]
    fn duration_subtraction() {
        let d1 = Duration::from_micros(100_000);
        let d2 = Duration::from_micros(30_000);
        let diff = d1 - d2;
        assert_eq!(diff.as_micros(), 70_000);
    }

    #[test]
    fn duration_subtraction_saturates() {
        let d1 = Duration::from_micros(30_000);
        let d2 = Duration::from_micros(100_000);
        let diff = d1 - d2;
        assert_eq!(diff, Duration::ZERO);
    }

    #[test]
    fn timestamp_add_duration() {
        let ts = Timestamp(1_000_000);
        let d = Duration::from_micros(500_000);
        let result = ts + d;
        assert_eq!(result.as_micros(), 1_500_000);
    }

    #[test]
    fn timestamp_sub_duration() {
        let ts = Timestamp(1_000_000);
        let d = Duration::from_micros(300_000);
        let result = ts - d;
        assert_eq!(result.as_micros(), 700_000);
    }

    #[test]
    fn timestamp_sub_timestamp() {
        let ts1 = Timestamp(2_000_000);
        let ts2 = Timestamp(1_500_000);
        let duration = ts1 - ts2;
        assert_eq!(duration.as_micros(), 500_000);
    }

    #[test]
    fn timestamp_elapsed() {
        let ts = Timestamp::now();
        std::thread::sleep(std::time::Duration::from_millis(10));
        let elapsed = ts.elapsed();
        assert!(elapsed.as_millis() >= 10);
    }

    #[test]
    fn timestamp_atomic_operations() {
        let atomic = AtomicU64::new(0);
        let ts = Timestamp(1_234_567);

        ts.store_atomic(&atomic, Ordering::Relaxed);
        let loaded = Timestamp::load_atomic(&atomic, Ordering::Relaxed);

        assert_eq!(loaded, ts);
    }

    #[test]
    fn duration_atomic_operations() {
        let atomic = AtomicU64::new(0);
        let d = Duration::from_micros(500_000);

        d.store_atomic(&atomic, Ordering::Relaxed);
        let loaded = Duration::load_atomic(&atomic, Ordering::Relaxed);

        assert_eq!(loaded, d);
    }

    #[test]
    fn duration_is_zero() {
        assert!(Duration::ZERO.is_zero());
        assert!(Duration::from_micros(0).is_zero());
        assert!(!Duration::from_micros(1).is_zero());
    }

    #[test]
    fn type_safety_prevents_mixing() {
        // This won't compile - type safety works!
        // let ts = Timestamp::now();
        // let d = Duration::from_micros(5_000_000);
        // let _ = ts + ts; // ❌ Error: no Add impl for Timestamp + Timestamp
    }
}
