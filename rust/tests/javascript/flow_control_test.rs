// JS: // Copyright (c) 2026 Cloudflare, Inc.
// JS: // Licensed under the MIT license found in the LICENSE.txt file or at:
// JS: //     https://opensource.org/license/mit
// JS:
// JS: import { expect, it, describe } from "vitest"
// JS: import { FlowController, type SendToken } from "../src/streams.js"
use crate::streams::{FlowController, SendToken};
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
// JS:
// JS: // To emulate random-ish chunk sizes while keeping the test reproducible, we cycle through this
// JS: // list of sizes.
// JS: const CHUNK_SIZES = [32 * 1024, 4 * 1024, 16000, 12345, 16, 9999, 4321, 8];
const CHUNK_SIZES: [f64; 8] = [
    32.0 * 1024.0,
    4.0 * 1024.0,
    16000.0,
    12345.0,
    16.0,
    9999.0,
    4321.0,
    8.0,
];
// JS:
// RUST-DIFF(Send clock): the fake clock is shared through an atomic, as the flow controller's clock is Send.
struct Clock(AtomicU64);

impl Clock {
    fn new(value: f64) -> Self {
        Self(AtomicU64::new(value.to_bits()))
    }

    fn get(&self) -> f64 {
        f64::from_bits(self.0.load(Ordering::Relaxed))
    }

    fn set(&self, value: f64) {
        self.0.store(value.to_bits(), Ordering::Relaxed);
    }
}

// JS: // Helper: simulates a stream with a fake clock and set RTT and bandwidth. Sends chunks, advances
// JS: // time, and acks in order.
// JS: class StreamSimulator {
struct StreamSimulator {
    // JS:   // Default RTT of 100ms.
    // JS:   rtt = 100;
    rtt: f64,
    // JS:
    // JS:   // Default bandwidth of 10kB/ms = 10MB/s = 1MB/RTT. This is larger than the initial window size
    // JS:   // of 256k, so the window should grow if staurated. Individual test cases may set something
    // JS:   // different, or modify the properties during the test.
    // JS:   bandwidth = 10 * 1024;
    bandwidth: f64,
    // JS:
    // JS:   // Convenience getter to calculate BDP.
    // JS:   get bdp() { return this.rtt * this.bandwidth; }
    // Implemented by `bdp()` below.
    // JS:
    // JS:   // Current simulated time.
    // JS:   t = 0;
    t: Arc<Clock>,
    // JS:
    // JS:   // Are we currently blocked, according to the flow controller's return values from onSend/onAck?
    // JS:   blocked = false;
    blocked: bool,
    // JS:
    // JS:   // The outgoing link is sending bytes (for a previous send()) until this time. Subsequent sends
    // JS:   // cannot start until after this time. This is how we simulate bandwidth constraints.
    // JS:   linkOccupiedUntil = 0;
    link_occupied_until: f64,
    // JS:
    // JS:   // The flow controller itself.
    // JS:   fc = new FlowController(() => this.t);
    fc: FlowController,
    // JS:
    // JS:   // In-flight writes, in send order. Each has its token and scheduled ack time.
    // JS:   inFlight: { token: SendToken, ackTime: number }[] = [];
    in_flight: VecDeque<(SendToken, f64)>,
    // JS:
}
impl StreamSimulator {
    fn new() -> Self {
        let t = Arc::new(Clock::new(0.0));
        let clock = t.clone();
        Self {
            rtt: 100.0,
            bandwidth: 10.0 * 1024.0,
            t,
            blocked: false,
            link_occupied_until: 0.0,
            fc: FlowController::new(move || clock.get()),
            in_flight: VecDeque::new(),
        }
    }

    fn bdp(&self) -> f64 {
        self.rtt * self.bandwidth
    }
    // JS:   // Send a chunk at the current time.
    // JS:   send(size: number) {
    fn send(&mut self, size: f64) {
        // JS:     // The new message begins sending now, unless some other message is still sending, in which
        // JS:     // case it sends after that message. It occupies the link until all the bytes are sent, based
        // JS:     // on the bandwidth.
        // JS:     this.linkOccupiedUntil = Math.max(this.linkOccupiedUntil, this.t) + size / this.bandwidth;
        self.link_occupied_until =
            self.link_occupied_until.max(self.t.get()) + size / self.bandwidth;
        // JS:
        // JS:     let { token, shouldBlock } = this.fc.onSend(size);
        let (token, should_block) = self.fc.on_send(size);
        // JS:
        // JS:     // ackTime = time when the chunk finishes writing out to the pipe, plus 1 rtt
        // JS:     this.inFlight.push({ token, ackTime: this.linkOccupiedUntil + this.rtt });
        self.in_flight
            .push_back((token, self.link_occupied_until + self.rtt));
        // JS:     this.blocked = shouldBlock;
        self.blocked = should_block;
        // JS:   }
    }
    // JS:
    // JS:   // Fill the window by sending chunks of the given size. Returns the number of chunks sent
    // JS:   // (the last one caused blocking).
    // JS:   fillWindow(chunkSize: number): number {
    fn fill_window(&mut self, chunk_size: f64) -> usize {
        // JS:     let count = 0;
        let mut count = 0;
        // JS:     while (!this.blocked) {
        while !self.blocked {
            // JS:       count++;
            count += 1;
            // JS:       this.send(chunkSize);
            self.send(chunk_size);
            // JS:     }
        }
        // JS:     return count;
        count
        // JS:   }
    }
    // JS:
    // JS:   // Advance to the next message's ack time, and deliver the ack to the flow controller.
    // JS:   waitForNextAck() {
    fn wait_for_next_ack(&mut self) {
        // JS:     let entry = this.inFlight.shift();
        let entry = self.in_flight.pop_front();
        // JS:     if (entry) {
        if let Some((token, ack_time)) = entry {
            // JS:       this.t = entry.ackTime;
            self.t.set(ack_time);
            // JS:       if (this.fc.onAck(entry.token)) {
            if self.fc.on_ack(token) {
                // JS:         this.blocked = false;
                self.blocked = false;
                // JS:       }
            }
            // JS:     }
        }
        // JS:   }
    }
    // JS:
    // JS:   // Simulate the application writing to the stream as fast as it can for the given duration.
    // JS:   saturateFor(duration: number): void {
    fn saturate_for(&mut self, duration: f64) {
        // JS:     let endTime = this.t + duration;
        let end_time = self.t.get() + duration;
        // JS:     let i = 0;
        let mut i = 0;
        // JS:
        // JS:     // Send chunks until blocked, then wait for acks until unblocked. Repeat until the end time
        // JS:     // is reached.
        // JS:     while (this.t < endTime) {
        while self.t.get() < end_time {
            // JS:       if (this.blocked) {
            if self.blocked {
                // JS:         this.waitForNextAck();
                self.wait_for_next_ack();
            // JS:       } else {
            } else {
                // JS:         this.send(CHUNK_SIZES[i++ % CHUNK_SIZES.length]);
                self.send(CHUNK_SIZES[i % CHUNK_SIZES.len()]);
                i += 1;
                // JS:       }
            }
            // JS:     }
        }
        // JS:
        // JS:     // Drain remaining acks.
        // JS:     while (this.inFlight.length > 0) {
        while !self.in_flight.is_empty() {
            // JS:       this.waitForNextAck();
            self.wait_for_next_ack();
            // JS:     }
        }
        // JS:   }
    }
    // JS: }
}
// JS:
// JS: describe("FlowController", () => {
mod flow_controller {
    use super::*;
    // JS:   it("blocks when window is full", () => {
    #[test]
    fn blocks_when_window_is_full() {
        // JS:     let sim = new StreamSimulator();
        let mut sim = StreamSimulator::new();
        // JS:     let initialWindow = sim.fc.window;  // 256KB
        let initial_window = sim.fc.window;
        // JS:
        // JS:     // Send chunks until blocked.
        // JS:     let count = sim.fillWindow(64 * 1024);
        let count = sim.fill_window(64.0 * 1024.0);
        // JS:     expect(count).toBe(4);  // 4 * 64KB = 256KB = window
        assert_eq!(count, 4);
        // JS:     expect(sim.fc.bytesInFlight).toBe(initialWindow);
        assert_eq!(sim.fc.bytes_in_flight, initial_window);
        // JS:   });
    }
    // JS:
    // JS:   it("unblocks after ack frees space", () => {
    #[test]
    fn unblocks_after_ack_frees_space() {
        // JS:     let sim = new StreamSimulator();
        let mut sim = StreamSimulator::new();
        // JS:
        // JS:     // Send 4 chunks at staggered times so acks arrive at different times.
        // JS:     sim.t = 0;
        sim.t.set(0.0);
        // JS:     sim.send(64 * 1024);
        sim.send(64.0 * 1024.0);
        // JS:     sim.t = 1;
        sim.t.set(1.0);
        // JS:     sim.send(64 * 1024);
        sim.send(64.0 * 1024.0);
        // JS:     sim.t = 2;
        sim.t.set(2.0);
        // JS:     sim.send(64 * 1024);
        sim.send(64.0 * 1024.0);
        // JS:     sim.t = 3;
        sim.t.set(3.0);
        // JS:     expect(sim.blocked).toBe(false);
        assert!(!sim.blocked);
        // JS:     sim.send(64 * 1024);
        sim.send(64.0 * 1024.0);
        // JS:     expect(sim.blocked).toBe(true);
        assert!(sim.blocked);
        // JS:
        // JS:     // Ack only the first one.
        // JS:     sim.waitForNextAck();
        sim.wait_for_next_ack();
        // JS:     expect(sim.fc.bytesInFlight).toBe(192 * 1024);  // 3 chunks still in flight
        assert_eq!(sim.fc.bytes_in_flight, 192.0 * 1024.0);
        // JS:   });
    }
    // JS:
    // JS:   it("window grows during startup", () => {
    #[test]
    fn window_grows_during_startup() {
        // JS:     let sim = new StreamSimulator();
        let mut sim = StreamSimulator::new();
        // JS:
        // JS:     let initialWindow = sim.fc.window;  // 256KB
        let initial_window = sim.fc.window;
        // JS:     // Simulate for a few RTTs — window should grow toward the BDP.
        // JS:     sim.saturateFor(sim.rtt * 5);
        sim.saturate_for(sim.rtt * 5.0);
        // JS:
        // JS:     // During startup, window should quickly grow past the 1MB BDP.
        // JS:     expect(sim.fc.window).toBeGreaterThan(initialWindow);
        assert!(sim.fc.window > initial_window);
        // JS:     expect(sim.fc.window).toBeGreaterThan(sim.bdp);
        assert!(sim.fc.window > sim.bdp());
        // JS:   });
    }
    // JS:
    // JS:   it("exits startup after window growth plateaus", () => {
    #[test]
    fn exits_startup_after_window_growth_plateaus() {
        // JS:     let sim = new StreamSimulator();
        let mut sim = StreamSimulator::new();
        // JS:
        // JS:     expect(sim.fc.inStartupPhase).toBe(true);
        assert!(sim.fc.in_startup_phase);
        // JS:
        // JS:     // Simulate for long enough that startup exits.
        // JS:     sim.saturateFor(sim.rtt * 50);
        sim.saturate_for(sim.rtt * 50.0);
        // JS:
        // JS:     expect(sim.fc.inStartupPhase).toBe(false);
        assert!(!sim.fc.in_startup_phase);
        // JS:   });
    }
    // JS:
    // JS:   it("steady-state window converges near BDP", () => {
    #[test]
    fn steady_state_window_converges_near_bdp() {
        // JS:     let sim = new StreamSimulator();
        let mut sim = StreamSimulator::new();
        // JS:
        // JS:     // Run through startup.
        // JS:     sim.saturateFor(sim.rtt * 50);
        sim.saturate_for(sim.rtt * 50.0);
        // JS:     expect(sim.fc.inStartupPhase).toBe(false);
        assert!(!sim.fc.in_startup_phase);
        // JS:
        // JS:     // Window should have settled around 25% above BDP.
        // JS:     expect(sim.fc.window).toBeGreaterThan(sim.bdp * 1.2);
        assert!(sim.fc.window > sim.bdp() * 1.2);
        // JS:     expect(sim.fc.window).toBeLessThan(sim.bdp * 1.3);
        assert!(sim.fc.window < sim.bdp() * 1.3);
        // JS:
        // JS:     // Keep running.
        // JS:     sim.saturateFor(sim.rtt * 20);
        sim.saturate_for(sim.rtt * 20.0);
        // JS:
        // JS:     // Window should be pretty stable.
        // JS:     expect(sim.fc.window).toBeGreaterThan(sim.bdp * 1.2);
        assert!(sim.fc.window > sim.bdp() * 1.2);
        // JS:     expect(sim.fc.window).toBeLessThan(sim.bdp * 1.3);
        assert!(sim.fc.window < sim.bdp() * 1.3);
        // JS:   });
    }
    // JS:
    // JS:   it("window does not shrink when app-limited", () => {
    #[test]
    fn window_does_not_shrink_when_app_limited() {
        // JS:     let sim = new StreamSimulator();
        let mut sim = StreamSimulator::new();
        // JS:
        // JS:     // Run through startup to establish a window.
        // JS:     sim.saturateFor(sim.rtt * 50);
        sim.saturate_for(sim.rtt * 50.0);
        // JS:     let windowAfterStartup = sim.fc.window;
        let window_after_startup = sim.fc.window;
        // JS:
        // JS:     // Now repeatedly send small chunks that don't fill the window, waiting for each one.
        // JS:     for (let i = 0; i < 50; i++) {
        for _ in 0..50 {
            // JS:       sim.send(1024);
            sim.send(1024.0);
            // JS:       sim.waitForNextAck();
            sim.wait_for_next_ack();
            // JS:     }
        }
        // JS:
        // JS:     // Window should not have shrunk.
        // JS:     expect(sim.fc.window).toStrictEqual(windowAfterStartup);
        assert_eq!(sim.fc.window, window_after_startup);
        // JS:   });
    }
    // JS:
    // JS:   it("window shrinks when pipe bandwidth decreases", () => {
    #[test]
    fn window_shrinks_when_pipe_bandwidth_decreases() {
        // JS:     let sim = new StreamSimulator();
        let mut sim = StreamSimulator::new();
        // JS:
        // JS:     // Run through startup at full speed — window should grow well above INITIAL_WINDOW.
        // JS:     sim.saturateFor(sim.rtt * 50);
        sim.saturate_for(sim.rtt * 50.0);
        // JS:     expect(sim.fc.inStartupPhase).toBe(false);
        assert!(!sim.fc.in_startup_phase);
        // JS:     expect(sim.fc.window).toBeGreaterThan(sim.bdp);
        assert!(sim.fc.window > sim.bdp());
        // JS:
        // JS:     // Simulate pipe bandwidth drop to 1/4 of original. The sender still tries to fill the
        // JS:     // window, but acks arrive more slowly (bottleneck delivers chunks at 1/4 the rate).
        // JS:     sim.bandwidth /= 4;
        sim.bandwidth /= 4.0;
        // JS:     sim.saturateFor(sim.rtt * 200);
        sim.saturate_for(sim.rtt * 200.0);
        // JS:
        // JS:     // The window should have decayed, and is now closer to the new, lower bdp.
        // JS:     expect(sim.fc.window).toBeLessThan(sim.bdp * 2);
        assert!(sim.fc.window < sim.bdp() * 2.0);
        // JS:   });
    }
    // JS:
    // JS:   it("onError restores bytesInFlight without changing window", () => {
    #[test]
    fn on_error_restores_bytes_in_flight_without_changing_window() {
        // JS:     let sim = new StreamSimulator();
        let mut sim = StreamSimulator::new();
        // JS:
        // JS:     let { token } = sim.fc.onSend(64 * 1024);
        let (token, _) = sim.fc.on_send(64.0 * 1024.0);
        // JS:     expect(sim.fc.bytesInFlight).toBe(64 * 1024);
        assert_eq!(sim.fc.bytes_in_flight, 64.0 * 1024.0);
        // JS:
        // JS:     let windowBefore = sim.fc.window;
        let window_before = sim.fc.window;
        // JS:     sim.fc.onError(token);
        sim.fc.on_error(token);
        // JS:     expect(sim.fc.bytesInFlight).toBe(0);
        assert_eq!(sim.fc.bytes_in_flight, 0.0);
        // JS:     expect(sim.fc.window).toBe(windowBefore);
        assert_eq!(sim.fc.window, window_before);
        // JS:   });
    }
    // JS:
    // JS:   it("growth collar limits window increase to growthFactor per RTT", () => {
    #[test]
    fn growth_collar_limits_window_increase_to_growth_factor_per_rtt() {
        // JS:     let sim = new StreamSimulator();
        let mut sim = StreamSimulator::new();
        // JS:     let chunkSize = 64 * 1024;
        let chunk_size = 64.0 * 1024.0;
        // JS:
        // JS:     // Do a first round to get past the first-ack bootstrap.
        // JS:     sim.fillWindow(chunkSize);
        sim.fill_window(chunk_size);
        // JS:     sim.waitForNextAck();
        sim.wait_for_next_ack();
        // JS:
        // JS:     // Record the initial window.
        // JS:     let windowBefore = sim.fc.window;
        let window_before = sim.fc.window;
        // JS:
        // JS:     // Do another round.
        // JS:     sim.fillWindow(chunkSize);
        sim.fill_window(chunk_size);
        // JS:     // Ack all of them.
        // JS:     while (sim.inFlight.length > 0) {
        while !sim.in_flight.is_empty() {
            // JS:       sim.waitForNextAck();
            sim.wait_for_next_ack();
            // JS:     }
        }
        // JS:
        // JS:     // In startup, window can at most double per RTT (STARTUP_GROWTH_FACTOR = 2).
        // JS:     expect(sim.fc.window).toBeLessThanOrEqual(windowBefore * 2 + 1);
        assert!(sim.fc.window <= window_before * 2.0 + 1.0);
        // JS:   });
    }
    // JS:
    // JS:   it("minimum window is enforced", () => {
    #[test]
    fn minimum_window_is_enforced() {
        // JS:     let sim = new StreamSimulator();
        let mut sim = StreamSimulator::new();
        // JS:
        // JS:     // Set RTT and bandwidth excessively low, to create a low BDP.
        // JS:     sim.rtt = 1;
        sim.rtt = 1.0;
        // JS:     sim.bandwidth = 1;
        sim.bandwidth = 1.0;
        // JS:
        // JS:     // We're going to have to run for quite a while since bandwidth is so low.
        // JS:     sim.saturateFor(10000000);
        sim.saturate_for(10_000_000.0);
        // JS:
        // JS:     // Window should have been clamped at MIN_WINDOW.
        // JS:     expect(sim.fc.window).toStrictEqual(64 * 1024);
        assert_eq!(sim.fc.window, 64.0 * 1024.0);
        // JS:   });
    }
    // JS: });
}
