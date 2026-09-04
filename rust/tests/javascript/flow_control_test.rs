// JS: // Copyright (c) 2026 Cloudflare, Inc.
// JS: // Licensed under the MIT license found in the LICENSE.txt file or at:
// JS: //     https://opensource.org/license/mit
// JS:
// JS: import { expect, it, describe } from "vitest"
// JS: import { FlowController, type SendToken } from "../src/streams.js"
// JS:
// JS: // To emulate random-ish chunk sizes while keeping the test reproducible, we cycle through this
// JS: // list of sizes.
// JS: const CHUNK_SIZES = [32 * 1024, 4 * 1024, 16000, 12345, 16, 9999, 4321, 8];
// JS:
// JS: // Helper: simulates a stream with a fake clock and set RTT and bandwidth. Sends chunks, advances
// JS: // time, and acks in order.
// JS: class StreamSimulator {
// JS:   // Default RTT of 100ms.
// JS:   rtt = 100;
// JS:
// JS:   // Default bandwidth of 10kB/ms = 10MB/s = 1MB/RTT. This is larger than the initial window size
// JS:   // of 256k, so the window should grow if staurated. Individual test cases may set something
// JS:   // different, or modify the properties during the test.
// JS:   bandwidth = 10 * 1024;
// JS:
// JS:   // Convenience getter to calculate BDP.
// JS:   get bdp() { return this.rtt * this.bandwidth; }
// JS:
// JS:   // Current simulated time.
// JS:   t = 0;
// JS:
// JS:   // Are we currently blocked, according to the flow controller's return values from onSend/onAck?
// JS:   blocked = false;
// JS:
// JS:   // The outgoing link is sending bytes (for a previous send()) until this time. Subsequent sends
// JS:   // cannot start until after this time. This is how we simulate bandwidth constraints.
// JS:   linkOccupiedUntil = 0;
// JS:
// JS:   // The flow controller itself.
// JS:   fc = new FlowController(() => this.t);
// JS:
// JS:   // In-flight writes, in send order. Each has its token and scheduled ack time.
// JS:   inFlight: { token: SendToken, ackTime: number }[] = [];
// JS:
// JS:   // Send a chunk at the current time.
// JS:   send(size: number) {
// JS:     // The new message begins sending now, unless some other message is still sending, in which
// JS:     // case it sends after that message. It occupies the link until all the bytes are sent, based
// JS:     // on the bandwidth.
// JS:     this.linkOccupiedUntil = Math.max(this.linkOccupiedUntil, this.t) + size / this.bandwidth;
// JS:
// JS:     let { token, shouldBlock } = this.fc.onSend(size);
// JS:
// JS:     // ackTime = time when the chunk finishes writing out to the pipe, plus 1 rtt
// JS:     this.inFlight.push({ token, ackTime: this.linkOccupiedUntil + this.rtt });
// JS:     this.blocked = shouldBlock;
// JS:   }
// JS:
// JS:   // Fill the window by sending chunks of the given size. Returns the number of chunks sent
// JS:   // (the last one caused blocking).
// JS:   fillWindow(chunkSize: number): number {
// JS:     let count = 0;
// JS:     while (!this.blocked) {
// JS:       count++;
// JS:       this.send(chunkSize);
// JS:     }
// JS:     return count;
// JS:   }
// JS:
// JS:   // Advance to the next message's ack time, and deliver the ack to the flow controller.
// JS:   waitForNextAck() {
// JS:     let entry = this.inFlight.shift();
// JS:     if (entry) {
// JS:       this.t = entry.ackTime;
// JS:       if (this.fc.onAck(entry.token)) {
// JS:         this.blocked = false;
// JS:       }
// JS:     }
// JS:   }
// JS:
// JS:   // Simulate the application writing to the stream as fast as it can for the given duration.
// JS:   saturateFor(duration: number): void {
// JS:     let endTime = this.t + duration;
// JS:     let i = 0;
// JS:
// JS:     // Send chunks until blocked, then wait for acks until unblocked. Repeat until the end time
// JS:     // is reached.
// JS:     while (this.t < endTime) {
// JS:       if (this.blocked) {
// JS:         this.waitForNextAck();
// JS:       } else {
// JS:         this.send(CHUNK_SIZES[i++ % CHUNK_SIZES.length]);
// JS:       }
// JS:     }
// JS:
// JS:     // Drain remaining acks.
// JS:     while (this.inFlight.length > 0) {
// JS:       this.waitForNextAck();
// JS:     }
// JS:   }
// JS: }
// JS:
// JS: describe("FlowController", () => {
// JS:   it("blocks when window is full", () => {
// JS:     let sim = new StreamSimulator();
// JS:     let initialWindow = sim.fc.window;  // 256KB
// JS:
// JS:     // Send chunks until blocked.
// JS:     let count = sim.fillWindow(64 * 1024);
// JS:     expect(count).toBe(4);  // 4 * 64KB = 256KB = window
// JS:     expect(sim.fc.bytesInFlight).toBe(initialWindow);
// JS:   });
// JS:
// JS:   it("unblocks after ack frees space", () => {
// JS:     let sim = new StreamSimulator();
// JS:
// JS:     // Send 4 chunks at staggered times so acks arrive at different times.
// JS:     sim.t = 0;
// JS:     sim.send(64 * 1024);
// JS:     sim.t = 1;
// JS:     sim.send(64 * 1024);
// JS:     sim.t = 2;
// JS:     sim.send(64 * 1024);
// JS:     sim.t = 3;
// JS:     expect(sim.blocked).toBe(false);
// JS:     sim.send(64 * 1024);
// JS:     expect(sim.blocked).toBe(true);
// JS:
// JS:     // Ack only the first one.
// JS:     sim.waitForNextAck();
// JS:     expect(sim.fc.bytesInFlight).toBe(192 * 1024);  // 3 chunks still in flight
// JS:   });
// JS:
// JS:   it("window grows during startup", () => {
// JS:     let sim = new StreamSimulator();
// JS:
// JS:     let initialWindow = sim.fc.window;  // 256KB
// JS:     // Simulate for a few RTTs — window should grow toward the BDP.
// JS:     sim.saturateFor(sim.rtt * 5);
// JS:
// JS:     // During startup, window should quickly grow past the 1MB BDP.
// JS:     expect(sim.fc.window).toBeGreaterThan(initialWindow);
// JS:     expect(sim.fc.window).toBeGreaterThan(sim.bdp);
// JS:   });
// JS:
// JS:   it("exits startup after window growth plateaus", () => {
// JS:     let sim = new StreamSimulator();
// JS:
// JS:     expect(sim.fc.inStartupPhase).toBe(true);
// JS:
// JS:     // Simulate for long enough that startup exits.
// JS:     sim.saturateFor(sim.rtt * 50);
// JS:
// JS:     expect(sim.fc.inStartupPhase).toBe(false);
// JS:   });
// JS:
// JS:   it("steady-state window converges near BDP", () => {
// JS:     let sim = new StreamSimulator();
// JS:
// JS:     // Run through startup.
// JS:     sim.saturateFor(sim.rtt * 50);
// JS:     expect(sim.fc.inStartupPhase).toBe(false);
// JS:
// JS:     // Window should have settled around 25% above BDP.
// JS:     expect(sim.fc.window).toBeGreaterThan(sim.bdp * 1.2);
// JS:     expect(sim.fc.window).toBeLessThan(sim.bdp * 1.3);
// JS:
// JS:     // Keep running.
// JS:     sim.saturateFor(sim.rtt * 20);
// JS:
// JS:     // Window should be pretty stable.
// JS:     expect(sim.fc.window).toBeGreaterThan(sim.bdp * 1.2);
// JS:     expect(sim.fc.window).toBeLessThan(sim.bdp * 1.3);
// JS:   });
// JS:
// JS:   it("window does not shrink when app-limited", () => {
// JS:     let sim = new StreamSimulator();
// JS:
// JS:     // Run through startup to establish a window.
// JS:     sim.saturateFor(sim.rtt * 50);
// JS:     let windowAfterStartup = sim.fc.window;
// JS:
// JS:     // Now repeatedly send small chunks that don't fill the window, waiting for each one.
// JS:     for (let i = 0; i < 50; i++) {
// JS:       sim.send(1024);
// JS:       sim.waitForNextAck();
// JS:     }
// JS:
// JS:     // Window should not have shrunk.
// JS:     expect(sim.fc.window).toStrictEqual(windowAfterStartup);
// JS:   });
// JS:
// JS:   it("window shrinks when pipe bandwidth decreases", () => {
// JS:     let sim = new StreamSimulator();
// JS:
// JS:     // Run through startup at full speed — window should grow well above INITIAL_WINDOW.
// JS:     sim.saturateFor(sim.rtt * 50);
// JS:     expect(sim.fc.inStartupPhase).toBe(false);
// JS:     expect(sim.fc.window).toBeGreaterThan(sim.bdp);
// JS:
// JS:     // Simulate pipe bandwidth drop to 1/4 of original. The sender still tries to fill the
// JS:     // window, but acks arrive more slowly (bottleneck delivers chunks at 1/4 the rate).
// JS:     sim.bandwidth /= 4;
// JS:     sim.saturateFor(sim.rtt * 200);
// JS:
// JS:     // The window should have decayed, and is now closer to the new, lower bdp.
// JS:     expect(sim.fc.window).toBeLessThan(sim.bdp * 2);
// JS:   });
// JS:
// JS:   it("onError restores bytesInFlight without changing window", () => {
// JS:     let sim = new StreamSimulator();
// JS:
// JS:     let { token } = sim.fc.onSend(64 * 1024);
// JS:     expect(sim.fc.bytesInFlight).toBe(64 * 1024);
// JS:
// JS:     let windowBefore = sim.fc.window;
// JS:     sim.fc.onError(token);
// JS:     expect(sim.fc.bytesInFlight).toBe(0);
// JS:     expect(sim.fc.window).toBe(windowBefore);
// JS:   });
// JS:
// JS:   it("growth collar limits window increase to growthFactor per RTT", () => {
// JS:     let sim = new StreamSimulator();
// JS:     let chunkSize = 64 * 1024;
// JS:
// JS:     // Do a first round to get past the first-ack bootstrap.
// JS:     sim.fillWindow(chunkSize);
// JS:     sim.waitForNextAck();
// JS:
// JS:     // Record the initial window.
// JS:     let windowBefore = sim.fc.window;
// JS:
// JS:     // Do another round.
// JS:     sim.fillWindow(chunkSize);
// JS:     // Ack all of them.
// JS:     while (sim.inFlight.length > 0) {
// JS:       sim.waitForNextAck();
// JS:     }
// JS:
// JS:     // In startup, window can at most double per RTT (STARTUP_GROWTH_FACTOR = 2).
// JS:     expect(sim.fc.window).toBeLessThanOrEqual(windowBefore * 2 + 1);
// JS:   });
// JS:
// JS:   it("minimum window is enforced", () => {
// JS:     let sim = new StreamSimulator();
// JS:
// JS:     // Set RTT and bandwidth excessively low, to create a low BDP.
// JS:     sim.rtt = 1;
// JS:     sim.bandwidth = 1;
// JS:
// JS:     // We're going to have to run for quite a while since bandwidth is so low.
// JS:     sim.saturateFor(10000000);
// JS:
// JS:     // Window should have been clamped at MIN_WINDOW.
// JS:     expect(sim.fc.window).toStrictEqual(64 * 1024);
// JS:   });
// JS: });
