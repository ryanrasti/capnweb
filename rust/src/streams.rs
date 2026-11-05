// JS: // Copyright (c) 2026 Cloudflare, Inc.
// JS: // Licensed under the MIT license found in the LICENSE.txt file or at:
// JS: //     https://opensource.org/license/mit
// JS:
// JS: import {
// JS:   StubHook, RpcPayload, PropertyPath, ErrorStubHook, PayloadStubHook, PromiseStubHook, streamImpl
// JS: } from "./core.js";
// JS:
// JS: // =======================================================================================
// JS: // WritableStreamStubHook - wraps a local WritableStream for export
// JS:
// JS: // Many WritableStreamStubHooks could point at the same WritableStream. We store a refcount in a
// JS: // separate object that they all share.
// JS: type BoxedWriterState = {
// JS:   refcount: number;
// JS:   writer: WritableStreamDefaultWriter;
// JS:   closed: boolean;
// JS: };
// JS:
// JS: class WritableStreamStubHook extends StubHook {
// JS:   private state?: BoxedWriterState;  // undefined when disposed
// JS:
// JS:   // Creates a new WritableStreamStubHook that is not duplicated from an existing hook.
// JS:   static create(stream: WritableStream): WritableStreamStubHook {
// JS:     let writer = stream.getWriter();  // Locks the stream
// JS:     return new WritableStreamStubHook({ refcount: 1, writer, closed: false });
// JS:   }
// JS:
// JS:   private constructor(state: BoxedWriterState, dupFrom?: WritableStreamStubHook) {
// JS:     super();
// JS:     this.state = state;
// JS:     if (dupFrom) {
// JS:       ++state.refcount;
// JS:     }
// JS:   }
// JS:
// JS:   private getState(): BoxedWriterState {
// JS:     if (this.state) {
// JS:       return this.state;
// JS:     } else {
// JS:       throw new Error("Attempted to use a WritableStreamStubHook after it was disposed.");
// JS:     }
// JS:   }
// JS:
// JS:   call(path: PropertyPath, args: RpcPayload): StubHook {
// JS:     try {
// JS:       let state = this.getState();
// JS:
// JS:       if (path.length !== 1 || typeof path[0] !== "string") {
// JS:         throw new Error("WritableStream stub only supports direct method calls");
// JS:       }
// JS:
// JS:       const method = path[0];
// JS:
// JS:       if (method !== "write" && method !== "close" && method !== "abort") {
// JS:         throw new Error(`Unknown WritableStream method: ${method}`);
// JS:       }
// JS:
// JS:       // Mark as closed if close() or abort() is called.
// JS:       if (method === "close" || method === "abort") {
// JS:         state.closed = true;
// JS:       }
// JS:
// JS:       // write(chunk) delivers asynchronously: stubs nested in the chunk must outlive
// JS:       // writer.write() returning. See RpcPayload.deliverStreamWrite().
// JS:       let promise = method === "write"
// JS:           ? args.deliverStreamWrite(state.writer)
// JS:           : args.deliverCall(state.writer[method] as Function, state.writer);
// JS:       return new PromiseStubHook(promise.then(payload => new PayloadStubHook(payload)));
// JS:     } catch (err) {
// JS:       // We took ownership of `args`, and there is no callee left to consume them.
// JS:       args.dispose();
// JS:       return new ErrorStubHook(err);
// JS:     }
// JS:   }
// JS:
// JS:   map(path: PropertyPath, captures: StubHook[], instructions: unknown[]): StubHook {
// JS:     // WritableStreams don't support map operations.
// JS:     for (let cap of captures) {
// JS:       cap.dispose();
// JS:     }
// JS:     return new ErrorStubHook(new Error("Cannot use map() on a WritableStream"));
// JS:   }
// JS:
// JS:   get(path: PropertyPath): StubHook {
// JS:     // WritableStreams don't expose properties over RPC.
// JS:     return new ErrorStubHook(new Error("Cannot access properties on a WritableStream stub"));
// JS:   }
// JS:
// JS:   dup(): StubHook {
// JS:     let state = this.getState();
// JS:     return new WritableStreamStubHook(state, this);
// JS:   }
// JS:
// JS:   pull(): RpcPayload | Promise<RpcPayload> {
// JS:     // WritableStreams can't be pulled - they're not promises.
// JS:     return Promise.reject(new Error("Cannot pull a WritableStream stub"));
// JS:   }
// JS:
// JS:   ignoreUnhandledRejections(): void {
// JS:     // Nothing to do.
// JS:   }
// JS:
// JS:   dispose(): void {
// JS:     let state = this.state;
// JS:     this.state = undefined;
// JS:     if (state) {
// JS:       if (--state.refcount === 0) {
// JS:         if (!state.closed) {
// JS:           // Abort the stream if not cleanly closed.
// JS:           state.writer.abort(new Error("WritableStream RPC stub was disposed without calling close()"))
// JS:               .catch(() => {});  // Ignore errors from abort.
// JS:         }
// JS:         state.writer.releaseLock();
// JS:       }
// JS:     }
// JS:   }
// JS:
// JS:   onBroken(callback: (error: any) => void): void {
// JS:     // WritableStream stubs don't really have a "broken" state in the same way.
// JS:     // The caller would notice when write/close/abort fails.
// JS:   }
// JS: }
// JS:
// JS: // =======================================================================================
// JS: // FlowController - BDP-based dynamic flow control for stream writes
// JS: //
// JS: // Estimates the bandwidth-delay product (BDP) of a stream by observing write sends and acks,
// JS: // and dynamically adjusts the window size to match. The window is set to the estimated BDP
// JS: // multiplied by a growth factor, so that the sender always pushes slightly more than the
// JS: // estimated capacity — naturally probing for increased bandwidth.
// JS: //
// JS: // The algorithm works in two phases:
// JS: // - Startup: The window is allowed to double each RTT (STARTUP_GROWTH_FACTOR = 2), enabling
// JS: //   rapid discovery of available bandwidth. Startup ends when the window stops growing
// JS: //   meaningfully for STARTUP_EXIT_ROUNDS consecutive RTT rounds.
// JS: // - Steady state: The window grows by at most STEADY_GROWTH_FACTOR (1.25) per RTT and
// JS: //   shrinks by at most DECAY_FACTOR (0.90) per RTT, providing stability.
// JS:
// JS: // Flow control constants — tunable.
// JS: //
// JS: // Initial window size in bytes. Used before we have any bandwidth estimate.
// JS: const INITIAL_WINDOW = 256 * 1024;
const INITIAL_WINDOW: f64 = 256.0 * 1024.0;
// JS: // Maximum window size in bytes.
// JS: const MAX_WINDOW = 1024 * 1024 * 1024;
const MAX_WINDOW: f64 = 1024.0 * 1024.0 * 1024.0;
// JS: // Minimum window size in bytes.
// JS: const MIN_WINDOW = 64 * 1024;
const MIN_WINDOW: f64 = 64.0 * 1024.0;
// JS: // During startup, we allow the window to grow by up to this factor per RTT.
// JS: const STARTUP_GROWTH_FACTOR = 2;
const STARTUP_GROWTH_FACTOR: f64 = 2.0;
// JS: // In steady state, we allow the window to grow by up to this factor per RTT.
// JS: const STEADY_GROWTH_FACTOR = 1.25;
const STEADY_GROWTH_FACTOR: f64 = 1.25;
// JS: // Allowed reduction in window size per RTT.
// JS: const DECAY_FACTOR = 0.90;
const DECAY_FACTOR: f64 = 0.90;
// JS: // Number of consecutive non-increasing ack rounds before exiting startup.
// JS: const STARTUP_EXIT_ROUNDS = 3;
const STARTUP_EXIT_ROUNDS: u32 = 3;
// JS:
// JS: // Opaque token returned by onSend() that must be passed back to onAck(). Carries the
// JS: // send-time snapshot needed to compute delivery rate and apply the window collar.
// JS: export type SendToken = {
#[derive(Clone, Copy)]
pub struct SendToken {
    // JS:   sentTime: number;
    sent_time: f64,
    // JS:   size: number;
    size: f64,
    // JS:   deliveredAtSend: number;
    delivered_at_send: f64,
    // JS:   deliveredTimeAtSend: number;
    delivered_time_at_send: f64,
    // JS:   windowAtSend: number;
    window_at_send: f64,
    // JS:   windowFullAtSend: boolean;
    window_full_at_send: bool,
    // JS: };
}
// JS:
// JS: // Exported for testing purposes only -- otherwise this is only used internally by
// JS: // createWritableStreamFromHook().
// JS: export class FlowController {
pub struct FlowController {
    // JS:   // The current window size in bytes. The sender blocks when bytesInFlight >= window.
    // JS:   window = INITIAL_WINDOW;
    pub window: f64,
    // JS:
    // JS:   // Total bytes currently in flight (sent but not yet acked).
    // JS:   bytesInFlight = 0;
    pub bytes_in_flight: f64,
    // JS:
    // JS:   // Whether we're still in the startup phase.
    // JS:   inStartupPhase = true;
    pub in_startup_phase: bool,
    // JS:
    // JS:   // ----- BDP estimation state (private) -----
    // JS:
    // JS:   // Total bytes acked so far.
    // JS:   private delivered = 0;
    delivered: f64,
    // JS:   // Time of most recent ack.
    // JS:   private deliveredTime = 0;
    delivered_time: f64,
    // JS:   // Time when the very first ack was received.
    // JS:   private firstAckTime = 0;
    first_ack_time: f64,
    // JS:   private firstAckDelivered = 0;
    first_ack_delivered: f64,
    // JS:   // Global minimum RTT observed (milliseconds).
    // JS:   private minRtt = Infinity;
    min_rtt: f64,
    // JS:
    // JS:   // For startup exit: count of consecutive RTT rounds where the window didn't meaningfully grow.
    // JS:   private roundsWithoutIncrease = 0;
    rounds_without_increase: u32,
    // JS:   // Window size at the start of the current round, for startup exit detection.
    // JS:   private lastRoundWindow = 0;
    last_round_window: f64,
    // JS:   // Time when the current round started.
    // JS:   private roundStartTime = 0;
    round_start_time: f64,
    now: Box<dyn Fn() -> f64 + Send>,
    // JS:
    // JS:   constructor(private now: () => number) {}
}
impl FlowController {
    pub fn new(now: impl Fn() -> f64 + Send + 'static) -> Self {
        Self {
            window: INITIAL_WINDOW,
            bytes_in_flight: 0.0,
            in_startup_phase: true,
            delivered: 0.0,
            delivered_time: 0.0,
            first_ack_time: 0.0,
            first_ack_delivered: 0.0,
            min_rtt: f64::INFINITY,
            rounds_without_increase: 0,
            last_round_window: 0.0,
            round_start_time: 0.0,
            now: Box::new(now),
        }
    }
    // JS:
    // JS:   // Called when a write of `size` bytes is about to be sent. Returns a token that must be
    // JS:   // passed to onAck() when the ack arrives, and whether the sender should block (window full).
    // JS:   onSend(size: number): { token: SendToken, shouldBlock: boolean } {
    pub fn on_send(&mut self, size: f64) -> (SendToken, bool) {
        // JS:     this.bytesInFlight += size;
        self.bytes_in_flight += size;
        // JS:
        // JS:     let token: SendToken = {
        let token = SendToken {
            // JS:       sentTime: this.now(),
            sent_time: (self.now)(),
            // JS:       size,
            size,
            // JS:       deliveredAtSend: this.delivered,
            delivered_at_send: self.delivered,
            // JS:       deliveredTimeAtSend: this.deliveredTime,
            delivered_time_at_send: self.delivered_time,
            // JS:       windowAtSend: this.window,
            window_at_send: self.window,
            // JS:       windowFullAtSend: this.bytesInFlight >= this.window,
            window_full_at_send: self.bytes_in_flight >= self.window,
            // JS:     };
        };
        // JS:
        // JS:     return { token, shouldBlock: token.windowFullAtSend };
        (token, token.window_full_at_send)
        // JS:   }
    }
    // JS:
    // JS:   // Called when a previously-sent write fails. Restores bytesInFlight without updating
    // JS:   // any BDP estimates.
    // JS:   onError(token: SendToken): void {
    pub fn on_error(&mut self, token: SendToken) {
        // JS:     this.bytesInFlight -= token.size;
        self.bytes_in_flight -= token.size;
        // JS:   }
    }
    // JS:
    // JS:   // Called when an ack is received for a previously-sent write. Updates BDP estimates and
    // JS:   // the window. Returns whether a blocked sender should now unblock.
    // JS:   onAck(token: SendToken): boolean {
    pub fn on_ack(&mut self, token: SendToken) -> bool {
        // JS:     let ackTime = this.now();
        let ack_time = (self.now)();
        // JS:
        // JS:     // Update delivery tracking metrics.
        // JS:     this.delivered += token.size;
        self.delivered += token.size;
        // JS:     this.deliveredTime = ackTime;
        self.delivered_time = ack_time;
        // JS:     this.bytesInFlight -= token.size;
        self.bytes_in_flight -= token.size;
        // JS:
        // JS:     // Update RTT estimate.
        // JS:     let rtt = ackTime - token.sentTime;
        let rtt = ack_time - token.sent_time;
        // JS:     this.minRtt = Math.min(this.minRtt, rtt);
        self.min_rtt = self.min_rtt.min(rtt);
        // JS:
        // JS:     // Update bandwidth estimate and window.
        // JS:     if (this.firstAckTime === 0) {
        if self.first_ack_time == 0.0 {
            // JS:       // This is the very first ack. We can't estimate bandwidth yet since we need to look
            // JS:       // at the interval between acks.
            // JS:       this.firstAckTime = ackTime;
            self.first_ack_time = ack_time;
            // JS:       this.firstAckDelivered = this.delivered;
            self.first_ack_delivered = self.delivered;
        // JS:     } else {
        } else {
            // JS:       let baseTime;
            let base_time;
            // JS:       let baseDelivered;
            let base_delivered;
            // JS:
            // JS:       if (token.deliveredTimeAtSend === 0) {
            if token.delivered_time_at_send == 0.0 {
                // JS:         // This write was sent before any acks had been received, but wasn't the very first
                // JS:         // write. We can estimate bandwidth starting from the first ack.
                // JS:         baseTime = this.firstAckTime;
                base_time = self.first_ack_time;
                // JS:         baseDelivered = this.firstAckDelivered;
                base_delivered = self.first_ack_delivered;
            // JS:       } else {
            } else {
                // JS:         baseTime = token.deliveredTimeAtSend;
                base_time = token.delivered_time_at_send;
                // JS:         baseDelivered = token.deliveredAtSend;
                base_delivered = token.delivered_at_send;
                // JS:       }
            }
            // JS:
            // JS:       let interval = ackTime - baseTime;
            let interval = ack_time - base_time;
            // JS:       let bytes = this.delivered - baseDelivered;
            let bytes = self.delivered - base_delivered;
            // JS:       let bandwidth = bytes / interval;
            let bandwidth = bytes / interval;
            // JS:
            // JS:       // Choose our target growth factor depending on whether we're at startup or steady
            // JS:       // state.
            // JS:       let growthFactor = this.inStartupPhase ? STARTUP_GROWTH_FACTOR : STEADY_GROWTH_FACTOR;
            let growth_factor = if self.in_startup_phase {
                STARTUP_GROWTH_FACTOR
            } else {
                STEADY_GROWTH_FACTOR
            };
            // JS:
            // JS:       // Calculate new window to be our calculated bandwidth-delay product, plus a growth
            // JS:       // factor to account for the possibility that bandwidth is constrained only due to
            // JS:       // the window having been too small.
            // JS:       let newWindow = bandwidth * this.minRtt * growthFactor;
            let mut new_window = bandwidth * self.min_rtt * growth_factor;
            // JS:
            // JS:       // Don't allow the window to grow too quickly -- it can only grow by at most
            // JS:       // `growthFactor` for each RTT.
            // JS:       newWindow = Math.min(newWindow, token.windowAtSend * growthFactor);
            new_window = new_window.min(token.window_at_send * growth_factor);
            // JS:
            // JS:       if (token.windowFullAtSend) {
            if token.window_full_at_send {
                // JS:         // Don't allow the window to shrink too quickly.
                // JS:         newWindow = Math.max(newWindow, token.windowAtSend * DECAY_FACTOR);
                new_window = new_window.max(token.window_at_send * DECAY_FACTOR);
            // JS:       } else {
            } else {
                // JS:         // Don't allow the window to shrink at all if we weren't saturating it -- in this
                // JS:         // case the sending app is not fully utilizing the connection, so no backpressure is
                // JS:         // needed. We clamp to this.window here, not this.windowAtSend, since we don't want to
                // JS:         // undo previous shrinkage, when alternating between sends that saturated and ones that
                // JS:         // didn't.
                // JS:         newWindow = Math.max(newWindow, this.window);
                new_window = new_window.max(self.window);
                // JS:       }
            }
            // JS:
            // JS:       // Clamp to min/max values.
            // JS:       this.window = Math.max(Math.min(newWindow, MAX_WINDOW), MIN_WINDOW);
            self.window = new_window.min(MAX_WINDOW).max(MIN_WINDOW);
            // JS:
            // JS:       // Check if the startup phase is done.
            // JS:       if (this.inStartupPhase && token.sentTime >= this.roundStartTime) {
            if self.in_startup_phase && token.sent_time >= self.round_start_time {
                // JS:         if (this.window > this.lastRoundWindow * STEADY_GROWTH_FACTOR) {
                if self.window > self.last_round_window * STEADY_GROWTH_FACTOR {
                    // JS:           // Saw a significant increase this round, so reset the counter.
                    // JS:           this.roundsWithoutIncrease = 0;
                    self.rounds_without_increase = 0;
                // JS:         } else {
                } else {
                    // JS:           // Window size didn't increase enough this round.
                    // JS:           if (++this.roundsWithoutIncrease >= STARTUP_EXIT_ROUNDS) {
                    self.rounds_without_increase += 1;
                    if self.rounds_without_increase >= STARTUP_EXIT_ROUNDS {
                        // JS:             // After three rounds with insufficient increase, exit startup mode.
                        // JS:             this.inStartupPhase = false;
                        self.in_startup_phase = false;
                        // JS:           }
                    }
                    // JS:         }
                }
                // JS:
                // JS:         // Advance to next round.
                // JS:         this.roundStartTime = ackTime;
                self.round_start_time = ack_time;
                // JS:         this.lastRoundWindow = this.window;
                self.last_round_window = self.window;
                // JS:       }
            }
            // JS:     }
        }
        // JS:
        // JS:     return this.bytesInFlight < this.window;
        self.bytes_in_flight < self.window
        // JS:   }
    }
    // JS: }
}
// JS:
// JS: // =======================================================================================
// JS: // createWritableStreamFromHook - creates a proxy WritableStream that forwards to a remote hook
// JS:
// JS: function createWritableStreamFromHook(hook: StubHook): WritableStream {
// JS:   let pendingError: any = undefined;
// JS:   let hookDisposed = false;
// JS:
// JS:   let fc = new FlowController(() => performance.now());
// JS:
// JS:   // If a previous write blocked waiting for the window to open, this resolver will unblock it.
// JS:   let windowResolve: (() => void) | undefined;
// JS:   let windowReject: ((e: unknown) => void) | undefined;
// JS:
// JS:   const disposeHook = () => {
// JS:     if (!hookDisposed) {
// JS:       hookDisposed = true;
// JS:       hook.dispose();
// JS:     }
// JS:   };
// JS:
// JS:   return new WritableStream({
// JS:     write(chunk, controller) {
// JS:       // If we already have an error, fail immediately.
// JS:       if (pendingError !== undefined) {
// JS:         throw pendingError;
// JS:       }
// JS:
// JS:       const payload = RpcPayload.fromAppParams([chunk]);
// JS:       const { promise, size } = hook.stream(["write"], payload);
// JS:
// JS:       if (size === undefined) {
// JS:         // Local call — await the promise directly to serialize writes (no overlapping).
// JS:         // We still need to detect errors to set pendingError.
// JS:         return promise.catch((err) => {
// JS:           if (pendingError === undefined) {
// JS:             pendingError = err;
// JS:           }
// JS:           throw err;
// JS:         });
// JS:       } else {
// JS:         // Remote call — use window-based flow control.
// JS:         let { token, shouldBlock } = fc.onSend(size);
// JS:
// JS:         // When the response comes back, update the window size based on BDP estimates.
// JS:         promise.then(() => {
// JS:           let hasCapacity = fc.onAck(token);
// JS:
// JS:           if (hasCapacity && windowResolve) {
// JS:             windowResolve();
// JS:             windowResolve = undefined;
// JS:             windowReject = undefined;
// JS:           }
// JS:         }, (err) => {
// JS:           fc.onError(token);
// JS:           if (pendingError === undefined) {
// JS:             pendingError = err;
// JS:             controller.error(err);
// JS:             disposeHook();
// JS:           }
// JS:           // Unblock any write waiting on backpressure -- reject it so the
// JS:           // stream finishes erroring instead of hanging forever.
// JS:           if (windowReject) {
// JS:             windowReject(err);
// JS:             windowResolve = undefined;
// JS:             windowReject = undefined;
// JS:           }
// JS:         });
// JS:
// JS:         // If we've filled (or exceeded) the window, block until acks free up space.
// JS:         if (shouldBlock) {
// JS:           return new Promise<void>((resolve, reject) => {
// JS:             windowResolve = resolve;
// JS:             windowReject = reject;
// JS:           });
// JS:         }
// JS:       }
// JS:     },
// JS:
// JS:     async close() {
// JS:       if (pendingError !== undefined) {
// JS:         disposeHook();
// JS:         throw pendingError;
// JS:       }
// JS:
// JS:       // Send close(). Per the RPC protocol, if any previous write failed, close() will also
// JS:       // fail with that error -- so there's no need to await pending writes first.
// JS:       const { promise } = hook.stream(["close"], RpcPayload.fromAppParams([]));
// JS:
// JS:       try {
// JS:         await promise;
// JS:       } catch (err) {
// JS:         // If a write error was detected (possibly while we were waiting for close()), prefer
// JS:         // throwing that, since the close error is likely just a consequence (e.g. "can't close
// JS:         // errored stream").
// JS:         throw pendingError ?? err;
// JS:       } finally {
// JS:         disposeHook();
// JS:       }
// JS:     },
// JS:
// JS:     abort(reason) {
// JS:       if (pendingError !== undefined) {
// JS:         return;
// JS:       }
// JS:
// JS:       pendingError = reason ?? new Error("WritableStream was aborted");
// JS:       if (windowReject) {
// JS:         windowReject(pendingError);
// JS:         windowResolve = undefined;
// JS:         windowReject = undefined;
// JS:       }
// JS:
// JS:       const { promise } = hook.stream(["abort"], RpcPayload.fromAppParams([reason]));
// JS:       promise.then(() => disposeHook(), () => disposeHook());
// JS:     }
// JS:   });
// JS: }
// JS:
// JS: // =======================================================================================
// JS: // ReadableStreamStubHook - wraps a local ReadableStream for disposal tracking
// JS: //
// JS: // This hook exists solely to live in RpcPayload.hooks so that the ReadableStream is properly
// JS: // disposed (canceled) when the payload is disposed. It does not handle any RPC operations --
// JS: // the actual data transfer is handled by pumping the stream into a pipe's WritableStream via
// JS: // pipeTo(). All methods other than dispose(), dup(), and ignoreUnhandledRejections() throw errors.
// JS:
// JS: // Many ReadableStreamStubHooks could point at the same ReadableStream. We store a refcount in a
// JS: // separate object that they all share.
// JS: type BoxedReadableState = {
// JS:   refcount: number;
// JS:   stream: ReadableStream;
// JS:   canceled: boolean;
// JS: };
// JS:
// JS: class ReadableStreamStubHook extends StubHook {
// JS:   private state?: BoxedReadableState;  // undefined when disposed
// JS:
// JS:   // Creates a new ReadableStreamStubHook.
// JS:   static create(stream: ReadableStream): ReadableStreamStubHook {
// JS:     return new ReadableStreamStubHook({ refcount: 1, stream, canceled: false });
// JS:   }
// JS:
// JS:   private constructor(state: BoxedReadableState, dupFrom?: ReadableStreamStubHook) {
// JS:     super();
// JS:     this.state = state;
// JS:     if (dupFrom) {
// JS:       ++state.refcount;
// JS:     }
// JS:   }
// JS:
// JS:   call(path: PropertyPath, args: RpcPayload): StubHook {
// JS:     args.dispose();
// JS:     return new ErrorStubHook(new Error("Cannot call methods on a ReadableStream stub"));
// JS:   }
// JS:
// JS:   map(path: PropertyPath, captures: StubHook[], instructions: unknown[]): StubHook {
// JS:     for (let cap of captures) {
// JS:       cap.dispose();
// JS:     }
// JS:     return new ErrorStubHook(new Error("Cannot use map() on a ReadableStream"));
// JS:   }
// JS:
// JS:   get(path: PropertyPath): StubHook {
// JS:     return new ErrorStubHook(new Error("Cannot access properties on a ReadableStream stub"));
// JS:   }
// JS:
// JS:   dup(): StubHook {
// JS:     let state = this.state;
// JS:     if (!state) {
// JS:       throw new Error("Attempted to dup a ReadableStreamStubHook after it was disposed.");
// JS:     }
// JS:     return new ReadableStreamStubHook(state, this);
// JS:   }
// JS:
// JS:   pull(): RpcPayload | Promise<RpcPayload> {
// JS:     return Promise.reject(new Error("Cannot pull a ReadableStream stub"));
// JS:   }
// JS:
// JS:   ignoreUnhandledRejections(): void {
// JS:     // Nothing to do.
// JS:   }
// JS:
// JS:   dispose(): void {
// JS:     let state = this.state;
// JS:     this.state = undefined;
// JS:     if (state) {
// JS:       if (--state.refcount === 0) {
// JS:         if (!state.canceled) {
// JS:           state.canceled = true;
// JS:
// JS:           // Don't try to cancel the stream if it's locked. It won't work anyway -- it'll throw
// JS:           // an exception, which we'd ignore anyway.
// JS:           //
// JS:           // This is a little janky but it makes some sense: If someone has locked the stream, they
// JS:           // have taken responsibility for fully reading it. The only reason we really need to
// JS:           // cancel when this hook is disposed is to handle the case where an application receives
// JS:           // a ReadableStream but completely ignores it -- we want it to be canceled naturally when
// JS:           // the payload is disposed.
// JS:           if (!state.stream.locked) {
// JS:             state.stream.cancel(
// JS:                 new Error("ReadableStream RPC stub was disposed without being consumed"))
// JS:                 .catch(() => {});  // Ignore errors from cancel.
// JS:           }
// JS:         }
// JS:       }
// JS:     }
// JS:   }
// JS:
// JS:   onBroken(callback: (error: any) => void): void {
// JS:     // ReadableStream stubs don't have a "broken" state.
// JS:   }
// JS: }
// JS:
// JS: // =======================================================================================
// JS: // Install the implementations into streamImpl
// JS:
// JS: streamImpl.createWritableStreamHook = WritableStreamStubHook.create;
// JS: streamImpl.createWritableStreamFromHook = createWritableStreamFromHook;
// JS: streamImpl.createReadableStreamHook = ReadableStreamStubHook.create;
// JS:
// JS: export function forceInitStreams() {}

// RUST-DIFF(byte streams): JavaScript passes any ReadableStream or WritableStream, whose chunks are
// any RPC value. Rust passes byte streams: a `ReadableStream` is an `AsyncRead` source whose chunks
// are sent as `Uint8Array`s, and a `WritableStream` an `AsyncWrite` sink that takes `Uint8Array`
// chunks. Their wire format is JavaScript's: a ReadableStream is `["readable", importId]` over a
// pipe the sender creates (`["pipe"]`) and pumps with `["stream"]` write calls under the flow
// controller above; a WritableStream is `["writable", exportId]`, an exported hook taking
// `write`, `close` and `abort` calls.

use std::pin::Pin;
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};

use crate::core::{
    CapnwebError, CapnwebValue, Error, MapInstructions, MaybePromise, PayloadStubHook,
    PromiseStubHook, PropertyKey, PropertyPath, Result, RpcPayload, StubHook, StubHookInner,
};

/// A byte source a [`ReadableStream`] sends.
pub type ByteSource = Pin<Box<dyn AsyncRead + Send>>;
/// A byte sink a [`WritableStream`] writes.
pub type ByteSink = Pin<Box<dyn AsyncWrite + Send>>;

/// The most a pumped ReadableStream sends per write call.
const PIPE_CHUNK: usize = 64 * 1024;

/// A byte stream sent to the peer as a JavaScript `ReadableStream` (its chunks `Uint8Array`s).
///
/// Single use: clones share it, and the first serialization takes the source.
#[derive(Clone)]
pub struct ReadableStream(Arc<Mutex<Option<ByteSource>>>);

impl ReadableStream {
    /// Wraps `source`, read until its end once the stream is sent.
    pub fn new(source: impl AsyncRead + Send + 'static) -> Self {
        Self(Arc::new(Mutex::new(Some(Box::pin(source)))))
    }

    pub(crate) fn take(&self) -> Result<ByteSource> {
        self.0
            .lock()
            .expect("ReadableStream mutex poisoned")
            .take()
            .ok_or_else(|| Error::Message("ReadableStream was already sent".to_owned()))
    }
}

impl std::fmt::Debug for ReadableStream {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ReadableStream")
    }
}

/// A byte sink the peer receives as a JavaScript `WritableStream` (taking `Uint8Array` chunks).
///
/// Single use: clones share it, and the first serialization takes the sink.
#[derive(Clone)]
pub struct WritableStream(Arc<Mutex<Option<ByteSink>>>);

impl WritableStream {
    /// Wraps `sink`: the peer's writes are written to it in order, its close shuts it down, and its
    /// abort (or the stub's release without a close) drops it.
    pub fn new(sink: impl AsyncWrite + Send + 'static) -> Self {
        Self(Arc::new(Mutex::new(Some(Box::pin(sink)))))
    }

    pub(crate) fn take(&self) -> Result<ByteSink> {
        self.0
            .lock()
            .expect("WritableStream mutex poisoned")
            .take()
            .ok_or_else(|| Error::Message("WritableStream was already sent".to_owned()))
    }
}

impl std::fmt::Debug for WritableStream {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("WritableStream")
    }
}

/// A call on an exported WritableStream, queued for its writer in the order it arrived.
enum WriterOp {
    Write(RpcPayload, oneshot::Sender<Result<()>>),
    Close(oneshot::Sender<Result<()>>),
    Abort(oneshot::Sender<Result<()>>),
}

/// An exported WritableStream: `write`, `close` and `abort` calls, taken in order by one writer task
/// that owns the sink (as JavaScript's writer queues them).
pub(crate) struct WritableStreamStubHook {
    ops: mpsc::UnboundedSender<WriterOp>,
}

impl WritableStreamStubHook {
    pub(crate) fn create(sink: ByteSink) -> StubHook {
        let (ops, queue) = mpsc::unbounded_channel();
        tokio::spawn(write_queue(sink, queue));
        Arc::new(Self { ops })
    }
}

/// The writer: each write's chunk written whole before the next; a failed write fails what follows;
/// the queue's end without a close (the hook released) drops the sink, as JavaScript's dispose aborts.
async fn write_queue(sink: ByteSink, mut queue: mpsc::UnboundedReceiver<WriterOp>) {
    let mut sink = Some(sink);
    let mut failed: Option<Error> = None;
    while let Some(op) = queue.recv().await {
        let (result, reply) = match op {
            WriterOp::Write(args, reply) => {
                (write_chunk(&mut sink, &mut failed, args).await, reply)
            }
            WriterOp::Close(reply) => {
                let result = match (failed.clone(), sink.take()) {
                    (Some(error), _) => Err(error),
                    (None, Some(mut sink)) => sink.shutdown().await.map_err(io_error),
                    (None, None) => Err(Error::Message("WritableStream is closed".to_owned())),
                };
                (result, reply)
            }
            WriterOp::Abort(reply) => {
                sink = None;
                failed
                    .get_or_insert_with(|| Error::Message("WritableStream was aborted".to_owned()));
                (Ok(()), reply)
            }
        };
        let _ = reply.send(result);
    }
}

async fn write_chunk(
    sink: &mut Option<ByteSink>,
    failed: &mut Option<Error>,
    args: RpcPayload,
) -> Result<()> {
    if let Some(error) = failed {
        return Err(error.clone());
    }
    let chunk = match args.deliver_resolve().await? {
        CapnwebValue::Array(mut values) if values.len() == 1 => match values.remove(0) {
            CapnwebValue::Bytes(bytes) => bytes,
            other => {
                return Err(Error::Type(format!(
                    "WritableStream chunk is not bytes: {other:?}"
                )));
            }
        },
        other => {
            return Err(Error::Type(format!(
                "WritableStream write takes one chunk, got {other:?}"
            )));
        }
    };
    let Some(writer) = sink.as_mut() else {
        return Err(Error::Message("WritableStream is closed".to_owned()));
    };
    if let Err(error) = writer.write_all(&chunk).await {
        let error = io_error(error);
        *failed = Some(error.clone());
        *sink = None;
        return Err(error);
    }
    Ok(())
}

fn io_error(error: std::io::Error) -> Error {
    Error::Message(error.to_string())
}

impl StubHookInner for WritableStreamStubHook {
    fn call(&self, path: PropertyPath, args: RpcPayload) -> Result<StubHook> {
        let method = match path.as_slice() {
            [PropertyKey::String(method)] => method.as_str(),
            _ => {
                return Err(Error::Message(
                    "WritableStream stub only supports direct method calls".to_owned(),
                ));
            }
        };
        let (reply, settled) = oneshot::channel();
        let op = match method {
            "write" => WriterOp::Write(args, reply),
            "close" => WriterOp::Close(reply),
            "abort" => WriterOp::Abort(reply),
            other => {
                return Err(Error::Message(format!(
                    "Unknown WritableStream method: {other}"
                )));
            }
        };
        if self.ops.send(op).is_err() {
            return Err(Error::Message("WritableStream writer is gone".to_owned()));
        }
        Ok(PromiseStubHook::from_future(async move {
            settled
                .await
                .map_err(|_| Error::Message("WritableStream writer is gone".to_owned()))??;
            Ok(Arc::new(PayloadStubHook::new(RpcPayload::from_app_return(
                CapnwebValue::Undefined,
            ))) as StubHook)
        }))
    }

    fn map(
        &self,
        _path: PropertyPath,
        _captures: Vec<StubHook>,
        _instructions: MapInstructions,
    ) -> Result<StubHook> {
        Err(Error::Message(
            "Cannot use map() on a WritableStream".to_owned(),
        ))
    }

    fn get(&self, _path: PropertyPath) -> Result<StubHook> {
        Err(Error::Message(
            "Cannot access properties on a WritableStream stub".to_owned(),
        ))
    }

    fn pull(&self) -> Result<MaybePromise<RpcPayload>> {
        Err(Error::Message(
            "Cannot pull a WritableStream stub".to_owned(),
        ))
    }

    fn ignore_unhandled_rejections(&self) {}

    fn on_broken(&self, _callback: Box<dyn FnOnce(Error) + Send + 'static>) {}
}

/// A pipe's writing end: `source` read and sent as write calls on `hook` (the peer's pipe), in order,
/// as many in flight as the flow controller's window allows; at its end a close, at a read error an
/// abort. A rejected write (the peer's reader gone) stops it, dropping the source. `hook` is
/// released at the end.
pub(crate) async fn pump(mut source: ByteSource, hook: StubHook) {
    let started = std::time::Instant::now();
    let mut flow = FlowController::new(move || started.elapsed().as_secs_f64() * 1000.0);
    let mut acks = futures::stream::FuturesUnordered::new();
    let mut buffer = vec![0u8; PIPE_CHUNK];
    use futures::StreamExt as _;
    'reading: loop {
        let read = source.read(&mut buffer).await;
        let n = match read {
            Ok(0) => break 'reading,
            Ok(n) => n,
            Err(error) => {
                drop(source);
                let reason =
                    CapnwebValue::Error(CapnwebError::from(Error::Message(error.to_string())));
                if let Ok(call) = hook.stream(
                    vec![PropertyKey::String("abort".to_owned())],
                    RpcPayload::from_app_params(CapnwebValue::Array(vec![reason])),
                ) {
                    let _ = call.promise.await;
                }
                return;
            }
        };
        let payload = RpcPayload::from_app_params(CapnwebValue::Array(vec![CapnwebValue::Bytes(
            buffer[..n].to_vec(),
        )]));
        let Ok(call) = hook.stream(vec![PropertyKey::String("write".to_owned())], payload) else {
            return;
        };
        match call.size {
            None => {
                if call.promise.await.is_err() {
                    return;
                }
            }
            Some(size) => {
                let (token, mut blocked) = flow.on_send(size as f64);
                acks.push(async move { (token, call.promise.await) });
                while blocked {
                    match acks.next().await {
                        Some((token, Ok(()))) => blocked = !flow.on_ack(token),
                        Some((token, Err(_))) => {
                            flow.on_error(token);
                            return;
                        }
                        None => break,
                    }
                }
            }
        }
        // (acks that have already come in, taken without waiting)
        while let Some(Some((token, result))) = futures::FutureExt::now_or_never(acks.next()) {
            match result {
                Ok(()) => {
                    flow.on_ack(token);
                }
                Err(_) => {
                    flow.on_error(token);
                    return;
                }
            }
        }
    }
    // (the source let go of at its end, not at the close's acknowledgement: a FIFO's reader gone at once)
    drop(source);
    // (every write acknowledged before the close, so a failed write isn't followed by one)
    while let Some((token, result)) = acks.next().await {
        if result.is_err() {
            flow.on_error(token);
            return;
        }
        flow.on_ack(token);
    }
    if let Ok(call) = hook.stream(
        vec![PropertyKey::String("close".to_owned())],
        RpcPayload::from_app_params(CapnwebValue::Array(Vec::new())),
    ) {
        let _ = call.promise.await;
    }
}
