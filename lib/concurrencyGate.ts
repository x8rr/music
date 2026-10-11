/**
 * A bounded semaphore with a bounded queue.
 *
 * Used to cap how many yt-dlp processes run at once. Each one is a ~75MB
 * Python interpreter, and the stream race starts one per play attempt, so
 * without a cap a busy hour turns into dozens of them and the box — not just
 * this process — runs out of memory. The queue is bounded too: a caller that
 * would wait behind more than `maxWaiting` others is refused immediately, so
 * the stream race moves on to another source instead of piling up.
 */
export class GateBusyError extends Error {
  constructor(message = "too many queued requests") {
    super(message);
    this.name = "GateBusyError";
  }
}

export class ConcurrencyGate {
  private active = 0;
  private readonly waiting: Array<() => void> = [];

  constructor(
    private readonly max: number,
    private readonly maxWaiting: number,
  ) {}

  async run<T>(work: () => Promise<T>): Promise<T> {
    if (this.active >= this.max) {
      if (this.waiting.length >= this.maxWaiting) throw new GateBusyError();
      await new Promise<void>((resolve) => this.waiting.push(resolve));
    } else {
      this.active++;
    }
    try {
      return await work();
    } finally {
      // Hand the slot straight to the next waiter rather than freeing it, so
      // `active` stays exact and nothing can sneak in ahead of the queue.
      const next = this.waiting.shift();
      if (next) next();
      else this.active--;
    }
  }

  stats() {
    return { active: this.active, waiting: this.waiting.length };
  }
}
