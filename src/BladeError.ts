export class BladeError extends Error {
  public readonly reason: string;

  constructor(reason: string, message?: string) {
    super(message);

    this.reason = reason;
  }
}
