export type BladeErrorHandler = (error: BladeError) => void;

export class BladeError extends Error {
  public readonly reason?: string;

  constructor(message: string, reason?: string) {
    super(message);

    this.reason = reason;
  }
}
