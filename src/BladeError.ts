export enum BladeErrorCode {
  Unknown = 0,
  KeyNotSet = 1,
  FieldIsRequired = 2,
}

export class BladeError extends Error {
  public readonly code: BladeErrorCode;

  constructor(code: BladeErrorCode, message: string) {
    super(message);
    this.code = code;
  }
}
