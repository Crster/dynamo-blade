export function decodeNext(next: string) {
  if (next) {
    try {
      return JSON.parse(Buffer.from(next, "base64").toString());
    } catch {
      throw new Error("Invalid next value");
    }
  }
}

export function encodeNext(next: any) {
  if (next) {
    return Buffer.from(JSON.stringify(next)).toString("base64");
  }
}
