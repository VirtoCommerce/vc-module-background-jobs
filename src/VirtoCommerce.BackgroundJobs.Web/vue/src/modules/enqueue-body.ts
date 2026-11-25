import type { EnqueueOptions } from "../api_client/virtocommerce.backgroundjobs";

/**
 * Builds the enqueue request body as text, with the payload JSON spliced in exactly as typed.
 * Parsing it into an object and stringifying it again would round large numbers and turn 1e400 into null
 * in the browser, before the server's raw payload binding (RawJsonPayloadConverter) sees it.
 * The caller validates the text; a blank one sends no payload.
 */
export function buildEnqueueBody(name: string, payloadText: string | undefined, options: EnqueueOptions): string {
  const payload = payloadText?.trim();
  const payloadMember = payload ? `,"payload":${payload}` : "";

  return `{"name":${JSON.stringify(name)},"options":${JSON.stringify(options)}${payloadMember}}`;
}
