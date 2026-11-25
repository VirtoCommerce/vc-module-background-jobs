import { BackgroundJobsAdminClient, type EnqueueOptions } from "../api_client/virtocommerce.backgroundjobs";
import { buildEnqueueBody } from "./enqueue-body";

/**
 * The generated client with one more method. Its `enqueue` stringifies a parsed payload object, which cannot keep
 * the typed text intact, and its `http` field is private, so this sends through `window.fetch` (the shell's
 * interceptor-wrapped one, which the generated client uses by default as well) and hands the response to the
 * generated `processEnqueue`, so errors surface as they do for `enqueue`.
 */
export class RawEnqueueClient extends BackgroundJobsAdminClient {
  enqueueRaw(name: string, payloadText: string | undefined, options: EnqueueOptions): Promise<string> {
    const options_: RequestInit = {
      body: buildEnqueueBody(name, payloadText, options),
      method: "POST",
      headers: {
        "Content-Type": "application/json-patch+json",
        Accept: "application/json",
      },
    };

    return this.transformOptions(options_)
      .then((transformed) => window.fetch("/api/background-jobs/enqueue", transformed))
      .then((response) => this.processEnqueue(response));
  }
}
