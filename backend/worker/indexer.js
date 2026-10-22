// Soroban Contract Event Indexer Worker
// Polls contract events and normalizes them for backend use

import axios from "axios";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "node:url";
import { isMainThread, parentPort } from "node:worker_threads";
import { scValToNative, xdr } from "@stellar/stellar-sdk";
import { loadIndexerConfig } from "./indexerConfig.js";

// CONFIGURATION
const CONTRACT_ID = process.env.SOROBAN_CONTRACT_ID || ""; // Set in env
const SOROBAN_RPC_URL = process.env.SOROBAN_RPC_URL || "https://rpc-futurenet.stellar.org";
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const INDEX_FILE = path.join(__dirname, "indexed-events.json");

// Operational settings are env-driven (see indexerConfig.js for the variables
// and their defaults, which match the values previously hardcoded here).
const {
  pollIntervalMs: POLL_INTERVAL_MS,
  maxRetries: MAX_RETRIES,
  initialBackoffMs: INITIAL_BACKOFF_MS,
} = loadIndexerConfig();

// Retry wrapper with exponential backoff
async function retryWithBackoff(fn, maxRetries = MAX_RETRIES) {
  let lastError;
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      return await fn();
    } catch (err) {
      lastError = err;
      if (attempt < maxRetries - 1) {
        const backoffMs = INITIAL_BACKOFF_MS * Math.pow(2, attempt);
        console.log(`[Indexer] Retry attempt ${attempt + 1}/${maxRetries} after ${backoffMs}ms. Error: ${err.message}`);
        await new Promise(resolve => setTimeout(resolve, backoffMs));
      } else {
        console.error(`[Indexer] All ${maxRetries} retries exhausted. Last error:`, err.message);
      }
    }
  }
  throw lastError;
}

const EVENT_TYPES = {
  Create: "create",
  Reserv: "reserve",
  Releas: "release",
  Refund: "refund",
  Dispt: "disputed",
};

function decodeScVal(value) {
  if (typeof value !== "string") return value;
  return scValToNative(xdr.ScVal.fromXDR(value, "base64"));
}

function eventData(value) {
  const decoded = decodeScVal(value);
  if (decoded instanceof Map) {
    return Object.fromEntries(decoded.entries());
  }
  return decoded && typeof decoded === "object" ? decoded : {};
}

function jsonSafe(value) {
  return typeof value === "bigint" ? value.toString() : value;
}

function addressString(value) {
  return typeof value === "string" || value == null ? value : value.toString();
}

// Decode Soroban's XDR topics and value before passing events to consumers.
export function normalizeEvent(event) {
  const topics = Array.isArray(event.topic)
    ? event.topic.map(decodeScVal)
    : Array.isArray(event.topics)
      ? event.topics.map(decodeScVal)
      : [];
  const data = eventData(event.value ?? event.data ?? event);
  const bountyId = data.bounty_id ?? data.bountyId ?? event.bounty_id;
  const maintainer = addressString(data.maintainer ?? event.maintainer);
  const contributor = addressString(data.contributor ?? event.contributor);
  const arbiter = addressString(data.arbiter ?? event.arbiter);

  return {
    id: event.id ?? event.paging_token ?? event.pagingToken,
    type: EVENT_TYPES[topics[1]] ?? event.type,
    bountyId: jsonSafe(bountyId),
    actor: maintainer ?? contributor ?? addressString(event.actor),
    maintainer,
    contributor,
    arbiter,
    amount: jsonSafe(data.amount ?? event.amount),
    timestamp: event.ledger_closed_at ?? event.ledgerClosedAt ?? event.timestamp,
    raw: event,
  };
}

// Save events to file (or replace with DB logic)
function saveEvents(events) {
  if (parentPort) {
    // In worker mode, send events to the main thread instead of persisting locally
    parentPort.postMessage({ type: "indexedEvents", events });
  } else {
    fs.writeFileSync(INDEX_FILE, JSON.stringify(events, null, 2));
  }
}

// Load last indexed event (for polling)
function loadLastEventId() {
  if (!fs.existsSync(INDEX_FILE)) return null;
  const events = JSON.parse(fs.readFileSync(INDEX_FILE, "utf-8"));
  return events.length ? events[events.length - 1].id : null;
}

// Poll Soroban contract events
async function pollEvents() {
  let lastEventId = loadLastEventId();
  try {
    const res = await retryWithBackoff(() =>
      axios.get(`${SOROBAN_RPC_URL}/events`, {
        params: {
          contract_id: CONTRACT_ID,
          from_id: lastEventId,
        },
      })
    );
    const events = res.data.events || [];
    if (events.length) {
      const normalized = events.map(normalizeEvent);
      let allEvents = [];
      if (!parentPort) {
        if (fs.existsSync(INDEX_FILE)) {
          allEvents = JSON.parse(fs.readFileSync(INDEX_FILE, "utf-8"));
        }
        allEvents.push(...normalized);
      } else {
        allEvents = normalized;
      }
      saveEvents(allEvents);
      console.log(`[Indexer] Indexed ${events.length} new events.`);
    } else {
      console.log("[Indexer] No new events.");
    }
  } catch (err) {
    console.error("[Indexer] Polling failed after all retries:", err.message);
  }
}

function startWorker() {
  console.log("[Indexer] Starting Soroban contract event indexer...");
  // Deliberately omits SOROBAN_RPC_URL: some RPC providers embed an API key in it.
  console.log(
    `[Indexer] Effective config: pollIntervalMs=${POLL_INTERVAL_MS} ` +
      `maxRetries=${MAX_RETRIES} initialBackoffMs=${INITIAL_BACKOFF_MS}`,
  );
  setInterval(pollEvents, POLL_INTERVAL_MS);
}

if (isMainThread && process.argv[1] && path.resolve(process.argv[1]) === __filename) {
  startWorker();
}

export { pollEvents, startWorker };
