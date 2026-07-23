import { LoroDoc } from "loro-crdt";
import {
  CrdtType,
  MessageType,
  UpdateStatusCode,
  decode,
  encode,
} from "loro-protocol";

const url = process.argv[2];
if (!url) {
  throw new Error("usage: node client.mjs <gateway-websocket-url>");
}

const roomId = `typescript-${Date.now()}-${process.pid}`;

class Client {
  constructor(peerId) {
    this.doc = new LoroDoc();
    this.doc.setPeerId(peerId);
    this.messages = [];
    this.waiters = [];
  }

  async connect() {
    this.ws = new WebSocket(url);
    this.ws.binaryType = "arraybuffer";
    this.ws.addEventListener("message", (event) => {
      const message = decode(new Uint8Array(event.data));
      const waiter = this.waiters.shift();
      if (waiter) {
        waiter(message);
      } else {
        this.messages.push(message);
      }
    });
    await new Promise((resolve, reject) => {
      this.ws.addEventListener("open", resolve, { once: true });
      this.ws.addEventListener("error", reject, { once: true });
    });
    this.send({
      crdt: CrdtType.Loro,
      roomId,
      type: MessageType.JoinRequest,
      auth: new Uint8Array(),
      version: this.doc.oplogVersion().encode(),
    });
    const joined = await this.next();
    if (joined.type !== MessageType.JoinResponseOk || joined.permission !== "write") {
      throw new Error(`join failed: ${JSON.stringify(joined)}`);
    }
  }

  send(message) {
    this.ws.send(encode(message));
  }

  next() {
    if (this.messages.length > 0) {
      return Promise.resolve(this.messages.shift());
    }
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("protocol receive timeout")), 5000);
      this.waiters.push((message) => {
        clearTimeout(timer);
        resolve(message);
      });
    });
  }

  async editAndSend(text, batchId) {
    const before = this.doc.oplogVersion();
    const target = this.doc.getText("text");
    target.insert(target.length, text);
    this.doc.commit();
    const update = this.doc.export({ mode: "update", from: before });
    this.send({
      crdt: CrdtType.Loro,
      roomId,
      type: MessageType.DocUpdate,
      updates: [update],
      batchId,
    });
    for (;;) {
      const message = await this.next();
      if (message.type === MessageType.Ack && message.refId === batchId) {
        if (message.status !== UpdateStatusCode.Ok) {
          throw new Error(`update rejected with status ${message.status}`);
        }
        return;
      }
      this.importUpdate(message);
    }
  }

  async receiveUpdate() {
    for (;;) {
      const message = await this.next();
      if (this.importUpdate(message)) {
        return;
      }
    }
  }

  importUpdate(message) {
    if (message.type !== MessageType.DocUpdate) {
      return false;
    }
    for (const update of message.updates) {
      this.doc.import(update);
    }
    return true;
  }

  close() {
    this.ws.close();
  }
}

const first = new Client(1001);
const second = new Client(2002);
await first.connect();
await second.connect();
await first.editAndSend("A", "0x0101010101010101");
await second.receiveUpdate();
await second.editAndSend("B", "0x0202020202020202");
await first.receiveUpdate();

const firstText = first.doc.getText("text").toString();
const secondText = second.doc.getText("text").toString();
if (firstText !== "AB" || secondText !== firstText) {
  throw new Error(`clients did not converge: ${firstText} != ${secondText}`);
}

first.close();
second.close();
console.log("official TypeScript clients converged through the gateway");
