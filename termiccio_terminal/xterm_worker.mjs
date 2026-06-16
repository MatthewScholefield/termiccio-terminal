import readline from "node:readline";
import headlessPkg from "@xterm/headless";
import serializePkg from "@xterm/addon-serialize";

const { Terminal } = headlessPkg;
const { SerializeAddon } = serializePkg;

const terminals = new Map();

function createTerminal(command) {
  const terminal = new Terminal({
    allowProposedApi: true,
    cols: command.cols,
    rows: command.rows,
    scrollback: command.scrollback,
  });
  const serializeAddon = new SerializeAddon();
  terminal.loadAddon(serializeAddon);
  terminals.set(command.terminal_id, { terminal, serializeAddon });
}

function getTerminal(terminalId) {
  const entry = terminals.get(terminalId);
  if (!entry) {
    throw new Error(`Unknown terminal: ${terminalId}`);
  }
  return entry;
}

function writeTerminal(terminalId, data) {
  const { terminal } = getTerminal(terminalId);
  return new Promise((resolve) => {
    terminal.write(data, resolve);
  });
}

async function handleCommand(command) {
  switch (command.type) {
    case "create":
      createTerminal(command);
      return {};
    case "write":
      await writeTerminal(command.terminal_id, command.data);
      return {};
    case "resize": {
      const { terminal } = getTerminal(command.terminal_id);
      terminal.resize(command.cols, command.rows);
      return {};
    }
    case "snapshot": {
      const { serializeAddon } = getTerminal(command.terminal_id);
      return { data: serializeAddon.serialize() };
    }
    case "dispose": {
      const entry = terminals.get(command.terminal_id);
      if (entry) {
        entry.terminal.dispose();
        terminals.delete(command.terminal_id);
      }
      return {};
    }
    default:
      throw new Error(`Unknown command type: ${command.type}`);
  }
}

function send(response) {
  process.stdout.write(`${JSON.stringify(response)}\n`);
}

const rl = readline.createInterface({
  input: process.stdin,
  crlfDelay: Infinity,
});

for await (const line of rl) {
  if (!line.trim()) continue;
  let command;
  try {
    command = JSON.parse(line);
    const result = await handleCommand(command);
    send({ request_id: command.request_id, ok: true, ...result });
  } catch (error) {
    send({
      request_id: command?.request_id,
      ok: false,
      error: error instanceof Error ? error.message : String(error),
    });
  }
}

for (const entry of terminals.values()) {
  entry.terminal.dispose();
}
terminals.clear();
