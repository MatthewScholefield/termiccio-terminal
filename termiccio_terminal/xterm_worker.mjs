import readline from "node:readline";
import headlessPkg from "@xterm/headless";
import serializePkg from "@xterm/addon-serialize";

const { Terminal } = headlessPkg;
const { SerializeAddon } = serializePkg;

const BACKGROUND_SAMPLE_ROWS = 4;
const BACKGROUND_DOMINANCE = 0.9;

const terminals = new Map();

function describeError(error) {
  if (error instanceof Error) {
    return error.stack || `${error.name}: ${error.message}`;
  }
  return String(error);
}

process.on("uncaughtException", (error) => {
  console.error(
    `Uncaught exception in headless xterm worker:\n${describeError(error)}`,
  );
  process.exit(1);
});

process.on("unhandledRejection", (reason) => {
  console.error(
    `Unhandled rejection in headless xterm worker:\n${describeError(reason)}`,
  );
  process.exit(1);
});

function createTerminal(command) {
  const terminal = new Terminal({
    allowProposedApi: true,
    cols: command.cols,
    rows: command.rows,
    scrollback: command.scrollback,
    theme: command.theme ?? undefined,
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

const ANSI_THEME_KEYS = [
  "black", "red", "green", "yellow", "blue", "magenta", "cyan", "white",
  "brightBlack", "brightRed", "brightGreen", "brightYellow", "brightBlue", "brightMagenta", "brightCyan", "brightWhite",
];

function paletteColor(index, terminal) {
  if (index < ANSI_THEME_KEYS.length) return terminal.options.theme?.[ANSI_THEME_KEYS[index]] ?? null;
  if (index < 232) {
    const offset = index - 16;
    const levels = [0, 95, 135, 175, 215, 255];
    const red = levels[Math.floor(offset / 36)];
    const green = levels[Math.floor((offset % 36) / 6)];
    const blue = levels[offset % 6];
    return `#${((red << 16) | (green << 8) | blue).toString(16).padStart(6, "0")}`;
  }
  const level = 8 + (index - 232) * 10;
  return `#${((level << 16) | (level << 8) | level).toString(16).padStart(6, "0")}`;
}

function effectiveExplicitBackground(cell, terminal) {
  if (cell.isInverse()) {
    if (cell.isFgRGB()) return `#${cell.getFgColor().toString(16).padStart(6, "0")}`;
    if (cell.isFgPalette()) return paletteColor(cell.getFgColor(), terminal);
    return terminal.options.theme?.foreground ?? null;
  }
  if (cell.isBgRGB()) return `#${cell.getBgColor().toString(16).padStart(6, "0")}`;
  if (cell.isBgPalette()) return paletteColor(cell.getBgColor(), terminal);
  return null;
}

function dominantBottomBackground(terminal) {
  const buffer = terminal.buffer.active;
  const firstRow = buffer.baseY + Math.max(0, terminal.rows - BACKGROUND_SAMPLE_ROWS);
  const counts = new Map();
  let explicitCount = 0;
  const total = terminal.rows > 0 ? Math.min(BACKGROUND_SAMPLE_ROWS, terminal.rows) * terminal.cols : 0;
  const cell = buffer.getNullCell();

  for (let row = firstRow; row < buffer.baseY + terminal.rows; row += 1) {
    const line = buffer.getLine(row);
    if (!line) continue;
    for (let column = 0; column < terminal.cols; column += 1) {
      line.getCell(column, cell);
      const color = effectiveExplicitBackground(cell, terminal);
      if (color === null) continue;
      counts.set(color, (counts.get(color) ?? 0) + 1);
      explicitCount += 1;
    }
  }

  if (total === 0 || explicitCount / total < BACKGROUND_DOMINANCE) return null;
  let dominantColor = null;
  let dominantCount = 0;
  for (const [color, count] of counts) {
    if (count > dominantCount) {
      dominantColor = color;
      dominantCount = count;
    }
  }
  if (dominantCount / total < BACKGROUND_DOMINANCE) return null;
  return dominantColor;
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
      const { terminal, serializeAddon } = getTerminal(command.terminal_id);
      return {
        data: serializeAddon.serialize(),
        background: dominantBottomBackground(terminal),
      };
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
      error: describeError(error),
    });
  }
}

for (const entry of terminals.values()) {
  entry.terminal.dispose();
}
terminals.clear();
