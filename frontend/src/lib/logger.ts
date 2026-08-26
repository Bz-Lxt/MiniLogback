type Level = 'debug' | 'info' | 'warn' | 'error';

const priority: Record<Level, number> = {
  debug: 10,
  info: 20,
  warn: 30,
  error: 40,
};

const configuredLevel = (import.meta.env.DEV ? 'debug' : 'warn') as Level;

function write(level: Level, message: string, context?: unknown) {
  if (priority[level] < priority[configuredLevel]) return;
  const method = level === 'debug' ? 'debug' : level;
  const prefix = `[MiniLogback UI] ${message}`;
  if (context === undefined) {
    console[method](prefix);
  } else {
    console[method](prefix, context);
  }
}

export const logger = {
  debug: (message: string, context?: unknown) => write('debug', message, context),
  info: (message: string, context?: unknown) => write('info', message, context),
  warn: (message: string, context?: unknown) => write('warn', message, context),
  error: (message: string, context?: unknown) => write('error', message, context),
};
