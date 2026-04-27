import { spawn } from 'child_process';
import net from 'net';
import process from 'process';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const projectRoot = path.resolve(__dirname, '..');

const children = [];
let shuttingDown = false;

function checkPortAvailability(port) {
  return new Promise((resolve) => {
    const server = net.createServer();

    server.once('error', () => {
      resolve(false);
    });

    server.once('listening', () => {
      server.close(() => resolve(true));
    });

    server.listen(port);
  });
}

async function findAvailablePort(startPort) {
  for (let port = startPort; port < startPort + 20; port += 1) {
    const available = await checkPortAvailability(port);

    if (available) {
      return port;
    }
  }

  throw new Error(`Kein freier Dev-Port ab ${startPort} gefunden.`);
}

function shutdown(code = 0) {
  if (shuttingDown) {
    return;
  }

  shuttingDown = true;

  for (const child of children) {
    if (!child.killed) {
      child.kill('SIGINT');
    }
  }

  setTimeout(() => {
    process.exit(code);
  }, 300);
}

const requestedPort = Number.parseInt(process.env.PORT || '4000', 10) || 4000;
const backendPort = await findAvailablePort(requestedPort);
const apiBaseUrl = process.env.VITE_API_BASE_URL || `http://127.0.0.1:${backendPort}`;
const backendRoot = path.join(projectRoot, 'backend');
const frontendRoot = path.join(projectRoot, 'frontend');

if (backendPort !== requestedPort) {
  console.log(`Port ${requestedPort} ist belegt, Backend startet stattdessen auf ${backendPort}.`);
}

console.log(`Frontend verwendet ${apiBaseUrl} als API-Basis.`);

const tasks = [
  {
    name: 'backend',
    cwd: backendRoot,
    command: process.platform === 'win32' ? process.env.ComSpec || 'cmd.exe' : 'npm',
    args: process.platform === 'win32' ? ['/d', '/s', '/c', 'npm run dev'] : ['run', 'dev'],
    env: {
      ...process.env,
      PORT: String(backendPort)
    }
  },
  {
    name: 'frontend',
    cwd: frontendRoot,
    command: process.platform === 'win32' ? process.env.ComSpec || 'cmd.exe' : 'npm',
    args:
      process.platform === 'win32'
        ? ['/d', '/s', '/c', 'npm run dev -- --host 127.0.0.1 --clearScreen false']
        : ['run', 'dev', '--', '--host', '127.0.0.1', '--clearScreen', 'false'],
    env: {
      ...process.env,
      VITE_API_BASE_URL: apiBaseUrl
    }
  }
];

for (const task of tasks) {
  const child = spawn(task.command, task.args, {
    cwd: task.cwd,
    stdio: 'inherit',
    env: task.env
  });

  child.on('error', (error) => {
    if (!shuttingDown) {
      console.error(`${task.name} dev task failed to start`, error);
      shutdown(1);
    }
  });

  child.on('exit', (code) => {
    if (!shuttingDown && code && code !== 0) {
      console.error(`${task.name} dev task exited with code ${code}`);
      shutdown(code);
    }
  });

  children.push(child);
}

process.on('SIGINT', () => shutdown(0));
process.on('SIGTERM', () => shutdown(0));
