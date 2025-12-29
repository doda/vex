#!/usr/bin/env node
import { ModalClient } from "modal";
import fs from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const CHUNK_SIZE = 4 * 1024 * 1024;

function parseArgs(argv) {
  const args = {
    cpu: 2,
    memoryMiB: 4096,
    timeoutSecs: 1800,
    idleTimeoutSecs: 300,
    appName: "vex-garage-test",
    keep: false,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--cpu") args.cpu = Number(argv[++i]);
    else if (arg === "--memory") args.memoryMiB = Number(argv[++i]);
    else if (arg === "--timeout") args.timeoutSecs = Number(argv[++i]);
    else if (arg === "--idle-timeout") args.idleTimeoutSecs = Number(argv[++i]);
    else if (arg === "--app") args.appName = argv[++i];
    else if (arg === "--keep") args.keep = true;
  }
  return args;
}

async function execLocal(command, args, cwd) {
  const result = await execFileAsync(command, args, { cwd });
  if (result.stdout) process.stdout.write(result.stdout);
  if (result.stderr) process.stderr.write(result.stderr);
  return result;
}

async function createTarball(repoRoot, tarPath) {
  const excludes = [
    ".git",
    ".looper",
    "node_modules",
    "tmp",
    "dist",
    "build",
    ".venv",
    ".pytest_cache",
  ];
  const args = ["-czf", tarPath];
  for (const entry of excludes) {
    args.push("--exclude", entry);
  }
  args.push("-C", repoRoot, ".");
  await execLocal("tar", args, repoRoot);
}

async function uploadFile(sandbox, localPath, remotePath) {
  const data = await fs.readFile(localPath);
  const remoteFile = await sandbox.open(remotePath, "w");
  for (let offset = 0; offset < data.length; offset += CHUNK_SIZE) {
    const chunk = data.subarray(offset, offset + CHUNK_SIZE);
    await remoteFile.write(chunk);
  }
  await remoteFile.flush();
  await remoteFile.close();
}

async function exec(sandbox, command, cwd, stream = false) {
  const wrapped = `cd '${cwd}' && ${command}`;
  const proc = await sandbox.exec(["bash", "-lc", wrapped]);

  let stdout = "";
  let stderr = "";

  if (stream) {
    const drain = async (src, dest, buf) => {
      for await (const chunk of src) {
        const text = typeof chunk === "string" ? chunk : new TextDecoder().decode(chunk);
        buf.s += text;
        dest.write(text);
      }
    };
    const stdoutBuf = { s: "" };
    const stderrBuf = { s: "" };
    await Promise.all([
      drain(proc.stdout, process.stdout, stdoutBuf),
      drain(proc.stderr, process.stderr, stderrBuf),
    ]);
    stdout = stdoutBuf.s;
    stderr = stderrBuf.s;
  } else {
    [stdout, stderr] = await Promise.all([proc.stdout.readText(), proc.stderr.readText()]);
  }

  const exitCode = await proc.wait();
  return { exitCode, stdout, stderr };
}

function ensureModalEnv() {
  if (!process.env.MODAL_TOKEN_ID || !process.env.MODAL_TOKEN_SECRET) {
    throw new Error("MODAL_TOKEN_ID and MODAL_TOKEN_SECRET must be set to run Modal sandboxes.");
  }
}

async function buildImage(client) {
  return client.images
    .fromRegistry("debian:bookworm-slim")
    .dockerfileCommands([
      "RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y curl ca-certificates tar gzip bash git openssl python3 && rm -rf /var/lib/apt/lists/*",
      "RUN curl -fsSL https://go.dev/dl/go1.23.4.linux-amd64.tar.gz | tar -C /usr/local -xz",
      "ENV PATH=$PATH:/usr/local/go/bin",
      "WORKDIR /workspace",
    ]);
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  ensureModalEnv();

  const repoRoot = path.resolve(path.dirname(new URL(import.meta.url).pathname), "../..");
  const tarPath = path.join("/tmp", `vex-upload-${Date.now()}.tar.gz`);

  console.log("Creating tarball...");
  await createTarball(repoRoot, tarPath);

  const client = new ModalClient();
  const app = await client.apps.fromName(args.appName, { createIfMissing: true });
  const image = await buildImage(client);

  console.log("Creating Modal sandbox...");
  const sandbox = await client.sandboxes.create(app, image, {
    cpu: args.cpu,
    memoryMiB: args.memoryMiB,
    timeoutMs: args.timeoutSecs * 1000,
    idleTimeoutMs: args.idleTimeoutSecs * 1000,
    env: {
      PATH: "/usr/local/go/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
    },
  });

  let exitCode = 1;
  try {
    console.log(`Sandbox: ${sandbox.sandboxId}`);
    await exec(sandbox, "mkdir -p /workspace/vex", "/", false);

    console.log("Uploading repo tarball...");
    await uploadFile(sandbox, tarPath, "/tmp/vex.tar.gz");
    await exec(sandbox, "tar -xzf /tmp/vex.tar.gz -C /workspace/vex", "/", false);

    const command = [
      "set -euo pipefail",
      "export PATH=/usr/local/go/bin:$PATH",
      "cd /workspace/vex",
      "bash scripts/setup-garage.sh",
      "source /tmp/vex-garage/creds.env",
      "go test ./tests/integration -v",
    ].join(" && ");

    console.log("Running Garage-backed integration tests...");
    const result = await exec(sandbox, command, "/", true);
    exitCode = result.exitCode;
  } finally {
    if (!args.keep) {
      console.log("Terminating sandbox...");
      await sandbox.terminate();
    } else {
      console.log("Sandbox left running (--keep).");
    }
  }

  process.exit(exitCode);
}

main().catch((err) => {
  console.error(err?.message || err);
  process.exit(1);
});
