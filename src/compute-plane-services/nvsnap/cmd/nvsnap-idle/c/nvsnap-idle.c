/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * nvsnap-idle keeps a container alive until it is stopped: the CRIU restore
 * placeholder and the model staging hold. It needs nothing from the image it
 * runs in (static, no shell) and is single-threaded on purpose.
 *
 * A CRIU restore recreates every dumped process and thread at its original
 * id, so the placeholder must hold no id the dump may use. The agent raises
 * the pid namespace's next pid well above the dumped range, then waits until
 * every task here other than init sits above it. This program helps that
 * along like `while true; do sleep 2; done`: one child at a time, each living
 * TICK_SECONDS, so after the raise the next child lands above the range and
 * the last one below it is gone within a tick. A multi-threaded runtime would
 * hold low thread ids for its whole life.
 *
 * Cold start in place:
 *
 *   nvsnap-idle --cold-start MARKER -- COMMAND [ARG...]
 *
 * A placeholder whose restore failed, in a pod nothing would recreate, starts
 * its workload the way the pod would have without nvsnap. The agent creates
 * MARKER, on a volume that outlives the container, then kills this program;
 * killing a pid namespace's init ends everything in it, the restored processes
 * included. When the container restarts, this program finds MARKER and execs
 * COMMAND, which becomes the container's init as in a fresh start; so does
 * every later restart.
 *
 * While it idles, it blanks COMMAND [ARG...] in its own argument memory, so
 * /proc/PID/cmdline (its ticking children's too) shows only
 * "nvsnap-idle --cold-start MARKER --". Tools that find the workload by its
 * command line (a gang health sidecar looking for "-m dynamo.vllm",
 * pkill -f) must not take the placeholder for it. The arguments are only
 * needed by a start that finds MARKER, which execs before this.
 *
 * Stopping: a restored workload runs in this container, as this program's
 * descendants or beside it in the pod's shared pid namespace. On SIGTERM
 * this program passes the signal to every other process of its container
 * (its cgroup) and waits until they have exited, so the workload shuts down
 * as it would have without nvsnap, instead of being killed when this program
 * exits. The kubelet's SIGKILL at the end of the grace period still ends
 * everything. Only in a Kubernetes pod's container (KUBERNETES_SERVICE_HOST
 * set, its cgroup "/" in a private cgroup namespace or a kubepods path):
 * run elsewhere, this program's cgroup may hold unrelated processes.
 * NVSNAP_IDLE_PROCS, for tests, names a file to read the pids from instead.
 */
#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define TICK_SECONDS 2

/* hide_args zeroes argv[from..argc-1] where the kernel shows the command
 * line. The strings are contiguous there; if not, they are left alone. */
static void hide_args(int argc, char **argv, int from) {
	if (from >= argc)
		return;
	for (int i = from + 1; i < argc; i++)
		if (argv[i] != argv[i - 1] + strlen(argv[i - 1]) + 1)
			return;
	char *end = argv[argc - 1] + strlen(argv[argc - 1]);
	memset(argv[from], 0, (size_t)(end - argv[from]));
}

static volatile sig_atomic_t stop;

/* procs_path finds the file listing this container's processes: the
 * cgroup v2 cgroup.procs of this process's cgroup, if that cgroup is a
 * Kubernetes container's. Returns 0 when there is none. */
static int procs_path(char *buf, size_t n) {
	const char *override = getenv("NVSNAP_IDLE_PROCS");
	if (override && override[0]) {
		snprintf(buf, n, "%s", override);
		return 1;
	}
	const char *k8s = getenv("KUBERNETES_SERVICE_HOST");
	if (!k8s || !k8s[0])
		return 0;
	FILE *f = fopen("/proc/self/cgroup", "r");
	if (!f)
		return 0;
	char line[4096];
	int ok = 0;
	while (fgets(line, sizeof line, f)) {
		if (strncmp(line, "0::", 3) != 0)
			continue;
		line[strcspn(line, "\n")] = '\0';
		const char *path = line + 3;
		/* In a private cgroup namespace the container's cgroup is "/". */
		if (strstr(path, "kubepods") || strcmp(path, "/") == 0) {
			snprintf(buf, n, "/sys/fs/cgroup%s/cgroup.procs", strcmp(path, "/") == 0 ? "" : path);
			ok = 1;
		}
		break;
	}
	fclose(f);
	return ok;
}

/* signal_others sends sig to every process listed in procs other than this
 * one; returns how many there were, or -1 when procs cannot be read. */
static int signal_others(const char *procs, int sig) {
	FILE *f = fopen(procs, "r");
	if (!f)
		return -1;
	pid_t self = getpid();
	int n = 0;
	long pid;
	while (fscanf(f, "%ld", &pid) == 1) {
		if ((pid_t)pid == self || pid <= 0)
			continue;
		if (sig == 0 && kill((pid_t)pid, 0) != 0)
			continue; /* gone, or listed in a file only a test keeps */
		if (sig != 0)
			kill((pid_t)pid, sig);
		n++;
	}
	fclose(f);
	return n;
}

/* forward_term passes SIGTERM to the rest of the container and waits,
 * reaping what is this program's, until none of it is left. */
static void forward_term(void) {
	char procs[4200];
	if (!procs_path(procs, sizeof procs))
		return;
	if (signal_others(procs, SIGTERM) <= 0)
		return;
	for (;;) {
		while (waitpid(-1, NULL, WNOHANG) > 0) {
		}
		if (signal_others(procs, 0) <= 0)
			return;
		struct timespec ts = {0, 100 * 1000 * 1000};
		nanosleep(&ts, NULL);
	}
}

static void on_term(int sig) { (void)sig; stop = 1; }

int main(int argc, char **argv) {
	if (argc > 1) {
		if (argc < 5 || strcmp(argv[1], "--cold-start") != 0 || argv[2][0] == '\0' ||
		    strcmp(argv[3], "--") != 0 || argv[4][0] == '\0') {
			fprintf(stderr, "usage: nvsnap-idle [--cold-start MARKER -- COMMAND [ARG...]]\n");
			return 2;
		}
		if (access(argv[2], F_OK) == 0) {
			execvp(argv[4], argv + 4);
			fprintf(stderr, "nvsnap-idle: cannot start %s: %s\n", argv[4], strerror(errno));
			return 127;
		}
		hide_args(argc, argv, 4);
	}

	struct sigaction sa = {0};
	sa.sa_handler = on_term;
	sigemptyset(&sa.sa_mask);
	/* No SA_RESTART: a term must interrupt the wait below. */
	sigaction(SIGTERM, &sa, NULL);
	sigaction(SIGINT, &sa, NULL);

	while (!stop) {
		pid_t child = fork();
		if (child == 0) {
			struct timespec ts = {TICK_SECONDS, 0};
			signal(SIGTERM, SIG_DFL);
			signal(SIGINT, SIG_DFL);
			while (nanosleep(&ts, &ts) == -1 && errno == EINTR) {
			}
			_exit(0);
		}
		if (child < 0) {
			struct timespec ts = {TICK_SECONDS, 0};
			nanosleep(&ts, NULL);
			continue;
		}
		/* As a container's init this also collects orphans re-parented here;
		 * keep waiting until our own child is gone. */
		for (;;) {
			pid_t got = waitpid(-1, NULL, 0);
			if (got == child || (got < 0 && errno == ECHILD))
				break;
			if (got < 0 && errno == EINTR && stop) {
				kill(child, SIGKILL);
				waitpid(child, NULL, 0);
				break;
			}
		}
	}
	forward_term();
	return 0;
}
