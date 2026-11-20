#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
"""The launch gate and GPU drain checks in libnvsnap_gpushare.

No GPU: a probe program includes gpushare.c and calls its gate and sync
functions directly, against a stub driver whose cuCtxSynchronize takes
SYNC_MS and counts its calls. The test checks that

  * a "step" permit a held launch has not taken yet counts as busy, and
    quiesce takes it back, so that launch cannot pass the gate afterwards;
  * a drain check ("poll") uses a synchronization that finished between
    two polls, instead of discarding it and reporting busy forever;
  * quiesce never uses a synchronization that finished, or started, before
    it was called;
  * a quiesce that finds the GPU busy while launches are held keeps them
    held (the ranks are catching up), and opens the gates otherwise;
  * when the process that sent "hold" exits, the gates open again, also
    when the state lock is busy at that moment (retried until it is free),
    and also after a "resume" from it that failed;
  * a batched copy (CUDA 12 and 13 entry points) waits at the memory gate.

Run: python3 -I tests/gpushare/host/test_gate_sync.py
(CUDA_INC names the directory with cuda.h; default /usr/local/cuda/include.)
"""

import os
import shutil
import subprocess
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.normpath(os.path.join(HERE, "../../../docker/agent/gpushare"))
CUDA_INC = os.environ.get("CUDA_INC", "/usr/local/cuda/include")

STUB = r"""
#include <stdlib.h>
#include <unistd.h>
typedef int CUresult;
int stub_syncs;
CUresult cuInit(unsigned f) { (void)f; return 0; }
CUresult cuGetErrorName(CUresult r, const char **s) { (void)r; *s = "stub"; return 0; }
CUresult cuDeviceGetCount(int *n) { *n = 1; return 0; }
CUresult cuDevicePrimaryCtxGetState(int d, unsigned *f, int *a) { (void)d; *f = 0; *a = 1; return 0; }
CUresult cuDevicePrimaryCtxRetain(void **c, int d) { (void)d; *c = (void *)1; return 0; }
CUresult cuDevicePrimaryCtxRelease(int d) { (void)d; return 0; }
CUresult cuCtxPushCurrent(void *c) { (void)c; return 0; }
CUresult cuCtxPopCurrent(void **c) { *c = (void *)1; return 0; }
int stub_batches;
CUresult cuMemcpyBatchAsync(void) { __atomic_add_fetch(&stub_batches, 1, __ATOMIC_SEQ_CST); return 0; }
CUresult cuMemcpyBatchAsync_v2(void) { __atomic_add_fetch(&stub_batches, 1, __ATOMIC_SEQ_CST); return 0; }
CUresult cuCtxSynchronize(void)
{
    const char *e = getenv("SYNC_MS");
    usleep((e ? atoi(e) : 0) * 1000);
    __atomic_add_fetch(&stub_syncs, 1, __ATOMIC_SEQ_CST);
    return 0;
}
"""

PROBE = r"""
#include "gpushare.c"
#include <sys/wait.h>

extern int stub_syncs, stub_batches;
static void *waiter(void *arg) { gate_enter(&lgate); gate_exit(&lgate); *(int *)arg = 1; return NULL; }
static CUresult (*batch_fn)(CUdeviceptr *, CUdeviceptr *, size_t *, size_t, CUmemcpyAttributes *, size_t *,
                            size_t, size_t *, CUstream);
static void *batcher(void *arg) { (void)arg; batch_fn(NULL, NULL, NULL, 0, NULL, NULL, 0, NULL, NULL); return NULL; }

int main(int argc, char **argv)
{
    CUresult r;
    char rep[256];
    if (argc > 1 && !strcmp(argv[1], "permit")) {
        /* A "step" permit not taken yet (the waiter has not run). */
        lgate.closed = 1;
        lgate.pass = 1;
        printf("drained=%d\n", drained(50, &r));
        do_quiesce(rep, sizeof(rep));
        printf("quiesce=%s pass=%d\n", rep[0] ? rep : "ok", lgate.pass);
        int passed = 0;
        pthread_t t;
        pthread_create(&t, NULL, waiter, &passed);
        usleep(200000);
        printf("passed_after_quiesce=%d\n", passed);
        gate_open();
        pthread_join(t, NULL);
        printf("passed_after_open=%d\n", passed);
        return 0;
    }
    if (argc > 1 && !strcmp(argv[1], "between")) {
        /* Polls 2 ms long, 10 ms apart; each sync takes SYNC_MS (5). */
        lgate.closed = 1;
        int idle = 0;
        for (int i = 0; i < 10; i++) {
            idle += drained(2, &r) == 1;
            usleep(10000);
        }
        printf("idle_polls=%d\n", idle);
        return 0;
    }
    if (argc > 1 && !strcmp(argv[1], "fresh")) {
        /* A poll's sync finishes and is not consumed; quiesce must sync
         * again after closing each gate. */
        lgate.closed = 1;
        drained(0, &r);
        usleep(20000);
        int before = __atomic_load_n(&stub_syncs, __ATOMIC_SEQ_CST);
        do_quiesce(rep, sizeof(rep));
        printf("quiesce=%s syncs=%d\n", rep[0] ? rep : "ok",
               __atomic_load_n(&stub_syncs, __ATOMIC_SEQ_CST) - before);
        return 0;
    }
    if (argc > 1 && !strcmp(argv[1], "busy")) {
        /* Each sync takes SYNC_MS (2500), longer than quiesce waits. */
        do_quiesce(rep, sizeof(rep));
        printf("unheld=%s:%d:%d\n", rep[0] == 'e' ? "busy" : "ok", lgate.closed, mgate.closed);
        do_hold(rep, sizeof(rep));
        do_quiesce(rep, sizeof(rep));
        printf("held=%s:%d:%d\n", rep[0] == 'e' ? "busy" : "ok", lgate.closed, mgate.closed);
        return 0;
    }
    if (argc > 1 && !strcmp(argv[1], "lease")) {
        /* A child sends "hold" over the control socket, then exits. */
        start_ctl();
        int up[2], down[2];
        if (pipe(up) || pipe(down)) return 3;
        pid_t c = fork();
        if (!c) {
            char b;
            int ok = request(getppid(), "hold", -1, rep, sizeof(rep), NULL) == 0 && !strcmp(rep, "ok held");
            if (write(up[1], ok ? "1" : "0", 1) != 1 || read(down[0], &b, 1) != 1) _exit(1);
            _exit(0);
        }
        char b = 0;
        if (read(up[0], &b, 1) != 1) return 4;
        usleep(300000);
        printf("hold=%c closed_while_alive=%d\n", b, lgate.closed);
        if (write(down[1], "x", 1) != 1) return 5;
        waitpid(c, NULL, 0);
        for (int i = 0; i < 100 && lgate.closed; i++) usleep(10000);
        printf("closed_after_exit=%d\n", lgate.closed);
        return 0;
    }
    if (argc > 1 && (!strcmp(argv[1], "leasebusy") || !strcmp(argv[1], "resumefail") || !strcmp(argv[1], "releasefail"))) {
        /* leasebusy: the holder exits while the state lock is busy.
         * resumefail: the holder's "resume" fails on the busy state lock,
         * then the holder exits while the lock is still busy. releasefail:
         * the same with a "release" refused on the busy lock (nothing was
         * released). Each way the gates must open once the lock is free. */
        const char *cmd2 = !strcmp(argv[1], "resumefail") ? "resume" : !strcmp(argv[1], "releasefail") ? "release" : NULL;
        int resume = cmd2 != NULL;
        start_ctl();
        int up[2], down[2];
        if (pipe(up) || pipe(down)) return 3;
        pid_t c = fork();
        if (!c) {
            char b, rr[256] = "";
            int ok = request(getppid(), "hold", -1, rep, sizeof(rep), NULL) == 0 && !strcmp(rep, "ok held");
            if (write(up[1], ok ? "1" : "0", 1) != 1 || read(down[0], &b, 1) != 1) _exit(1);
            if (resume) {
                request(getppid(), cmd2, -1, rr, sizeof(rr), NULL);
                if (write(up[1], rr[0] == 'o' ? "o" : "e", 1) != 1) _exit(1);
            }
            _exit(0);
        }
        char b = 0, rb = '-';
        if (read(up[0], &b, 1) != 1) return 4;
        pthread_mutex_lock(&mu);
        if (write(down[1], "x", 1) != 1) return 5;
        if (resume && read(up[0], &rb, 1) != 1) return 6;
        waitpid(c, NULL, 0);
        usleep(2600000);  /* lock_ctl gives up after 2 s */
        printf("hold=%c resume=%c closed_while_locked=%d\n", b, rb, lgate.closed);
        pthread_mutex_unlock(&mu);
        for (int i = 0; i < 300 && lgate.closed; i++) usleep(10000);
        printf("closed_after_unlock=%d\n", lgate.closed);
        return 0;
    }
    if (argc > 1 && !strcmp(argv[1], "batch")) {
        /* cuGetProcAddress("cuMemcpyBatchAsync") returns the CUDA 12
         * function or, for CUDA 13, _v2: each gets its own wrapper. */
        void *v1 = cuda_sym("cuMemcpyBatchAsync"), *v2 = cuda_sym("cuMemcpyBatchAsync_v2");
        printf("v1=%d v2=%d\n", wrapper_for("cuMemcpyBatchAsync", v1) == (void *)w_cuMemcpyBatchAsync,
               wrapper_for("cuMemcpyBatchAsync", v2) == (void *)w_cuMemcpyBatchAsync_v2);
        batch_fn = wrapper_for("cuMemcpyBatchAsync", v1);
        mgate.closed = 1;
        pthread_t t;
        pthread_create(&t, NULL, batcher, NULL);
        usleep(200000);
        printf("copied_while_closed=%d\n", stub_batches);
        gate_open();
        pthread_join(t, NULL);
        printf("copied_after_open=%d\n", stub_batches);
        return 0;
    }
    return 2;
}
"""


class GateSync(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not shutil.which("gcc") or not os.path.exists(os.path.join(CUDA_INC, "cuda.h")):
            raise unittest.SkipTest("needs gcc and cuda.h (CUDA_INC)")
        cls.tmp = tempfile.mkdtemp()
        for name, text in (("stub.c", STUB), ("probe.c", PROBE)):
            with open(os.path.join(cls.tmp, name), "w") as f:
                f.write(text)
        subprocess.run(["gcc", "-shared", "-fPIC", "-o", os.path.join(cls.tmp, "libcuda.so.1"),
                        os.path.join(cls.tmp, "stub.c")], check=True)
        subprocess.run(["gcc", "-O2", "-I", SRC, "-I", CUDA_INC, "-o", os.path.join(cls.tmp, "probe"),
                        os.path.join(cls.tmp, "probe.c"), os.path.join(cls.tmp, "libcuda.so.1"),
                        "-Wl,-rpath," + cls.tmp, "-ldl", "-lpthread"], check=True)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def probe(self, case, sync_ms=0):
        p = subprocess.run([os.path.join(self.tmp, "probe"), case], capture_output=True, text=True, timeout=60,
                           env=dict(os.environ, SYNC_MS=str(sync_ms)))
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        return dict(kv.split("=", 1) for line in p.stdout.split("\n") for kv in line.split() if "=" in kv)

    def test_untaken_permit_is_busy_and_revoked(self):
        out = self.probe("permit")
        self.assertEqual(out["drained"], "0", out)
        self.assertEqual(out["quiesce"], "ok", out)
        self.assertEqual(out["pass"], "0", out)
        self.assertEqual(out["passed_after_quiesce"], "0", "a launch passed the gate after quiesce")
        self.assertEqual(out["passed_after_open"], "1", out)

    def test_sync_finished_between_polls_is_used(self):
        out = self.probe("between", sync_ms=5)
        self.assertGreater(int(out["idle_polls"]), 0, "every poll reported busy on a drained GPU")

    def test_quiesce_syncs_again(self):
        out = self.probe("fresh", sync_ms=1)
        self.assertEqual(out["quiesce"], "ok", out)
        self.assertGreaterEqual(int(out["syncs"]), 2, out)

    def test_busy_quiesce_keeps_held_launches_held(self):
        out = self.probe("busy", sync_ms=2500)
        self.assertEqual(out["unheld"], "busy:0:0", "without a hold, a busy quiesce opens the gates")
        self.assertEqual(out["held"], "busy:1:0", "a busy quiesce let held launches run ahead of their peers")

    def test_gates_open_when_the_holder_exits(self):
        out = self.probe("lease")
        self.assertEqual(out["hold"], "1", out)
        self.assertEqual(out["closed_while_alive"], "1", out)
        self.assertEqual(out["closed_after_exit"], "0", "launches stayed held after the tool that held them exited")

    def test_gates_open_when_the_holder_exits_on_a_busy_lock(self):
        out = self.probe("leasebusy")
        self.assertEqual(out["hold"], "1", out)
        self.assertEqual(out["closed_while_locked"], "1", out)
        self.assertEqual(out["closed_after_unlock"], "0", "launches stayed held after the state lock was free again")

    def test_failed_resume_keeps_the_lease(self):
        out = self.probe("resumefail")
        self.assertEqual(out["hold"], "1", out)
        self.assertEqual(out["resume"], "e", "resume was expected to fail on the busy state lock: %s" % out)
        self.assertEqual(out["closed_after_unlock"], "0", "a failed resume ended the lease: launches stayed held")

    def test_refused_release_keeps_the_lease(self):
        out = self.probe("releasefail")
        self.assertEqual(out["hold"], "1", out)
        self.assertEqual(out["resume"], "e", "release was expected to be refused on the busy state lock: %s" % out)
        self.assertEqual(out["closed_after_unlock"], "0", "a refused release ended the lease: launches stayed held")

    def test_batched_copy_waits_at_the_memory_gate(self):
        out = self.probe("batch")
        self.assertEqual((out["v1"], out["v2"]), ("1", "1"), out)
        self.assertEqual(out["copied_while_closed"], "0", "a batched copy passed the closed memory gate")
        self.assertEqual(out["copied_after_open"], "1", out)


if __name__ == "__main__":
    unittest.main(verbosity=2)
