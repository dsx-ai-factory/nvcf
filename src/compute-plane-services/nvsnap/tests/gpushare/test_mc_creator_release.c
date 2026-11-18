/*
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
*/
/*
 * A multicast object (NVLS) shared by one process per GPU, under
 * libnvsnap_gpushare, when its creator no longer holds the handle it
 * created.
 *
 * Process 0 creates the object and sends it as an fd (or, with MC_FABRIC=1,
 * a fabric handle) to the others. Each process binds memory on its GPU and
 * maps the object; process 0 writes through the multicast mapping and every
 * process checks its own memory. Then nvsnap-gpu-suspend suspend + resume
 * runs on all of them, twice, with the same check after each. Modes:
 *
 *   keep   the creator uses the handle it created (control)
 *   self   the creator imports its own export, uses that, and releases the
 *          handle it created (seen with vLLM + FlashInfer MNNVL on GB300)
 *   late   as self, but the creator adds its GPU through the created handle
 *          before releasing it
 *   drop   the creator joins, binds and maps through the created handle,
 *          tears all of that down and releases it; the others keep using
 *          the object
 *
 * Build: gcc -O2 -I/usr/local/cuda/include -o test_mc_creator_release \
 *            test_mc_creator_release.c -L/usr/local/cuda/lib64/stubs -lcuda
 * Run:   LD_PRELOAD=.../libnvsnap_gpushare.so ./test_mc_creator_release <nvsnap-gpu-suspend> <mode> [ngpu]
 *        (needs 2+ GPUs with multicast support; MC_FABRIC=1 also needs an
 *        IMEX channel and NVSNAP_GPUSHARE_FABRIC=1)
 */
#include <cuda.h>
#include <poll.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#define MAXP 8
#define CK(x) do { CUresult r_ = (x); if (r_ != CUDA_SUCCESS) { const char *s_ = "?"; \
    cuGetErrorString(r_, &s_); fprintf(stderr, "[%d] %s: %s\n", getpid(), #x, s_); exit(2); } } while (0)

/* A multimem store reaches the memory every GPU bound to the object. */
static const char *ptx =
    ".version 8.1\n.target sm_90\n.address_size 64\n"
    ".visible .entry fill(.param .u64 p, .param .u32 v) {\n"
    "  .reg .b64 a; .reg .b32 x;\n"
    "  ld.param.u64 a, [p]; ld.param.u32 x, [v];\n"
    "  multimem.st.global.u32 [a], x; ret;\n}\n";

static int fab;

static void send_h(int s, CUmemGenericAllocationHandle mc)
{
    CUmemFabricHandle fh;
    int fd = -1;
    char cbuf[CMSG_SPACE(sizeof(int))] = {0};
    struct iovec iov = { &fh, sizeof(fh) };
    struct msghdr m = { .msg_iov = &iov, .msg_iovlen = 1 };
    if (fab) {
        CK(cuMemExportToShareableHandle(&fh, mc, CU_MEM_HANDLE_TYPE_FABRIC, 0));
    } else {
        CK(cuMemExportToShareableHandle(&fd, mc, CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR, 0));
        m.msg_control = cbuf;
        m.msg_controllen = sizeof(cbuf);
        struct cmsghdr *c = CMSG_FIRSTHDR(&m);
        c->cmsg_level = SOL_SOCKET; c->cmsg_type = SCM_RIGHTS; c->cmsg_len = CMSG_LEN(sizeof(int));
        memcpy(CMSG_DATA(c), &fd, sizeof(int));
    }
    if (sendmsg(s, &m, 0) < 0) exit(2);
    if (fd >= 0) close(fd);
}

static CUmemGenericAllocationHandle recv_h(int s)
{
    CUmemFabricHandle fh;
    CUmemGenericAllocationHandle h;
    char cbuf[CMSG_SPACE(sizeof(int))];
    struct iovec iov = { &fh, sizeof(fh) };
    struct msghdr m = { .msg_iov = &iov, .msg_iovlen = 1, .msg_control = cbuf, .msg_controllen = sizeof(cbuf) };
    if (recvmsg(s, &m, 0) <= 0) exit(2);
    if (fab) {
        CK(cuMemImportFromShareableHandle(&h, &fh, CU_MEM_HANDLE_TYPE_FABRIC));
    } else {
        int fd;
        struct cmsghdr *c = CMSG_FIRSTHDR(&m);
        if (!c) exit(2);
        memcpy(&fd, CMSG_DATA(c), sizeof(int));
        CK(cuMemImportFromShareableHandle(&h, (void *)(uintptr_t)fd, CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR));
        close(fd);
    }
    return h;
}

/* Process k on GPU k. Commands from the parent on cmd: 'w' v (write v
 * through the multicast mapping), 'r' v (check v): one status byte back;
 * 'q': exit. */
static void child(int k, int n, const char *mode, int peers[MAXP], int cmd, int st)
{
    CUdevice dev; CUcontext ctx; CUmodule mod; CUfunction fn;
    CK(cuInit(0)); CK(cuDeviceGet(&dev, k));
    CK(cuDevicePrimaryCtxRetain(&ctx, dev)); CK(cuCtxSetCurrent(ctx));
    CUmemAllocationHandleType ht = fab ? CU_MEM_HANDLE_TYPE_FABRIC : CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR;
    CUmulticastObjectProp mp = { .numDevices = n, .handleTypes = ht };
    size_t g;
    CK(cuMulticastGetGranularity(&g, &mp, CU_MULTICAST_GRANULARITY_RECOMMENDED));
    mp.size = g;
    CUmemAllocationProp p = { .type = CU_MEM_ALLOCATION_TYPE_PINNED, .location = { CU_MEM_LOCATION_TYPE_DEVICE, k },
                              .requestedHandleTypes = ht };
    CUmemAccessDesc a = { { CU_MEM_LOCATION_TYPE_DEVICE, k }, CU_MEM_ACCESS_FLAGS_PROT_READWRITE };
    CUmemGenericAllocationHandle mc, uc;
    CUdeviceptr mva, uva;
    CK(cuMemCreate(&uc, g, &p, 0));
    CK(cuMemAddressReserve(&uva, g, g, 0, 0));
    CK(cuMemMap(uva, g, 0, uc, 0));
    CK(cuMemSetAccess(uva, g, &a, 1));
    CK(cuMemsetD32(uva, 0, g / 4));
    CK(cuMemAddressReserve(&mva, g, g, 0, 0));
    if (k == 0) {
        CUmemGenericAllocationHandle own;
        CK(cuMulticastCreate(&own, &mp));
        for (int i = 1; i < n; i++) send_h(peers[i], own);
        if (!strcmp(mode, "keep")) {
            mc = own;
        } else if (!strcmp(mode, "drop")) {
            /* join and use it, then let go of everything: the others keep it */
            CK(cuMulticastAddDevice(own, dev));
            CK(cuMulticastBindMem(own, 0, uc, 0, g, 0));
            CK(cuMemMap(mva, g, 0, own, 0));
            CK(cuMemSetAccess(mva, g, &a, 1));
            CK(cuMemUnmap(mva, g));
            CK(cuMulticastUnbind(own, dev, 0, g));
            CK(cuMemRelease(own));
            mc = 0;
        } else {
            int sp[2];
            if (socketpair(AF_UNIX, SOCK_DGRAM, 0, sp)) exit(2);
            if (!strcmp(mode, "late")) CK(cuMulticastAddDevice(own, dev));
            send_h(sp[0], own);
            mc = recv_h(sp[1]);
            close(sp[0]); close(sp[1]);
            if (!strcmp(mode, "self")) CK(cuMulticastAddDevice(mc, dev));
            CK(cuMemRelease(own));
        }
        if (!strcmp(mode, "keep")) CK(cuMulticastAddDevice(mc, dev));
    } else {
        mc = recv_h(peers[k]);
        CK(cuMulticastAddDevice(mc, dev));
    }
    if (mc) {
        CK(cuMulticastBindMem(mc, 0, uc, 0, g, 0));
        CK(cuMemMap(mva, g, 0, mc, 0));
        CK(cuMemSetAccess(mva, g, &a, 1));
    }
    CK(cuModuleLoadData(&mod, ptx));
    CK(cuModuleGetFunction(&fn, mod, "fill"));
    char c = 0;
    if (write(st, &c, 1) != 1) exit(2);
    for (;;) {
        uint32_t v = 0, got = 0;
        if (read(cmd, &c, 1) != 1 || c == 'q') exit(0);
        if (read(cmd, &v, 4) != 4) exit(2);
        char ok = 0;
        if (c == 'w') {
            void *args[] = { &mva, &v };
            CK(cuLaunchKernel(fn, 1, 1, 1, 1, 1, 1, 0, 0, args, NULL));
            CK(cuCtxSynchronize());
        } else if (mc) {  /* "drop": process 0 no longer holds the memory the store reaches */
            CK(cuMemcpyDtoH(&got, uva, 4));
            if (got != v) { fprintf(stderr, "[%d] gpu %d: read %#x, want %#x\n", getpid(), k, got, v); ok = 1; }
        }
        if (write(st, &ok, 1) != 1) exit(2);
    }
}

static int n, writer, cmd[MAXP], st[MAXP];
static pid_t pids[MAXP];

/* A status byte from process i; -1 if it died or hung (a minute). */
static int status(int i)
{
    struct pollfd p = { st[i], POLLIN, 0 };
    char ok;
    if (poll(&p, 1, 60000) != 1 || read(st[i], &ok, 1) != 1) {
        fprintf(stderr, "process %d (pid %d) died or hung\n", i, pids[i]);
        return -1;
    }
    return ok;
}

/* Send a command to process k (all: -1); 0 if every one answered 0. */
static int ask(int k, char c, uint32_t v)
{
    int bad = 0;
    for (int i = 0; i < n; i++)
        if (k < 0 || k == i) {
            if (write(cmd[i], &c, 1) != 1 || write(cmd[i], &v, 4) != 4) return -1;
        }
    for (int i = 0; i < n; i++)
        if (k < 0 || k == i) {
            int ok = status(i);
            if (ok < 0) return -1;
            bad |= ok;
        }
    return bad;
}

static int check(uint32_t v) { return ask(writer, 'w', v) || ask(-1, 'r', v); }

static const char *tool;
static int run(const char *action)
{
    char c[1024];
    int o = snprintf(c, sizeof c, "%s %s", tool, action);
    for (int i = 0; i < n; i++) o += snprintf(c + o, sizeof c - o, " %d", pids[i]);
    printf("$ %s\n", c);
    fflush(stdout);
    int s = system(c);
    return WIFEXITED(s) ? WEXITSTATUS(s) : -1;
}

int main(int argc, char **argv)
{
    if (argc < 3) { fprintf(stderr, "usage: %s <nvsnap-gpu-suspend> keep|self|late|drop [ngpu]\n", argv[0]); return 1; }
    tool = argv[1];
    const char *mode = argv[2];
    fab = getenv("MC_FABRIC") != NULL;
    n = argc > 3 ? atoi(argv[3]) : 2;
    if (n < 2 || n > MAXP) return 1;
    writer = !strcmp(mode, "drop");  /* process 0 has no mapping left */
    int peers[MAXP][2];
    for (int i = 0; i < n; i++) if (socketpair(AF_UNIX, SOCK_DGRAM, 0, peers[i])) return 1;
    for (int i = 0; i < n; i++) {
        int c[2], s[2];
        if (pipe(c) || pipe(s)) return 1;
        if (!(pids[i] = fork())) {
            int mine[MAXP];
            for (int j = 0; j < n; j++) mine[j] = i == 0 ? peers[j][0] : peers[j][1];
            child(i, n, mode, mine, c[0], s[1]);
        }
        cmd[i] = c[1];
        st[i] = s[0];
        close(c[0]); close(s[1]);
    }
    int fails = 0;
    for (int i = 0; !fails && i < n; i++)
        if (status(i)) { fprintf(stderr, "process %d: setup failed\n", i); fails = 1; }
    if (!fails && check(0x1111)) { printf("FAIL: before suspend\n"); fails = 1; }
    for (int cycle = 1; !fails && cycle <= 2; cycle++) {
        if (run("suspend") || run("resume")) { printf("FAIL: cycle %d: suspend + resume\n", cycle); fails = 1; break; }
        if (check(0x1111 * (cycle + 1))) { printf("FAIL: cycle %d: multicast store after resume\n", cycle); fails = 1; }
    }
    for (int i = 0; !fails && i < n; i++) if (write(cmd[i], "q", 1) != 1) fails = 1;
    for (int i = 0; i < n; i++) {
        int s;
        if (fails) { kill(pids[i], SIGKILL); continue; }  /* a frozen pid exits only once the holder lets go */
        waitpid(pids[i], &s, 0);
        if (!WIFEXITED(s) || WEXITSTATUS(s)) fails = 1;
    }
    printf("mc_creator_release %s%s n=%d => %s\n", mode, fab ? " fabric" : "", n, fails ? "FAIL" : "PASS");
    return fails;
}
