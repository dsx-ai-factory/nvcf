#!/usr/bin/env python3
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
"""libnvsnap_ibv: NCCL's verbs, destroyed before a checkpoint and created
again after it, on other NICs.

No NIC: a fake libibverbs moves bytes between the queue pairs of one
process, and checks every queue pair number, GID, LID and key it is given
against the live objects. fake_move() makes it another node: a new GID and
LID, and new numbers for every queue pair and registration. A program that
uses verbs as NCCL does (dlopen, dlvsym, the shadow context's ops) writes
with immediate data from one queue pair to the other, then the test

  * suspends: every device is closed, and a completion NCCL had not polled
    yet, and a receive it had posted, survive;
  * moves to the "other node", restores, exchanges the new numbers through
    a fabmap-style file and reconnects;
  * writes again with the numbers and keys NCCL had from before: the bytes
    land, the receive posted before the suspend completes, and the
    completion names NCCL's queue pair number.

Run: python3 -I tests/gpushare/host/test_ibv.py
(IBV_INC names the directory with infiniband/verbs.h; default /usr/include.
CUDA_INC names the one with cuda.h; default /usr/local/cuda/include.)
"""

import os
import shutil
import subprocess
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.normpath(os.path.join(HERE, "../../../docker/agent/gpushare"))
IBV_INC = os.environ.get("IBV_INC", "/usr/include")
CUDA_INC = os.environ.get("CUDA_INC", "/usr/local/cuda/include")

FAKE = r"""
#include <infiniband/verbs.h>
#include <stdlib.h>
#include <string.h>

/* Two devices that number their keys and queue pairs alike, as NICs do. */
static int epoch, counter[2], open_ctxs;
static struct ibv_device dev0 = { .node_type = IBV_NODE_CA, .transport_type = IBV_TRANSPORT_IB, .name = "fake0" };
static struct ibv_device dev1 = { .node_type = IBV_NODE_CA, .transport_type = IBV_TRANSPORT_IB, .name = "fake1" };
static struct ibv_device *devs[3] = { &dev0, &dev1, NULL };
static int di(struct ibv_context *c) { return c->device == &dev1; }

struct fmr { struct ibv_mr m; int live; };
struct fwc { struct ibv_wc w; };
struct fcq { struct ibv_cq c; struct ibv_wc q[256]; int n; int live; };
struct frecv { uint64_t wr_id; struct ibv_sge sg; };
struct fqp { struct ibv_qp q; uint32_t dest; union ibv_gid dgid; uint16_t dlid; int global;
             struct frecv r[64]; int nr; int live; };
static struct fmr *mrs[256]; static int n_mrs;
static struct fqp *qps[256]; static int n_qps;

static void gid_of(int e, int d, union ibv_gid *g)
{
    memset(g, 0, sizeof(*g));
    g->raw[10] = g->raw[11] = 0xff;
    g->raw[12] = 10; g->raw[14] = d; g->raw[15] = 1 + e;
}

int fake_open_contexts(void) { return open_ctxs; }
void fake_move(void) { epoch++; counter[0] = counter[1] = 0; }

static void push(struct fcq *c, struct ibv_wc w) { if (c->n < 256) c->q[c->n++] = w; }

static int poll_cq(struct ibv_cq *cq, int n, struct ibv_wc *wc)
{
    struct fcq *c = (struct fcq *)cq;
    int k = c->n < n ? c->n : n;
    memcpy(wc, c->q, k * sizeof(*wc));
    memmove(c->q, c->q + k, (c->n - k) * sizeof(*wc));
    c->n -= k;
    return k;
}

/* A key of device c's (queue pairs here connect within one device). */
static struct fmr *mr_by(uint32_t key, int remote, struct ibv_context *c)
{
    for (int i = 0; i < n_mrs; i++)
        if (mrs[i]->live && mrs[i]->m.context->device == c->device &&
            (remote ? mrs[i]->m.rkey : mrs[i]->m.lkey) == key) return mrs[i];
    return NULL;
}

static int post_send(struct ibv_qp *qp, struct ibv_send_wr *wr, struct ibv_send_wr **bad)
{
    struct fqp *s = (struct fqp *)qp;
    union ibv_gid me;
    gid_of(epoch, di(qp->context), &me);
    for (; wr; wr = wr->next) {
        int status = IBV_WC_SUCCESS;
        struct fqp *d = NULL;
        for (int i = 0; i < n_qps; i++)
            if (qps[i]->live && qps[i]->q.context->device == qp->context->device && qps[i]->q.qp_num == s->dest) d = qps[i];
        if (s->q.state != IBV_QPS_RTS || !d || (s->global ? memcmp(s->dgid.raw, me.raw, 16) : s->dlid != 1 + epoch))
            status = IBV_WC_RETRY_EXC_ERR;
        size_t len = 0;
        for (int j = 0; j < wr->num_sge; j++) len += wr->sg_list[j].length;
        if (!status && len) {
            struct fmr *rm = mr_by(wr->wr.rdma.rkey, 1, qp->context);
            if (!rm || wr->wr.rdma.remote_addr < (uintptr_t)rm->m.addr ||
                wr->wr.rdma.remote_addr + len > (uintptr_t)rm->m.addr + rm->m.length)
                status = IBV_WC_REM_ACCESS_ERR;
            char *to = (char *)(uintptr_t)wr->wr.rdma.remote_addr;
            for (int j = 0; !status && j < wr->num_sge; j++) {
                if (!mr_by(wr->sg_list[j].lkey, 0, qp->context)) { status = IBV_WC_LOC_PROT_ERR; break; }
                memcpy(to, (void *)(uintptr_t)wr->sg_list[j].addr, wr->sg_list[j].length);
                to += wr->sg_list[j].length;
            }
        }
        if (!status && wr->opcode == IBV_WR_RDMA_WRITE_WITH_IMM) {
            if (!d->nr) status = IBV_WC_RNR_RETRY_EXC_ERR;
            else {
                struct ibv_wc w = { .wr_id = d->r[0].wr_id, .status = IBV_WC_SUCCESS, .opcode = IBV_WC_RECV_RDMA_WITH_IMM,
                                    .byte_len = len, .imm_data = wr->imm_data, .qp_num = d->q.qp_num,
                                    .wc_flags = IBV_WC_WITH_IMM };
                memmove(d->r, d->r + 1, --d->nr * sizeof(d->r[0]));
                push((struct fcq *)d->q.recv_cq, w);
            }
        }
        if (status || (wr->send_flags & IBV_SEND_SIGNALED)) {
            struct ibv_wc w = { .wr_id = wr->wr_id, .status = status, .opcode = IBV_WC_RDMA_WRITE, .qp_num = s->q.qp_num };
            push((struct fcq *)s->q.send_cq, w);
        }
        if (status) { *bad = wr; s->q.state = IBV_QPS_ERR; return 0; }
    }
    return 0;
}

static int post_recv(struct ibv_qp *qp, struct ibv_recv_wr *wr, struct ibv_recv_wr **bad)
{
    struct fqp *q = (struct fqp *)qp;
    for (; wr; wr = wr->next) {
        if (q->nr == 64 || (wr->num_sge && !mr_by(wr->sg_list[0].lkey, 0, qp->context))) { *bad = wr; return EINVAL; }
        q->r[q->nr].wr_id = wr->wr_id;
        if (wr->num_sge) q->r[q->nr].sg = wr->sg_list[0];
        q->nr++;
    }
    return 0;
}

struct ibv_device **(ibv_get_device_list)(int *n) { if (n) *n = 2; return devs; }
void ibv_free_device_list(struct ibv_device **l) { (void)l; }
const char *ibv_get_device_name(struct ibv_device *d) { return d->name; }
struct ibv_context *ibv_open_device(struct ibv_device *d)
{
    struct ibv_context *c = calloc(1, sizeof(*c));
    c->device = d; c->async_fd = -1; c->cmd_fd = -1;
    c->ops.poll_cq = poll_cq; c->ops.post_send = post_send; c->ops.post_recv = post_recv;
    open_ctxs++;
    return c;
}
int ibv_close_device(struct ibv_context *c) { free(c); open_ctxs--; return 0; }
int ibv_get_async_event(struct ibv_context *c, struct ibv_async_event *e) { (void)c; (void)e; return -1; }
void ibv_ack_async_event(struct ibv_async_event *e) { (void)e; }
int ibv_query_device(struct ibv_context *c, struct ibv_device_attr *a) { (void)c; memset(a, 0, sizeof(*a)); return 0; }
int (ibv_query_port)(struct ibv_context *c, uint8_t p, struct _compat_ibv_port_attr *a)
{
    (void)c; (void)p;
    struct ibv_port_attr *x = (struct ibv_port_attr *)a;
    memset(x, 0, sizeof(*x));
    x->state = IBV_PORT_ACTIVE; x->lid = 1 + epoch; x->link_layer = IBV_LINK_LAYER_ETHERNET;
    return 0;
}
int ibv_query_gid(struct ibv_context *c, uint8_t p, int i, union ibv_gid *g)
{
    (void)p;
    if (i != 3) { memset(g, 0, sizeof(*g)); g->raw[0] = 0xfe; g->raw[1] = 0x80; g->raw[14] = di(c); g->raw[15] = i; return 0; }
    gid_of(epoch, di(c), g);
    return 0;
}
int ibv_query_pkey(struct ibv_context *c, uint8_t p, int i, __be16 *k) { (void)c; (void)p; (void)i; *k = 0xffff; return 0; }
int ibv_query_qp(struct ibv_qp *q, struct ibv_qp_attr *a, int m, struct ibv_qp_init_attr *ia)
{ (void)q; (void)a; (void)m; (void)ia; return 0; }
struct ibv_pd *ibv_alloc_pd(struct ibv_context *c) { struct ibv_pd *p = calloc(1, sizeof(*p)); p->context = c; return p; }
int ibv_dealloc_pd(struct ibv_pd *p) { free(p); return 0; }
struct ibv_mr *(ibv_reg_mr)(struct ibv_pd *pd, void *addr, size_t len, int access)
{
    (void)access;
    struct fmr *m = calloc(1, sizeof(*m));
    m->m.pd = pd; m->m.context = pd->context; m->m.addr = addr; m->m.length = len;
    int d = di(pd->context);
    m->m.lkey = 5000 + 1000 * epoch + counter[d]; m->m.rkey = 7000 + 1000 * epoch + counter[d]++;
    m->live = 1;
    mrs[n_mrs++] = m;
    return &m->m;
}
int ibv_dereg_mr(struct ibv_mr *m) { ((struct fmr *)m)->live = 0; return 0; }
struct ibv_cq *ibv_create_cq(struct ibv_context *c, int cqe, void *x, struct ibv_comp_channel *ch, int v)
{
    (void)ch; (void)v;
    struct fcq *q = calloc(1, sizeof(*q));
    q->c.context = c; q->c.cqe = cqe; q->c.cq_context = x; q->live = 1;
    return &q->c;
}
int ibv_destroy_cq(struct ibv_cq *c) { ((struct fcq *)c)->live = 0; return 0; }
struct ibv_qp *ibv_create_qp(struct ibv_pd *pd, struct ibv_qp_init_attr *a)
{
    struct fqp *q = calloc(1, sizeof(*q));
    q->q.context = pd->context; q->q.pd = pd; q->q.send_cq = a->send_cq; q->q.recv_cq = a->recv_cq;
    q->q.qp_num = 100 + 1000 * epoch + counter[di(pd->context)]++; q->q.state = IBV_QPS_RESET; q->q.qp_type = a->qp_type;
    q->live = 1;
    qps[n_qps++] = q;
    return &q->q;
}
int ibv_modify_qp(struct ibv_qp *qp, struct ibv_qp_attr *a, int mask)
{
    struct fqp *q = (struct fqp *)qp;
    if (mask & IBV_QP_DEST_QPN) q->dest = a->dest_qp_num;
    if (mask & IBV_QP_AV) q->dgid = a->ah_attr.grh.dgid, q->dlid = a->ah_attr.dlid, q->global = a->ah_attr.is_global;
    if (mask & IBV_QP_STATE) q->q.state = a->qp_state;
    return 0;
}
int ibv_destroy_qp(struct ibv_qp *q) { ((struct fqp *)q)->live = 0; return 0; }
int ibv_fork_init(void) { return 0; }
const char *ibv_event_type_str(enum ibv_event_type e) { (void)e; return "event"; }
"""

FAKE_MAP = r"""
IBVERBS_1.1 {
  global:
    ibv_get_device_list; ibv_free_device_list; ibv_get_device_name; ibv_open_device;
    ibv_close_device; ibv_get_async_event; ibv_ack_async_event; ibv_query_device;
    ibv_query_port; ibv_query_gid; ibv_query_pkey; ibv_query_qp; ibv_alloc_pd;
    ibv_dealloc_pd; ibv_reg_mr; ibv_dereg_mr; ibv_create_cq; ibv_destroy_cq;
    ibv_create_qp; ibv_modify_qp; ibv_destroy_qp; ibv_fork_init; ibv_event_type_str;
  local: *;
};
FAKE_1 { global: fake_open_contexts; fake_move; };
"""

# Uses verbs as NCCL does: dlopen the library NCCL_IBVERBS_LIB names,
# dlvsym every function at IBVERBS_1.1, and post and poll through the
# context's ops.
APP = r"""
#define _GNU_SOURCE
#include <dlfcn.h>
#include <infiniband/verbs.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define S(f) __typeof__(&f) p_##f = dlvsym(h, #f, "IBVERBS_1.1")

int main(int argc, char **argv)
{
    void *h = dlopen(argv[1], RTLD_NOW);
    if (!h) { printf("dlopen %s\n", dlerror()); return 2; }
    int (*fence)(char *, size_t) = dlsym(h, "nvsnap_ibv_fence");
    int (*suspend)(char *, size_t) = dlsym(h, "nvsnap_ibv_suspend");
    int (*restore)(char *, size_t) = dlsym(h, "nvsnap_ibv_restore");
    int (*map)(FILE *) = dlsym(h, "nvsnap_ibv_map");
    int (*connect)(const char *, char *, size_t) = dlsym(h, "nvsnap_ibv_connect");
    void (*open_)(void) = dlsym(h, "nvsnap_ibv_open");
    struct ibv_device **(*getl)(int *) = dlvsym(h, "ibv_get_device_list", "IBVERBS_1.1");
    S(ibv_open_device); S(ibv_alloc_pd); S(ibv_create_cq); S(ibv_create_qp);
    S(ibv_modify_qp); S(ibv_query_gid);
    struct ibv_mr *(*regmr)(struct ibv_pd *, void *, size_t, int) = dlvsym(h, "ibv_reg_mr", "IBVERBS_1.1");
    struct ibv_port_attr pa;
    int (*qport)(struct ibv_context *, uint8_t, struct ibv_port_attr *) = dlvsym(h, "ibv_query_port", "IBVERBS_1.1");

    int nd;
    struct ibv_device **l = getl(&nd);
    void *fake = dlopen("libibverbs.so.1", RTLD_NOW | RTLD_NOLOAD);  /* loaded by the library */
    if (!fake) { printf("the library did not load the real verbs\n"); return 2; }
    int (*open_ctxs)(void) = dlsym(fake, "fake_open_contexts");
    void (*move)(void) = dlsym(fake, "fake_move");
    /* The same objects on both devices: their numbers and keys collide. */
    enum { D = 2 };
    struct ibv_qp *a[D], *b[D];
    struct ibv_cq *cq[D];
    uint32_t qa[D], qb[D], rkey[D], lkey[D];
    static char src[D][64], dst[D][64];
    for (int d = 0; d < D; d++) {
        struct ibv_context *c = p_ibv_open_device(l[d]);
        struct ibv_pd *pd = p_ibv_alloc_pd(c);
        struct ibv_mr *ms = regmr(pd, src[d], sizeof(src[d]), 0), *md = regmr(pd, dst[d], sizeof(dst[d]), 7);
        cq[d] = p_ibv_create_cq(c, 64, NULL, NULL, 0);
        struct ibv_qp_init_attr ia = { .send_cq = cq[d], .recv_cq = cq[d], .qp_type = IBV_QPT_RC,
                                       .cap = { .max_send_wr = 16, .max_recv_wr = 16, .max_send_sge = 1, .max_recv_sge = 1 } };
        a[d] = p_ibv_create_qp(pd, &ia), b[d] = p_ibv_create_qp(pd, &ia);
        union ibv_gid gid;
        p_ibv_query_gid(c, 1, 3, &gid);
        qport(c, 1, &pa);
        /* What NCCL keeps and sends its peer: numbers, keys, GID and LID. */
        qa[d] = a[d]->qp_num, qb[d] = b[d]->qp_num, rkey[d] = md->rkey, lkey[d] = ms->lkey;
        struct ibv_qp *pair[2][2] = { { a[d], b[d] }, { b[d], a[d] } };
        for (int i = 0; i < 2; i++) {
            struct ibv_qp_attr x = { .qp_state = IBV_QPS_INIT, .port_num = 1 };
            p_ibv_modify_qp(pair[i][0], &x, IBV_QP_STATE | IBV_QP_PORT);
            struct ibv_qp_attr y = { .qp_state = IBV_QPS_RTR, .dest_qp_num = pair[i][1]->qp_num,
                                     .ah_attr = { .is_global = 1, .dlid = pa.lid, .port_num = 1,
                                                  .grh = { .dgid = gid, .sgid_index = 3 } } };
            p_ibv_modify_qp(pair[i][0], &y, IBV_QP_STATE | IBV_QP_AV | IBV_QP_DEST_QPN);
            struct ibv_qp_attr z = { .qp_state = IBV_QPS_RTS };
            p_ibv_modify_qp(pair[i][0], &z, IBV_QP_STATE);
        }
    }
    printf("same_numbers=%d\n", qa[0] == qa[1] && lkey[0] == lkey[1] && rkey[0] == rkey[1]);
    struct ibv_wc wc[8];
    int got;
    struct ibv_send_wr *wb;
    for (int round = 0; round < 2; round++) {
        for (int d = 0; d < D; d++) {
            /* A receive on b, posted now, completed by the write after the suspend. */
            struct ibv_recv_wr rw = { .wr_id = 40 + round }, *rb;
            if (b[d]->context->ops.post_recv(b[d], &rw, &rb)) { printf("post_recv failed\n"); return 1; }
            /* A signaled write from a nobody polls before the suspend. */
            struct ibv_sge sg0 = { (uintptr_t)src[d], 4, lkey[d] };
            struct ibv_send_wr w0 = { .wr_id = 7, .sg_list = &sg0, .num_sge = 1, .opcode = IBV_WR_RDMA_WRITE,
                                      .send_flags = IBV_SEND_SIGNALED, .wr = { .rdma = { (uintptr_t)dst[d] + 32, rkey[d] } } };
            if (a[d]->context->ops.post_send(a[d], &w0, &wb)) { printf("post_send failed\n"); return 1; }
        }
        char rep[256];
        /* Round 1 as gpushare does it: fence at "quiesce", then "release". */
        if (round == 1 && fence(rep, sizeof(rep))) { printf("fence: %s\n", rep); return 1; }
        if (suspend(rep, sizeof(rep))) { printf("suspend: %s\n", rep); return 1; }
        printf("round=%d suspended=%s open_after_suspend=%d\n", round, rep, open_ctxs());
        move();
        if (restore(rep, sizeof(rep))) { printf("restore: %s\n", rep); return 1; }
        char path[] = "/tmp/ibvmapXXXXXX";
        int fd = mkstemp(path);
        FILE *f = fdopen(fd, "w");
        int lines = map(f);
        fclose(f);
        if (connect(path, rep, sizeof(rep))) { printf("connect: %s\n", rep); return 1; }
        unlink(path);
        open_();
        printf("lines=%d\n", lines);
        for (int d = 0; d < D; d++) {
            /* NCCL's numbers and keys, from before. */
            snprintf(src[d], sizeof(src[d]), "hello%d%d", d, round);
            struct ibv_sge sg = { (uintptr_t)src[d], 8, lkey[d] };
            struct ibv_send_wr w = { .wr_id = 9, .sg_list = &sg, .num_sge = 1, .opcode = IBV_WR_RDMA_WRITE_WITH_IMM,
                                     .send_flags = IBV_SEND_SIGNALED, .imm_data = 77,
                                     .wr = { .rdma = { (uintptr_t)dst[d], rkey[d] } } };
            if (a[d]->context->ops.post_send(a[d], &w, &wb)) { printf("post_send after restore failed\n"); return 1; }
            int held = 0, wrote = 0, recvd = 0, bad = 0;
            for (int t = 0; t < 4 && (got = cq[d]->context->ops.poll_cq(cq[d], 8, wc)) >= 0; t++)
                for (int i = 0; i < got; i++) {
                    if (wc[i].status) { bad++; printf("wc status %d wr_id %lu\n", wc[i].status, (unsigned long)wc[i].wr_id); }
                    else if (wc[i].wr_id == 7 && wc[i].qp_num == qa[d]) held++;
                    else if (wc[i].wr_id == 9 && wc[i].qp_num == qa[d]) wrote++;
                    else if (wc[i].wr_id == (uint64_t)(40 + round) && wc[i].qp_num == qb[d] && wc[i].imm_data == 77) recvd++;
                    else { bad++; printf("unexpected wc wr_id %lu qp %u\n", (unsigned long)wc[i].wr_id, wc[i].qp_num); }
                }
            printf("held=%d wrote=%d recvd=%d bad=%d data=%s\n", held, wrote, recvd, bad,
                   strncmp(dst[d], src[d], 8) ? "wrong" : "ok");
        }
    }
    return 0;
}
"""


class Ibv(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not shutil.which("gcc") or not os.path.exists(os.path.join(IBV_INC, "infiniband/verbs.h")) or \
                not os.path.exists(os.path.join(CUDA_INC, "cuda.h")):
            raise unittest.SkipTest("needs gcc, infiniband/verbs.h (IBV_INC) and cuda.h (CUDA_INC)")
        cls.tmp = tempfile.mkdtemp()
        t = cls.tmp
        for name, text in (("fake.c", FAKE), ("fake.map", FAKE_MAP), ("app.c", APP)):
            with open(os.path.join(t, name), "w") as f:
                f.write(text)
        inc = ["-I", IBV_INC]
        subprocess.run(["gcc", "-shared", "-fPIC", *inc, "-Wl,--version-script=" + os.path.join(t, "fake.map"),
                        "-o", os.path.join(t, "libibverbs.so.1"), os.path.join(t, "fake.c")], check=True)
        subprocess.run(["gcc", "-O2", "-Wall", "-Wextra", "-Werror", "-fPIC", "-shared", "-fvisibility=hidden", *inc,
                        "-I", CUDA_INC, "-Wl,--version-script=" + os.path.join(SRC, "nvsnap_ibv.map"),
                        "-o", os.path.join(t, "libnvsnap_ibv.so"), os.path.join(SRC, "nvsnap_ibv.c"),
                        "-ldl", "-lpthread"], check=True)
        subprocess.run(["gcc", "-O2", "-Wall", "-Werror", *inc, "-o", os.path.join(t, "app"), os.path.join(t, "app.c"), "-ldl"], check=True)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def test_restore_on_another_nic(self):
        p = subprocess.run([os.path.join(self.tmp, "app"), os.path.join(self.tmp, "libnvsnap_ibv.so")],
                           capture_output=True, text=True, timeout=60,
                           env=dict(os.environ, LD_LIBRARY_PATH=self.tmp))
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        rounds = [dict(kv.split("=", 1) for kv in l.split() if "=" in kv) for l in p.stdout.splitlines()]
        for r in (0, 1):  # two checkpoints in a row
            sus = [x for x in rounds if x.get("round") == str(r)][0]
            self.assertEqual(sus["open_after_suspend"], "0", p.stdout)
        self.assertIn("same_numbers=1", p.stdout)  # two NICs, the same keys and queue pair numbers
        done = [x for x in rounds if "held" in x]
        self.assertEqual(len(done), 4, p.stdout + p.stderr)  # 2 rounds x 2 devices
        for d in done:
            self.assertEqual(d, {"held": "1", "wrote": "1", "recvd": "1", "bad": "0", "data": "ok"}, p.stdout + p.stderr)
        moved = [x for x in rounds if "lines" in x]
        self.assertGreaterEqual(int(moved[0]["lines"]), 10, p.stdout)  # per device: a port, 2 keys, 2 queue pairs


if __name__ == "__main__":
    unittest.main(verbosity=2)
