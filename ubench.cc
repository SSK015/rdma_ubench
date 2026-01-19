#include <gflags/gflags.h>
#include <infiniband/verbs.h>
#include <spdlog/spdlog.h>
#include <unistd.h>
#include <zmq.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <future>
#include <iostream>
#include <memory>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "client.h"
#include "ib.h"
#include "log.h"
#include "server.h"
#include "zmq_helper.h"

DEFINE_string(zmq_ip, "10.156.112.101", "ZeroMQ server IP");
DEFINE_uint32(zmq_port, 7889, "ZeroMQ server port.");
DEFINE_string(ib_dev, "mlx5_0", "InfiniBand device");
DEFINE_uint32(ib_port, 1, "InfiniBand port");
DEFINE_bool(server, false, "Is server");
DEFINE_uint32(server_threads, 32, "Number of server threads");
DEFINE_uint32(client_threads, 16, "Number of client threads");
DEFINE_uint32(msg_size, 64, "Message size (byte)");
DEFINE_uint32(batch, 16, "Number of requests in a batch");
DEFINE_bool(read, false, "Do read instead of write");
DEFINE_uint32(sec, 5, "Benchmark duration (seconds)");
DEFINE_uint32(num_sge, 1, "Number of scatter-gather elements");

typedef unsigned long long cycles_t;
static inline cycles_t get_cycles()
{
	unsigned low, high;
	unsigned long long val;
	asm volatile ("rdtsc" : "=a" (low), "=d" (high));
	val = high;
	val = (val << 32) | low;
	return val;
}

static unsigned int Offset(unsigned int server_id, unsigned int client_id,
                           unsigned int slot) {
  return (server_id * FLAGS_client_threads + client_id) * FLAGS_batch + slot;
}

static void Server() {
  size_t buf_size = 2 * FLAGS_server_threads * FLAGS_client_threads *
                    FLAGS_batch * FLAGS_msg_size * 2;
  auto buf = new volatile char[buf_size];
  std::fill(buf, buf + buf_size, '\0');
  IbContext ib_ctx(FLAGS_ib_dev, FLAGS_ib_port, buf, buf_size);
  ServerContext server_ctx(ib_ctx);
  server_ctx.Init(FLAGS_server_threads, FLAGS_client_threads, FLAGS_zmq_ip,
                  FLAGS_zmq_port);
  std::this_thread::sleep_for(std::chrono::seconds(FLAGS_sec + 2));
}

void ClientThread(ClientContext &ctx, unsigned int id, std::atomic_bool &stop,
                  std::promise<uint64_t> prm) {
  auto qp_ctx = ctx.req_qps_.at(id);
  ibv_sge sge[FLAGS_batch][FLAGS_num_sge];
  for (unsigned int i = 0; i < FLAGS_batch; i++) {
    std::memset(sge[i], 0, FLAGS_num_sge * sizeof(ibv_sge));
  }

  ibv_send_wr *wr = new ibv_send_wr[FLAGS_batch];
  ibv_send_wr *bad_wr;
  std::memset(wr, 0, FLAGS_batch * sizeof(ibv_send_wr));
  uint64_t comps = 0;
  std::minstd_rand gen(42);
  std::uniform_int_distribution<unsigned int> dis(0, FLAGS_server_threads - 1);
  std::vector<unsigned int> next_slot(FLAGS_server_threads);

  for (unsigned int i = 0; i < FLAGS_batch; i++) {
    for (unsigned int j = 0; j < FLAGS_num_sge; j++) {
      sge[i][j].length = FLAGS_msg_size;
      sge[i][j].lkey = ctx.ib_ctx_.mr_->lkey;
    }
    wr[i].wr_id = i;
    wr[i].sg_list = sge[i];
    wr[i].num_sge = FLAGS_num_sge;
    wr[i].opcode = FLAGS_read ? IBV_WR_RDMA_READ : IBV_WR_RDMA_WRITE;
    // wr[i].send_flags = FLAGS_read ? 0 : IBV_SEND_INLINE;
    wr[i].next = &wr[i + 1];
    wr[i].wr.rdma.rkey = qp_ctx->remote_.rkey;
  }
  wr[FLAGS_batch - 1].next = nullptr;
  wr[FLAGS_batch - 1].send_flags |= IBV_SEND_SIGNALED;

  int loop_count = 0;
  while (!stop.load(std::memory_order_acquire)) {
    for (unsigned int i = 0; i < FLAGS_batch; i++) {
      for (unsigned int j = 0; j < FLAGS_num_sge; j++) {
      auto target = dis(gen);
      auto slot = next_slot.at(target)++;
      if (next_slot[target] == FLAGS_batch) {
        next_slot[target] = 0;
      }
      auto offset = Offset(target, id, slot);
      sge[i][j].addr = reinterpret_cast<uintptr_t>(qp_ctx->ib_ctx_.buf_ + offset * FLAGS_msg_size);
      }
    
      wr[i].wr.rdma.remote_addr = reinterpret_cast<uintptr_t>(qp_ctx->remote_.addr);
      loop_count++;
    int rc = ibv_post_send(qp_ctx->qp_, wr, &bad_wr);
    RequireZero(rc, "ibv_post_send failed with rc: " + std::to_string(rc) + " and i : " + std::to_string(loop_count));
    ibv_wc wc;
    std::memset(&wc, 0, sizeof(ibv_wc));
    int ne = 0;
    while (ne == 0) {
      ne = ibv_poll_cq(qp_ctx->cq_, FLAGS_batch, &wc);
    }

    if (ne > 1) {
      spdlog::error("{}", ne);
    }
    if (wc.status != IBV_WC_SUCCESS) {
      spdlog::error("Bad wc status {:d}: {}", wc.status,
                    ibv_wc_status_str(wc.status));
    }
    comps += FLAGS_batch * FLAGS_num_sge;
  }

  spdlog::info("Client thread {} completed", id);

  prm.set_value(comps);
}

static void Client() {
  size_t buf_size = 2 * FLAGS_server_threads * FLAGS_client_threads *
                    FLAGS_batch * FLAGS_msg_size;
  auto buf = new volatile char[buf_size];
  std::fill(buf, buf + buf_size, '\0');
  IbContext ib_ctx(FLAGS_ib_dev, FLAGS_ib_port, buf, buf_size);
  ClientContext client_ctx(ib_ctx);
  client_ctx.Init(FLAGS_server_threads, FLAGS_client_threads, FLAGS_zmq_ip,
                  FLAGS_zmq_port);

  std::atomic_bool stop = ATOMIC_VAR_INIT(false);
  std::vector<std::thread> threads;
  std::vector<std::future<uint64_t>> futs;
  for (unsigned int i = 0; i < FLAGS_client_threads; i++) {
    std::promise<uint64_t> prm;
    spdlog::info("Client thread {} started !!!", i);
    futs.push_back(prm.get_future());
    threads.emplace_back(ClientThread, std::ref(client_ctx), i, std::ref(stop),
                         std::move(prm));
    
  }

  std::this_thread::sleep_for(std::chrono::seconds(FLAGS_sec));
  // uint64_t start_cycle = get_cycles();
  // volatile int i = 0;
  // while (i < 10000) {
  //   uint64_t end_cycle = get_cycles();
  //   if (end_cycle - start_cycle > (uint64_t(1'000'000) * 2500 * FLAGS_sec)) {
  //     // spdlog::info("{}", i);
  //     break;
  //   }
  //   i = i + 1;
  // }
  stop.store(true, std::memory_order_release);
  spdlog::info("Stopping client threads");
  for (auto &t : threads) {
    t.join();
  }
  uint64_t comps = 0;
  for (auto &fut : futs) {
    comps += fut.get();
  }
  std::string op_str = FLAGS_read ? "read" : "write";
  spdlog::info("{} {}/s", comps / FLAGS_sec, op_str);
}

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  if (FLAGS_batch > kMaxSendWr || FLAGS_batch > kMaxRecvWr) {
    spdlog::error("Batch size {} should not exceed kMaxSendWr and kMaxRecvWr");
    std::exit(EXIT_FAILURE);
  }
  if (FLAGS_server) {
    spdlog::info("Running as server");
    Server();
  } else {
    spdlog::info("Running as client");
    Client();
  }
  return 0;
}
