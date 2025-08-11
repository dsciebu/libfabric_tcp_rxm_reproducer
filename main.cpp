#include <cstring>

#include <charconv>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <source_location>
#include <sstream>
#include <stdexcept>
#include <string>
#include <sys/types.h>
#include <sys/wait.h>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <unistd.h>

extern "C" {
#include <rdma/fabric.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_eq.h>
#include <rdma/fi_errno.h>
#include <rdma/fi_rma.h>
#include <rdma/fi_tagged.h>
}

sockaddr_in addressExchangeServerAddr;
int addressExchangeServerFd;

namespace {

fi_info *makeHints(bool useRXM) {

  fi_info *hints = fi_allocinfo();
  if (!hints) {
    throw std::runtime_error("Hints allocation failed");
  }

  hints->domain_attr->threading = FI_THREAD_SAFE;
  hints->domain_attr->mr_mode =
      FI_MR_LOCAL | FI_MR_ALLOCATED | FI_MR_PROV_KEY | FI_MR_VIRT_ADDR;
  hints->domain_attr->name = nullptr;
  hints->fabric_attr->prov_name = strdup("tcp");

  hints->domain_attr->resource_mgmt = FI_RM_ENABLED;
  hints->ep_attr->type = FI_EP_RDM;
  // hints->ep_attr->type = FI_EP_MSG;
  if (useRXM)
    hints->ep_attr->protocol = FI_PROTO_RXM;
  // hints->ep_attr->protocol = FI_PROTO_UNSPEC;
  hints->addr_format = FI_FORMAT_UNSPEC;
  hints->dest_addr = nullptr;
  hints->domain_attr->control_progress = FI_PROGRESS_AUTO;
  hints->domain_attr->data_progress = FI_PROGRESS_AUTO;
  hints->mode = FI_CONTEXT;
  hints->caps = FI_MSG | FI_RMA | FI_TAGGED | FI_SOURCE | FI_DIRECTED_RECV;

  return hints;
}

fi_cq_attr initCqAttrDefault() {
  fi_cq_attr attr{};
  // VERY important setting - without that the sync data sent with fi_(...)data
  // instructions (like fi_writedata) is not formatted properly and cannot be
  // read properly
  attr.format = FI_CQ_FORMAT_TAGGED;
  // These 2 settings are needed to use fi_cq_sread
  attr.wait_obj = FI_WAIT_UNSPEC;
  attr.wait_cond = FI_CQ_COND_NONE;
  return attr;
}

fi_av_attr initAvAttrDefault() {
  fi_av_attr avAttr{};
  std::memset(&avAttr, 0, sizeof(fi_av_attr));
  avAttr.type = FI_AV_MAP;
  return avAttr;
}

auto getAddr(fid *endpoint) {
  std::size_t addrLen = 0;
  auto ret = fi_getname(endpoint, nullptr, &addrLen);
  if ((ret != -FI_ETOOSMALL) || (addrLen <= 0)) {
    throw std::runtime_error("Unexpected error");
  }

  auto data = std::vector<char>(addrLen, 0);

  ret = fi_getname(endpoint, static_cast<void *>(data.data()), &addrLen);
  if (ret) {
    throw std::runtime_error("Unexpected error");
  }

  data.shrink_to_fit();
  return data;
}

void CHECK(int ret, const std::source_location location =
                        std::source_location::current()) {
  if (ret) {
    std::ostringstream oss;
    oss << "Check failed file: " << location.file_name() << "("
        << location.line() << ":" << location.column() << ") `"
        << location.function_name() << "`: " << fi_strerror(ret);

    throw std::runtime_error(oss.str());
  }
}

void serverHandleClient(int clientSocket,
                        std::vector<std::vector<char>> &clientAddr) {
  int clientId;
  char addrBuffer[256];

  recv(clientSocket, &clientId, sizeof(clientId), 0);
  recv(clientSocket, addrBuffer, sizeof(addrBuffer), 0);

  static std::mutex vecMutex;
  std::lock_guard<std::mutex> lock(vecMutex);
  if (clientId >= 0 && clientId < clientAddr.size()) {
    std::memcpy(clientAddr[clientId].data(), addrBuffer, sizeof(addrBuffer));
    // printf("Client %d address stored: %s\n", clientId, addrBuffer);
  }
  close(clientSocket);
}

void clientSendAddress(int rank, const std::vector<char> &clientAddress) {
  int clientSock = socket(AF_INET, SOCK_STREAM, 0);
  if (clientSock < 0) {
    perror("Socket creation failed");
    exit(EXIT_FAILURE);
  }

  while (connect(clientSock, (struct sockaddr *)&addressExchangeServerAddr,
                 sizeof(addressExchangeServerAddr)) < 0) {
    // printf("Rank #%d connection to server failed", rank);
    sleep(1);
  }

  // printf("Client %d connected to server on port %d...\n", rank,
  //        ntohs(addressExchangeServerAddr.sin_port));
  send(clientSock, &rank, sizeof(rank), 0);
  send(clientSock, clientAddress.data(), 256, 0);
  // printf("Client %d sent address: %s.\n", rank, clientAddress.data());

  close(clientSock);
}

void createAddressExchangeServer() {
  int addrlen = sizeof(addressExchangeServerAddr);

  addressExchangeServerFd = socket(AF_INET, SOCK_STREAM, 0);
  addressExchangeServerAddr.sin_family = AF_INET;
  addressExchangeServerAddr.sin_addr.s_addr = INADDR_ANY;
  addressExchangeServerAddr.sin_port = 0;

  bind(addressExchangeServerFd, (struct sockaddr *)&addressExchangeServerAddr,
       sizeof(addressExchangeServerAddr));

  socklen_t len = sizeof(addressExchangeServerAddr);
  if (getsockname(addressExchangeServerFd,
                  (struct sockaddr *)&addressExchangeServerAddr, &len) < 0) {
    perror("getsockname");
    close(addressExchangeServerFd);
    exit(1);
  }
}

void serverExchangeAddresses(const std::vector<char> &serverAddr,
                             std::vector<std::vector<char>> &clientAddr) {
  int addrlen = sizeof(addressExchangeServerAddr);
  listen(addressExchangeServerFd, clientAddr.size());

  // printf("Server listening on port %d...\n",
  for (const auto &c : clientAddr) {
    int client_fd = accept(addressExchangeServerFd,
                           (struct sockaddr *)&addressExchangeServerAddr,
                           (socklen_t *)&addrlen);
    serverHandleClient(client_fd, clientAddr);
  }
  printf("serverExchangeAddresses DONE\n");

  close(addressExchangeServerFd);
}

void printInputs(size_t clientsCnt, bool useRXM) {
  printf("INPUTS: \n");
  printf("number of client processes = %ld\n", clientsCnt);
  printf("use RXM utility provider = %d\n", useRXM);
  printf("\n");
}

} // namespace

int main(int argc, char *argv[]) {
  if (argc < 3) {
    std::cout << "Usage: " << argv[0]
              << " <number_of_client_processes> <use_RXM>" << std::endl;
    return 1;
  }
  size_t clientsCnt;
  std::from_chars(argv[1], argv[1] + std::strlen(argv[1]), clientsCnt);
  int tmpVal;
  std::from_chars(argv[2], argv[2] + std::strlen(argv[1]), tmpVal);
  const bool useRXM = tmpVal != 0;

  printInputs(clientsCnt, useRXM);

  static constexpr size_t bufferSize{sizeof(int)};
  static constexpr size_t iterations{1000};

  std::vector<char> serverAddr{};
  std::vector<std::vector<char>> clientAddr(clientsCnt, std::vector<char>(256));

  fi_info *hints = makeHints(useRXM);
  fi_info *info = nullptr;
  fid_fabric *fabric = nullptr;
  fid_domain *domain = nullptr;
  fid_ep *ep = nullptr;
  fid_cq *rxCq = nullptr;
  fid_cq *txCq = nullptr;
  fid_av *av = nullptr;

  auto rxCqAttr = initCqAttrDefault();
  auto txCqAtrr = initCqAttrDefault();
  auto avAttr = initAvAttrDefault();

  CHECK(fi_getinfo(fi_version(), nullptr, nullptr, 0, hints, &info));
  CHECK(fi_fabric(info->fabric_attr, &fabric, nullptr));
  CHECK(fi_domain(fabric, info, &domain, nullptr));
  CHECK(fi_endpoint(domain, info, &ep, nullptr));
  CHECK(fi_cq_open(domain, &rxCqAttr, &rxCq, nullptr));
  CHECK(fi_cq_open(domain, &txCqAtrr, &txCq, nullptr));
  CHECK(fi_av_open(domain, &avAttr, &av, nullptr));

  CHECK(fi_ep_bind(ep, &txCq->fid, FI_TRANSMIT));
  CHECK(fi_ep_bind(ep, &rxCq->fid, FI_RECV));
  CHECK(fi_ep_bind(ep, &av->fid, 0));
  CHECK(fi_enable(ep));

  const auto serverBufferSize = bufferSize * clientsCnt;
  auto serverBuffer = aligned_alloc(16, serverBufferSize);
  auto sendBuffer = aligned_alloc(16, bufferSize);
  std::memset(sendBuffer, 1, bufferSize);

  std::size_t PP_MR_KEY = 0xC0DE;
  fid_mr *mr = nullptr;
  fid_mr *mrSend = nullptr;
  const auto flags = FI_SEND | FI_RECV | FI_REMOTE_READ | FI_REMOTE_WRITE;
  fi_mr_reg(domain, serverBuffer, serverBufferSize, flags, 0, PP_MR_KEY, 0, &(mr),
            nullptr);
  fi_mr_reg(domain, sendBuffer, bufferSize, flags, 0, PP_MR_KEY+1, 0, &(mrSend),
            nullptr);

  const auto localAddr = getAddr(&ep->fid);
  fi_addr_t localFiAddr = 0;
  if (info->domain_attr->caps & FI_LOCAL_COMM) {
    CHECK(fi_av_insert(av, localAddr.data(), 1, &localFiAddr, 0, nullptr) < 0);
  }
  serverAddr = localAddr;

  // START SERVER thread in the main process:
  createAddressExchangeServer();
  std::thread server([&] {
    std::vector<fi_addr_t> remoteFiAddr(clientsCnt);

    // After creating the libfabric endpoint, we need to share the
    serverExchangeAddresses(serverAddr, clientAddr);
    for (size_t c = 0; c < clientsCnt; c++) {
      CHECK(fi_av_insert(av, clientAddr[c].data(), 1, &remoteFiAddr[c], 0,
                         nullptr) < 0);
    }

    for (size_t it = 0; it < iterations; it++) {
      std::memset(serverBuffer, 0, serverBufferSize);

      for (size_t c = 0; c < clientsCnt; c++) {
        // printf("SERVER fi_recv\n");
        // Each client 'writes' to its one cell in the server buffer
        const auto tag = c;
        while (fi_send(ep, sendBuffer, bufferSize, fi_mr_desc(mrSend),
                       remoteFiAddr[c], // tag, 0ULL,
                       nullptr)) {
        };
        while (fi_recv(ep, static_cast<int *>(serverBuffer) + c, bufferSize,
                        fi_mr_desc(mr), remoteFiAddr[c], //tag, 0ULL,
                        nullptr)) {
        };

        // auto ret = fi_cq_readfrom(rxCq, &comp, 1, &comp.src_addr);
        // printf("SERVER fi_cq_readfrom from #%ld\n", c);
        fi_cq_err_entry comp{};
        while (1) {
          const auto ret = fi_cq_readfrom(txCq, &comp, 1, &comp.src_addr);
          if (ret == 1)
            break;
          if (ret == -FI_EAGAIN || ret == -FI_EINTR)
            // printf("SERVER FI_EAGAIN fi_cq_readerr from #%ld\n", c);
            continue;
          if (ret == -FI_EAVAIL) {
            // printf("SERVER FI_EAVAIL fi_cq_readerr from #%ld\n", c);
            fi_cq_readerr(rxCq, &comp, 0);
            continue;
          }
          [[unlikely]] if (ret < 0) {
            printf("SERVER Error in rea from #%ld\n", c);
            throw std::runtime_error("Cq wait unexpected error");
          }
        };
        while (1) {
          const auto ret = fi_cq_readfrom(rxCq, &comp, 1, &comp.src_addr);
          if (ret == 1)
            break;
          if (ret == -FI_EAGAIN || ret == -FI_EINTR)
            // printf("SERVER FI_EAGAIN fi_cq_readerr from #%ld\n", c);
            continue;
          if (ret == -FI_EAVAIL) {
            // printf("SERVER FI_EAVAIL fi_cq_readerr from #%ld\n", c);
            fi_cq_readerr(rxCq, &comp, 0);
            continue;
          }
          [[unlikely]] if (ret < 0) {
            printf("SERVER Error in rea from #%ld\n", c);
            throw std::runtime_error("Cq wait unexpected error");
          }
        };
      }

      // Check
      for (int c = 0; c < clientsCnt; c++) {
        if ((static_cast<int *>(serverBuffer))[c] != c + it) {
          std::string err("Mismatch at iter:" + std::to_string(it) +
                          ", client:" + std::to_string(c) +
                          ". Expected: " + std::to_string(c + it) + ", got:" +
                          std::to_string(static_cast<int *>(serverBuffer)[c]) + ".");
          printf("ERR: %s\n", err.c_str());
        }
      }
    }

    // Prevent closing the server before all the transmissions are done
    sleep(5);

    fi_close(&mr->fid);
    fi_close(&mrSend->fid);
    free(serverBuffer);
    fi_close(&ep->fid);
    fi_close(&av->fid);
    fi_close(&rxCq->fid);
    fi_close(&txCq->fid);
    fi_close(&domain->fid);
    fi_close(&fabric->fid);
    fi_freeinfo(info);
    fi_freeinfo(hints);
  });

  // Client body:
  const auto clientLam = ([&](int rank) {
    fi_info *hints = makeHints(useRXM);
    hints->addr_format = FI_FORMAT_UNSPEC;
    hints->dest_addr = nullptr;
    fi_info *info = nullptr;
    fid_fabric *fabric = nullptr;
    fid_domain *domain = nullptr;
    fid_ep *ep = nullptr;
    fid_cq *rxCq = nullptr;
    fid_cq *txCq = nullptr;
    fid_av *av = nullptr;
    auto rxCqAttr = initCqAttrDefault();
    auto txCqAtrr = initCqAttrDefault();
    auto avAttr = initAvAttrDefault();

    // printf("CLIENT #%d START\n", rank);
    CHECK(fi_getinfo(fi_version(), nullptr, nullptr, 0, hints, &info));
    CHECK(fi_fabric(info->fabric_attr, &fabric, nullptr));
    CHECK(fi_domain(fabric, info, &domain, nullptr));
    CHECK(fi_endpoint(domain, info, &ep, nullptr));
    CHECK(fi_cq_open(domain, &rxCqAttr, &rxCq, nullptr));
    CHECK(fi_cq_open(domain, &txCqAtrr, &txCq, nullptr));
    CHECK(fi_av_open(domain, &avAttr, &av, nullptr));
    CHECK(fi_ep_bind(ep, &txCq->fid, FI_TRANSMIT));
    CHECK(fi_ep_bind(ep, &rxCq->fid, FI_RECV));
    CHECK(fi_ep_bind(ep, &av->fid, 0));
    CHECK(fi_enable(ep));

    auto clientBuffer = aligned_alloc(16, bufferSize);
    auto recvBuffer = aligned_alloc(16, bufferSize);
    std::size_t PP_MR_KEY = 0xC0DE;
    fid_mr *mr = nullptr;
    fid_mr *mrRecv = nullptr;
    auto flags = FI_SEND | FI_RECV;
    CHECK(fi_mr_reg(domain, clientBuffer, bufferSize, flags, 0, PP_MR_KEY, 0, &(mr),
                    nullptr));
    const auto localAddr = getAddr(&ep->fid);
    CHECK(fi_mr_reg(domain, recvBuffer, bufferSize, flags, 0, PP_MR_KEY+1, 0,
                    &(mrRecv), nullptr));

    clientAddr[rank] = localAddr;
    clientSendAddress(rank, clientAddr[rank]);

    fi_addr_t localFiAddr = 0;
    if (info->domain_attr->caps & FI_LOCAL_COMM) {
      auto ret =
          fi_av_insert(av, localAddr.data(), 1, &localFiAddr, 0, nullptr);
      CHECK(ret < 0);
      CHECK(ret != 1);
    }

    fi_addr_t remoteFiAddr = 0;
    CHECK(fi_av_insert(av, serverAddr.data(), 1, &remoteFiAddr, 0, nullptr) <
          0);

    const auto tag = static_cast<size_t>(rank);
    for (size_t it = 0; it < iterations; it++) {
      *static_cast<int *>(clientBuffer) = rank + it;

      // printf("CLIENT #%d fi_send\n", rank);
      while (fi_send(ep, clientBuffer, bufferSize, fi_mr_desc(mr), remoteFiAddr, //tag,
      nullptr) != 0) {
      };
      while (fi_recv(ep, recvBuffer, bufferSize, fi_mr_desc(mrRecv), remoteFiAddr, //tag,
                      nullptr) != 0) {
      };

      fi_cq_err_entry comp{};

      while (fi_cq_readfrom(txCq, &comp, 1, &comp.src_addr) < 0) {
        // printf("CLIENT #%d fi_cq_readfrom\n", rank);
      };
      while (fi_cq_readfrom(rxCq, &comp, 1, &comp.src_addr) < 0) {
        // printf("CLIENT #%d fi_cq_readfrom\n", rank);
      };
      // printf("CLIENT #%d comp.flags=%lld comp.src_addr=%ld\n", rank,
      //        comp.flags & FI_SEND, comp.src_addr);
    }

    // Prevent closing the client before all the transmissions are done
    sleep(5);

    fi_close(&mr->fid);
    fi_close(&mrRecv->fid);
    free(clientBuffer);
    fi_close(&ep->fid);
    fi_close(&av->fid);
    fi_close(&rxCq->fid);
    fi_close(&txCq->fid);
    fi_close(&domain->fid);
    fi_close(&fabric->fid);
    fi_freeinfo(info);
    hints->dest_addr = nullptr;
    fi_freeinfo(hints);
  });

  // Create client processes:
  std::vector<pid_t> clients(clientsCnt);
  for (size_t i = 0; i < clientsCnt; i++) {
    clients[i] = fork();
    if (clients[i] == -1) {
      throw std::runtime_error("Failed to fork process");
    } else if (clients[i] == 0) {
      // Child process
      try {
        clientLam(i);
      } catch (const std::exception &e) {
        std::cerr << "Error in process " << i << ": " << e.what() << "\n";
        exit(1);
      }
      exit(0);
    }
  }

  // Wait for all child processes to complete and check their exit status
  for (auto rank = 0LU; rank < clientsCnt; rank++) {
    int status{-1};
    const pid_t pid = waitpid(clients[rank], &status, 0);
    if (pid == -1) {
      const auto killProcesses = [&](size_t firstIdx) {
        for (auto r = firstIdx; r < clientsCnt; r++) {
          kill(clients[r], SIGTERM); // NOLINT(misc-include-cleaner)
        }
      };
      killProcesses(rank);
      throw std::runtime_error("Failed to wait for child process");
    }
  }

  server.join();
}
