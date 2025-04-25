#include <cstdlib>
#include <cstring>
#include <iostream>
#include <netdb.h>
#include <string>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <stdint.h>
#include <cassert>
#include <iomanip>

constexpr size_t k_max_msg = 4096;
constexpr int response_msg_size = 23;

struct Request_header {
    int16_t request_api_key;
    int16_t request_api_version;
    int32_t correlation_id;
};

struct Response_body {
    int16_t be_error_code;
    uint8_t api_keys_length = 0x02; 
    int16_t be_api_key = htons(18);
    int16_t be_min_version = htons(0);
    int16_t be_max_version = htons(4);
    int32_t be_throttle_time_ms = htonl(0);
    uint8_t no_tags = 0x00;
    uint8_t api_key_tags = 0x00;
};

struct Request_message {
    int32_t message_size;
    Request_header header;
};

struct Response_message {
    int32_t message_size = htonl(response_msg_size-4);
    int32_t correlation_id;
    Response_body header;
};

void log(const char* msg) {
    std::cerr << msg << std::endl;
}

int32_t read_full(int fd, char* buf, size_t n) {
    while (n > 0) {
        ssize_t rv = read(fd, buf, n);
        if (rv <= 0) return -1;
        assert(static_cast<size_t>(rv) <= n);
        n -= static_cast<size_t>(rv);
        buf += rv;
    }
    return 0;
}

bool parse_request_message(int client_fd, Request_message& request) {
    char rbuf[4 + k_max_msg];
    if (read_full(client_fd, rbuf, 4) != 0) {
        log("Error reading message size");
        return false;
    }

    uint32_t message_size_net = 0;
    memcpy(&message_size_net, rbuf, 4);
    request.message_size = ntohl(message_size_net);

    if (request.message_size > k_max_msg) {
        log("Message too long");
        return false;
    }

    if (read_full(client_fd, &rbuf[4], request.message_size) != 0) {
        log("Error reading full request");
        return false;
    }

    memcpy(&request.header.request_api_key, rbuf + 4, 2);
    memcpy(&request.header.request_api_version, rbuf + 6, 2);
    memcpy(&request.header.correlation_id, rbuf + 8, 4);

    request.header.request_api_key = ntohs(request.header.request_api_key);
    request.header.request_api_version = ntohs(request.header.request_api_version);
    request.header.correlation_id = ntohl(request.header.correlation_id);

    return true;
}

void send_response(int client_fd, const Request_message& req) {
    Response_message res;
    res.correlation_id = htonl(req.header.correlation_id);
    res.header.be_error_code = htons(
        (req.header.request_api_version >= 0 && req.header.request_api_version <= 4) ? 0 : 35
    );

    char wbuf[response_msg_size];
    memcpy(wbuf, &res.message_size, 4);
    memcpy(wbuf + 4, &res.correlation_id, 4);
    memcpy(wbuf + 8, &res.header.be_error_code, 2);
    memcpy(wbuf + 10, &res.header.api_keys_length, 1);
    memcpy(wbuf + 11, &res.header.be_api_key, 2);
    memcpy(wbuf + 13, &res.header.be_min_version, 2);
    memcpy(wbuf + 15, &res.header.be_max_version, 2);
    memcpy(wbuf + 17, &res.header.be_throttle_time_ms, 4);
    memcpy(wbuf + 21, &res.header.no_tags, 1);
    memcpy(wbuf + 22, &res.header.api_key_tags, 1);

    write(client_fd, wbuf, sizeof(wbuf));

    std::cout << "Sent Response (Hex): ";
    for (int i = 0; i < sizeof(wbuf); ++i) {
        std::cout << std::hex << std::setw(2) << std::setfill('0')
                  << static_cast<uint32_t>(static_cast<uint8_t>(wbuf[i])) << " ";
    }
    std::cout << std::dec << std::endl;
}

int main() {
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) return perror("socket"), 1;

    int reuse = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0)
        return perror("setsockopt"), close(server_fd), 1;

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(9092);

    if (bind(server_fd, reinterpret_cast<sockaddr*>(&server_addr), sizeof(server_addr)) != 0)
        return perror("bind"), close(server_fd), 1;

    if (listen(server_fd, 5) != 0)
        return perror("listen"), close(server_fd), 1;

    std::cout << "Waiting for a client to connect...\n";
    sockaddr_in client_addr{};
    socklen_t client_len = sizeof(client_addr);

    int client_fd = accept(server_fd, reinterpret_cast<sockaddr*>(&client_addr), &client_len);
    if (client_fd < 0)
        return perror("accept"), close(server_fd), 1;

    std::cout << "Client connected\n";

    Request_message req;
    if (parse_request_message(client_fd, req)) {
        std::cout << "message_size: " << req.message_size << "\n";
        std::cout << "api_key: " << req.header.request_api_key << "\n";
        std::cout << "api_version: " << req.header.request_api_version << "\n";
        std::cout << "correlation_id: " << req.header.correlation_id << "\n";
        send_response(client_fd, req);
    }

    close(client_fd);
    close(server_fd);
    return 0;
}
