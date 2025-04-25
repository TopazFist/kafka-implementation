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
#include <assert.h>
#include <iomanip>

const size_t k_max_msg = 4096;

struct Request_header{
    int16_t request_api_key;
    int16_t request_api_version;
    int32_t correlation_id;
};


struct Request_message{
    int32_t message_size;
    Request_header header;
 };


static void msg(const char *msg) {
   std::cerr << msg << std::endl;
}


static int32_t read_full(int fd, char *buf, size_t n) {
   while (n > 0) {
       ssize_t rv = read(fd, buf, n);
       if (rv <= 0) {
           return -1;  // error, or unexpected EOF
       }
       assert((size_t)rv <= n);
       n -= (size_t)rv;
       buf += rv;
   }
   return 0;
}


static int32_t parse_request_message(int client_fd, Request_message &request ) {
    char rbuf[4 + k_max_msg]; //header
    errno = 0;

    if (read_full(client_fd, rbuf, 4) != 0) {
        msg("Error reading message size");
        return -1;
    }

    uint32_t message_size_net = 0;
    memcpy(&message_size_net, rbuf, 4);
    request.message_size = message_size_net;

    uint32_t message_size = ntohl(message_size_net);

    if (message_size > k_max_msg) {
        msg("too long");
        return -1;
    }
    

    // Read full request body
    if (read_full(client_fd, &rbuf[4], message_size) != 0) {
        msg("read() error");
        return -1;
    }

    // Parse request_api_key
    uint16_t api_key_net = 0;
    memcpy(&api_key_net, rbuf + 4, 2);
    request.header.request_api_key = (api_key_net);

    // Parse request_api_version
    uint16_t version_net = 0;
    memcpy(&version_net, rbuf + 6, 2);
    request.header.request_api_version = (version_net);

    // Parse correlation_id
    uint32_t corr_id_net = 0;
    memcpy(&corr_id_net, rbuf + 8, 4);
    request.header.correlation_id = (corr_id_net);
    
    return true;
}


int main(int argc, char* argv[]) {
    // Disable output buffering
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "Failed to create server socket: " << std::endl;
        return 1;
    }

    // Since the tester restarts your program quite often, setting SO_REUSEADDR
    // ensures that we don't run into 'Address already in use' errors
    int reuse = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        close(server_fd);
        std::cerr << "setsockopt failed: " << std::endl;
        return 1;
    }

    struct sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(9092);

    if (bind(server_fd, reinterpret_cast<struct sockaddr*>(&server_addr), sizeof(server_addr)) != 0) {
        close(server_fd);
        std::cerr << "Failed to bind to port 9092" << std::endl;
        return 1;
    }

    int connection_backlog = 5;
    if (listen(server_fd, connection_backlog) != 0) {
        close(server_fd);
        std::cerr << "listen failed" << std::endl;
        return 1;
    }

    std::cout << "Waiting for a client to connect...\n";

    struct sockaddr_in client_addr{};
    socklen_t client_addr_len = sizeof(client_addr);

    // You can use print statements as follows for debugging, they'll be visible when running tests.
    std::cerr << "Logs from your program will appear here!\n";
    
    // Uncomment this block to pass the first stage
    // 
    int client_fd = accept(server_fd, reinterpret_cast<struct sockaddr*>(&client_addr), &client_addr_len);
    std::cout << "Client connected\n";


    Request_message req;

    if (parse_request_message(client_fd, req)) {
        std::cout << "message_size: " << req.message_size << "\n";
        std::cout << "api_key: " << req.header.request_api_key << "\n";
        std::cout << "api_version: " << req.header.request_api_version << "\n";
        std::cout << "correlation_id: " << req.header.correlation_id << "\n";
        
        if (req.header.request_api_version < 0 || req.header.request_api_version > 4) {
            int32_t error_code = htons(35);

            char wbuf[10];

            memcpy(wbuf, &req.message_size, 4);
            memcpy(wbuf + 4, &req.header.correlation_id, 4);
            memcpy(wbuf + 8, &error_code, 2);

            write(client_fd, wbuf, sizeof(wbuf));
            
        }
    }

    close(client_fd);

    close(server_fd);
    return 0;
}