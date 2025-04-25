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

const size_t k_max_msg = 4096;

struct Request_message{
    int32_t message_size;
    int32_t correlation_id;
    // Request_header header;
};

static void msg(const char *msg) {
    std::cerr << msg << std::endl;

}
// struct Request_header{

// };

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

static int32_t do_something(int client_fd){
    char rbuf[4 + k_max_msg];
    errno = 0;

    int32_t err = read_full(client_fd, rbuf, 4);

    if (err != 0) {
        msg("Error reading message size");
        return -1;
    }
    
    uint32_t message_size = 0;
    memcpy(&message_size, rbuf, 4);
    message_size = ntohl(message_size);
    std::cout << "Message size: " << message_size << std::endl;


    if (message_size > k_max_msg) {
        msg("too long");
        return -1;
    }

    err = read_full(client_fd, &rbuf[4], 8);
    if (err) {
        msg("read() error");
        return err;
    }


    uint32_t correlation_id;
    memcpy(&correlation_id, &rbuf[4+4], 4);
    correlation_id = ntohl(correlation_id);
    std::cout << "correlation_id: " << correlation_id << std::endl;

    for (int i = 0; i < 13; ++i) {
        printf("%02x ", static_cast<unsigned char>(rbuf[i]));
    }
    printf("\n");

    write(client_fd, &message_size, sizeof(message_size));
    write(client_fd, &correlation_id, sizeof(correlation_id));
    return 0;
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

    do_something(client_fd);
    

    // int32_t message_size = htonl(0);  // No payload
    // int32_t correlation_id = htonl(7);

    // write(client_fd, &message_size, sizeof(message_size));
    // write(client_fd, &correlation_id, sizeof(correlation_id));

    close(client_fd);

    close(server_fd);
    return 0;
}