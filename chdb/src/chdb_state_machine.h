#include "rpc.h"
#include "raft_state_machine.h"

#include <vector>
#include <memory>
#include <chrono>
#include <atomic>
#include <condition_variable>


class chdb_command : public raft_command {
public:
    enum command_type {
        CMD_NONE = 0xdead,   // Do nothing
        CMD_GET,        // Get a key-value pair
        CMD_PUT,        // Put a key-value pair
    };

    struct result {
        std::chrono::system_clock::time_point start;
        int key, value, tx_id;
        command_type tp;

        bool done;
        std::mutex mtx; // protect the struct
        std::condition_variable cv; // notify the caller
    };

    chdb_command();

    chdb_command(command_type tp, const int &key, const int &value, const int &tx_id);

    chdb_command(const chdb_command &cmd);

    virtual ~chdb_command() {}


    command_type cmd_tp;
    int key, value, tx_id;
    std::shared_ptr<result> res;


    virtual int size() const override {
        return 13; // cmd_tp(char) + tx_id + key + val
    }

    virtual void serialize(char *buf, int size) const override;

    virtual void deserialize(const char *buf, int size);
};

marshall &operator<<(marshall &m, const chdb_command &cmd);

unmarshall &operator>>(unmarshall &u, chdb_command &cmd);

class chdb_state_machine : public raft_state_machine {
public:
    virtual ~chdb_state_machine() {}

    // Apply a log to the state machine.
    virtual void apply_log(raft_command &cmd) override;

    // Generate a snapshot of the current state.
    // In Chdb, you don't need to implement this function
    virtual std::vector<char> snapshot() {
        return std::vector<char>();
    }

    // Apply the snapshot to the state mahine.
    // In Chdb, you don't need to implement this function
    virtual void apply_snapshot(const std::vector<char> &) {}
};