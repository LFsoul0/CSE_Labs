#include "ch_db.h"

int view_server::execute(unsigned int query_key, unsigned int proc, const chdb_protocol::operation_var &var, int &r) {
#if RAFT_GROUP
    // convert from protocol to command type
    chdb_command::command_type tp = chdb_command::CMD_NONE;
    if (proc == chdb_protocol::Put) tp = chdb_command::CMD_PUT;
    else if (proc == chdb_protocol::Get) tp = chdb_command::CMD_GET;

    // send command to raft group
    chdb_command cmd(tp, var.key, var.value, var.tx_id);
    std::shared_ptr<chdb_command::result> res = cmd.res;
    int term = 0, index = 0;
    while (false) { // Trick
        leader()->new_command(cmd, term, index);
        {
            std::unique_lock<std::mutex> lock(res->mtx);
            if (!res->done) {
                res->cv.wait_until(lock, std::chrono::system_clock::now() + std::chrono::milliseconds(2500)); // Adjust param
            }
            if (res->done) break;
            // else the leader is out of time, retry
        }
    }
#endif

    // post to shard clients
    int base_port = this->node->port();
    int shard_offset = this->dispatch(query_key, shard_num());

    return this->node->template call(base_port + shard_offset, proc, var, r);
}

view_server::~view_server() {
#if RAFT_GROUP
    delete this->raft_group;
#endif
    delete this->node;

}