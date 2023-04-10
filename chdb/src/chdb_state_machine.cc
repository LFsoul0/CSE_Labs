#include "chdb_state_machine.h"

chdb_command::chdb_command() : chdb_command(CMD_NONE, 0, 0, 0) { }

chdb_command::chdb_command(command_type tp, const int &key, const int &value, const int &tx_id)
        : cmd_tp(tp), key(key), value(value), tx_id(tx_id), res(std::make_shared<result>()) 
{
    res->start = std::chrono::system_clock::now();
    res->tp = tp;
    res->key = key;
    res->tx_id = tx_id;
}

chdb_command::chdb_command(const chdb_command &cmd) :
        cmd_tp(cmd.cmd_tp), key(cmd.key), value(cmd.value), tx_id(cmd.tx_id), res(cmd.res) { }


void chdb_command::serialize(char* buf, int size) const {
    if (size < this->size()) return;

    // type
    buf[0] = (char)cmd_tp;
    // tx_id
    for (int i = 0; i < 4; ++i) {
        buf[i + 1] = (tx_id >> 8 * (3 - i)) & 0xff;
    }
    // key
    for (int i = 0; i < 4; ++i) {
        buf[i + 5] = (key >> 8 * (3 - i)) & 0xff;
    }
    // value
    for (int i = 0; i < 4; ++i) {
        buf[i + 9] = (value >> 8 * (3 - i)) & 0xff;
    }

    return;
}

void chdb_command::deserialize(const char *buf, int size) {
    if (size < this->size()) return;

    // type
    cmd_tp = (command_type)buf[0];
    // tx_id
    for (int i = 0; i < 4; ++i) {
        tx_id |= (buf[i + 1] & 0xff) << 8 * (3 - i);
    }
    // key
    for (int i = 0; i < 4; ++i) {
        key |= (buf[i + 5] & 0xff) << 8 * (3 - i);
    }
    // value
    for (int i = 0; i < 4; ++i) {
        value |= (buf[i + 9] & 0xff) << 8 * (3 - i);
    }

    return;
}

marshall &operator<<(marshall &m, const chdb_command &cmd) {
    m << (char)cmd.cmd_tp;
    m << cmd.tx_id;
    m << cmd.key;
    m << cmd.value;
    return m;
}

unmarshall &operator>>(unmarshall &u, chdb_command &cmd) {
    char tp = 0;
    u >> tp;
    cmd.cmd_tp = (chdb_command::command_type)tp;
    u >> cmd.tx_id;
    u >> cmd.key;
    u >> cmd.value;
    return u;
}

void chdb_state_machine::apply_log(raft_command &cmd) {
    chdb_command& chdb_cmd = dynamic_cast<chdb_command&>(cmd);
    std::unique_lock<std::mutex> res_lock(chdb_cmd.res->mtx);

    // set result done
    chdb_cmd.res->tp = chdb_cmd.cmd_tp;
    chdb_cmd.res->tx_id = chdb_cmd.tx_id;
    chdb_cmd.res->key = chdb_cmd.key;
    chdb_cmd.res->value = chdb_cmd.value;
    chdb_cmd.res->done = true;
    chdb_cmd.res->cv.notify_all();
    return;
}