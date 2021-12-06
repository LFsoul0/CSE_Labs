#include "raft_state_machine.h"


kv_command::kv_command() : kv_command(CMD_NONE, "", "") { }

kv_command::kv_command(command_type tp, const std::string &key, const std::string &value) : 
    cmd_tp(tp), key(key), value(value), res(std::make_shared<result>())
{
    res->start = std::chrono::system_clock::now();
    res->key = key;
}

kv_command::kv_command(const kv_command &cmd) :
    cmd_tp(cmd.cmd_tp), key(cmd.key), value(cmd.value), res(cmd.res) {}

kv_command::~kv_command() { }

int kv_command::size() const {
    return key.size() + value.size() + 11; // type(char) + key_size(int) + val_size(int) + key(+'\0') + val(+'\0')
}


void kv_command::serialize(char* buf, int size) const {
    if (size < this->size()) return;

    int key_size = key.size() + 1, val_size = value.size() + 1;
    buf[0] = (char)cmd_tp;
    for (int i = 0; i < 4; ++i) {
        buf[i + 1] = (key_size >> 8 * (3 - i)) & 0xff;
    }
    for (int i = 0; i < 4; ++i) {
        buf[i + 5] = (val_size >> 8 * (3 - i)) & 0xff;
    }

    strcpy(buf + 9, key.data());
    strcpy(buf + key_size + 9, value.data());

    return;
}

void kv_command::deserialize(const char* buf, int size) {
    if (size < 9) return;

    int key_size = 0, val_size = 0;
    cmd_tp = (command_type)buf[0];
    for (int i = 0; i < 4; ++i) {
        key_size |= (buf[i + 1] & 0xff) << 8 * (3 - i);
    }
    for (int i = 0; i < 4; ++i) {
        val_size |= (buf[i + 5] & 0xff) << 8 * (3 - i);
    }

    if (size < key_size + val_size + 9) return;
    key.assign(buf + 9);
    value.assign(buf + key_size + 9);

    return;
}

marshall& operator<<(marshall &m, const kv_command& cmd) {
    m << (char)cmd.cmd_tp;
    m << cmd.key;
    m << cmd.value;
    return m;
}

unmarshall& operator>>(unmarshall &u, kv_command& cmd) {
    char tp = 0;
    u >> tp;
    cmd.cmd_tp = (kv_command::command_type)tp;
    u >> cmd.key;
    u >> cmd.value;
    return u;
}

kv_state_machine::~kv_state_machine() {

}

void kv_state_machine::apply_log(raft_command &cmd) {
    std::unique_lock<std::mutex> lock(mtx);
    kv_command &kv_cmd = dynamic_cast<kv_command&>(cmd);
    std::unique_lock<std::mutex> res_lock(kv_cmd.res->mtx);

    std::string val;
    bool succ = true;
    switch (kv_cmd.cmd_tp) {
        case kv_command::CMD_NONE: {
            // do nothing
            break;
        }
        case kv_command::CMD_GET: {
            auto it = store.find(kv_cmd.key);
            succ = it != store.end();
            if (succ) {
                val = it->second;
            }
            else {
                val = "";
            }
            break;
        }
        case kv_command::CMD_PUT: {
            auto ret = store.insert(std::pair<std::string, std::string>(kv_cmd.key, kv_cmd.value));
            succ = ret.second;
            if (succ) {
                val = kv_cmd.value;
            }
            else {
                val = ret.first->second; // old value
                store.erase(ret.first);
                store.insert(std::pair<std::string, std::string>(kv_cmd.key, kv_cmd.value)); // check failure?
            }
            break;
        }
        case kv_command::CMD_DEL: {
            auto it = store.find(kv_cmd.key);
            succ = it != store.end();
            if (succ) {
                val = it->second; // old value
                store.erase(it);
            }
            else {
                val = "";
            }
            break;
        }
    }

    // printf("apply cmd[type %d, key %s, val %s] res[succ %d, val %s]\n", kv_cmd.cmd_tp, kv_cmd.key.data(), kv_cmd.value.data(), succ, val.data());
    kv_cmd.res->done = true;
    kv_cmd.res->succ = succ;
    kv_cmd.res->key = kv_cmd.key;
    kv_cmd.res->value = val;
    kv_cmd.res->cv.notify_all();
    return;
}

std::vector<char> kv_state_machine::snapshot() {
    std::unique_lock<std::mutex> lock(mtx);
    std::vector<char> snapshot;
    std::stringstream ss;
    ss << (int)store.size() << '\n';
    for (auto& kv : store) {
        ss << kv.first << '\n';
        ss << kv.second << '\n';
    }
    std::string str = ss.str();
    snapshot.assign(str.begin(), str.end());
    return snapshot;
}

void kv_state_machine::apply_snapshot(const std::vector<char>& snapshot) {
    std::unique_lock<std::mutex> lock(mtx);
    std::string str(snapshot.begin(), snapshot.end());
    std::stringstream ss(str);
    int size = 0;
    ss >> size;
    ss.get();
    for (int i = 0; i < size; ++i) {
        std::pair<std::string, std::string> kv;
        std::getline(ss, kv.first);
        std::getline(ss, kv.second);
        store.insert(kv);
    }
    return;    
}
