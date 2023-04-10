#include "shard_client.h"


int shard_client::put(chdb_protocol::operation_var var, int &r) {
    // generate undo log entry (get old value)
    log_entry undo_entry(var.tx_id, var.key, value_entry());
    auto it = store[primary_replica].find(var.key);
    bool exist = it != store[primary_replica].end();
    if (exist) {
        undo_entry.val = it->second; // old value
    }
    else {
        undo_entry.create = true; // no old value
    }

    // append undo log
    undo_logs.push_back(undo_entry);

    // put to store (replace)
    std::pair<int, value_entry> entry(var.key, value_entry(var.value));
    for (auto& s : store) {
        if (exist) {
            s.erase(var.key);
        }
        s.insert(entry);
    }
    
    r = 0; // no fail?
    return 0; 
}

int shard_client::get(chdb_protocol::operation_var var, int &r) {
    // get from primary
    auto it = store[primary_replica].find(var.key);
    bool exist = it != store[primary_replica].end();
    if (exist) {
        r = it->second.value;
    }
    else {
        r = -1; // other error code?
    }

    return 0;
}

int shard_client::commit(chdb_protocol::commit_var var, int &r) {

    remove_tx(var.tx_id);

    return 0;
}

int shard_client::rollback(chdb_protocol::rollback_var var, int &r) {
    // rollback store
    for (auto it = undo_logs.rbegin(); it != undo_logs.rend(); ++it) {
        if (it->tx_id == var.tx_id) {
            for (auto& s : store) {
                s.erase(it->key);
                if (!it->create) {
                    s.insert(std::pair<int, value_entry>(it->key, it->val));
                }
            }
        }
    }

    remove_tx(var.tx_id);

    return 0;
}

int shard_client::check_prepare_state(chdb_protocol::check_prepare_state_var var, int &r) {
    auto it = prepare_states.find(var.tx_id);
    if (it != prepare_states.end()) {
        r = it->second;
    }
    else {
        r = chdb_protocol::prepare_not_ok; // other error code?
    }

    return 0;
}

int shard_client::prepare(chdb_protocol::prepare_var var, int &r) {
    // check active
    if (active) {
        r = true;
    }
    else {
        bool readonly = true;
        for (log_entry& log : undo_logs) {
            if (log.tx_id == var.tx_id) {
                readonly = false;
                break;
            }
        }
        r = readonly;
    }

    // save prepare state
    auto ret = prepare_states.insert(std::pair<int, int>(var.tx_id, r));
    if (!ret.second) {
        prepare_states.erase(ret.first);
        prepare_states.insert(std::pair<int, int>(var.tx_id, r));
    }

    return 0;
}

shard_client::~shard_client() {
    delete node;
}

void shard_client::remove_tx(int tx_id)
{
    // delete log
    for (auto it = undo_logs.begin(); it != undo_logs.end(); ) {
        if (it->tx_id == tx_id) {
            it = undo_logs.erase(it);
        }
        else {
            ++it;
        }
    }

    // remove prepare state?
}