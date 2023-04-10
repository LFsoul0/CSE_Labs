#include "tx_region.h"


int tx_region::put(const int key, const int val) {
    printf("tx[%d] put (%d, %d)\n", tx_id, key, val);

    chdb_protocol::operation_var var{};
    var.tx_id = tx_id;
    var.key = key;
    var.value = val;

    int r;
    this->db->vserver->execute(key, chdb_protocol::Put, var, r);
    return r;
}

int tx_region::get(const int key) {
    printf("tx[%d] get %d\n", tx_id, key);

    chdb_protocol::operation_var var{};
    var.tx_id = tx_id;
    var.key = key;

    int r;
    this->db->vserver->execute(key, chdb_protocol::Get, var, r);
    return r;
}

int tx_region::tx_can_commit() {
    chdb_protocol::prepare_var var{};
    var.tx_id = tx_id;
    int r = 0;

    // broadcast
    int baseport = db->vserver->node->port();
    for (int offset = 1; offset < db->vserver->shard_num(); ++offset) {
        db->vserver->node->call(baseport + offset, chdb_protocol::Prepare, var, r);
        if (!r) {
            return chdb_protocol::prepare_not_ok;
        }
    }
    return chdb_protocol::prepare_ok;
}

int tx_region::tx_begin() {
    printf("tx[%d] begin\n", tx_id);

    db->mtx.lock();

    return 0;
}

int tx_region::tx_commit() {
    printf("tx[%d] commit\n", tx_id);

    chdb_protocol::commit_var var{};
    var.tx_id = tx_id;
    int r = 0;

    // broadcast
    int baseport = db->vserver->node->port();
    for (int offset = 1; offset < db->vserver->shard_num(); ++offset) {
        db->vserver->node->call(baseport + offset, chdb_protocol::Commit, var, r);
    }

    db->mtx.unlock();

    return 0;
}

int tx_region::tx_abort() {
    printf("tx[%d] abort\n", tx_id);

    chdb_protocol::rollback_var var{};
    var.tx_id = tx_id;
    int r = 0;

    // broadcast
    int baseport = db->vserver->node->port();
    for (int offset = 1; offset < db->vserver->shard_num(); ++offset) {
        db->vserver->node->call(baseport + offset, chdb_protocol::Rollback, var, r);
    }

    db->mtx.unlock();

    return 0;
}
