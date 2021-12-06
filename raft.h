#ifndef raft_h
#define raft_h

#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <ctime>
#include <algorithm>
#include <thread>
#include <random>
#include <stdarg.h>

#include "rpc.h"
#include "raft_storage.h"
#include "raft_protocol.h"
#include "raft_state_machine.h"

using std::chrono::system_clock;

template<typename state_machine, typename command>
class raft {

static_assert(std::is_base_of<raft_state_machine, state_machine>(), "state_machine must inherit from raft_state_machine");
static_assert(std::is_base_of<raft_command, command>(), "command must inherit from raft_command");


friend class thread_pool;

#define RAFT_LOG(fmt, args...) \
    do { \
        auto now = \
        std::chrono::duration_cast<std::chrono::milliseconds>(\
            std::chrono::system_clock::now().time_since_epoch()\
        ).count();\
        printf("[%ld][%s:%d][node %d term %d fl %d bl %d c %d a %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, log.front().index, last_log_index(), commit_index, last_applied, ##args); \
    } while(0);


#ifndef max
#define max(a,b) (((a) > (b)) ? (a) : (b))
#endif

#ifndef min
#define min(a,b) (((a) < (b)) ? (a) : (b))
#endif

public:
    raft(
        rpcs* rpc_server,
        std::vector<rpcc*> rpc_clients,
        int idx, 
        raft_storage<command>* storage,
        state_machine* state    
    );
    ~raft();

    // start the raft node.
    // Please make sure all of the rpc request handlers have been registered before this method.
    void start();

    // stop the raft node. 
    // Please make sure all of the background threads are joined in this method.
    // Notice: you should check whether is server should be stopped by calling is_stopped(). 
    //         Once it returns true, you should break all of your long-running loops in the background threads.
    void stop();

    // send a new command to the raft nodes.
    // This method returns true if this raft node is the leader that successfully appends the log.
    // If this node is not the leader, returns false. 
    bool new_command(command cmd, int &term, int &index);

    // returns whether this node is the leader, you should also set the current term;
    bool is_leader(int &term);

    // save a snapshot of the state machine and compact the log.
    bool save_snapshot();

private:
    std::mutex mtx;                     // A big lock to protect the whole data structure
    ThrPool* thread_pool;
    raft_storage<command>* storage;              // To persist the raft log
    state_machine* state;  // The state machine that applies the raft log, e.g. a kv store

    rpcs* rpc_server;               // RPC server to recieve and handle the RPC requests
    std::vector<rpcc*> rpc_clients; // RPC clients of all raft nodes including this node
    int my_id;                     // The index of this node in rpc_clients, start from 0

    std::atomic_bool stopped;

    enum raft_role {
        follower,
        candidate,
        leader
    };
    raft_role role; // volatile
    int current_term; // persist

    std::thread* background_election;
    std::thread* background_ping;
    std::thread* background_commit;
    std::thread* background_apply;

    // persist states
    int vote_for;
    std::vector<log_entry<command>> log;
    std::vector<char> snapshot;

    // volatile states
    int commit_index;
    int last_applied;

    // candidate volatile states
    int vote_count;
    std::vector<bool> vote_for_me;

    // leader volatile states
    std::vector<int> next_index;
    std::vector<int> match_index;
    std::vector<int> match_count;

    // times
    system_clock::time_point last_received_RPC_time;
    system_clock::duration follower_election_timeout;
    system_clock::duration candidate_election_timeout;

private:
    // RPC handlers
    int request_vote(request_vote_args arg, request_vote_reply& reply);

    int append_entries(append_entries_args<command> arg, append_entries_reply& reply);

    int install_snapshot(install_snapshot_args arg, install_snapshot_reply& reply);

    // RPC helpers
    void send_request_vote(int target, request_vote_args arg);
    void handle_request_vote_reply(int target, const request_vote_args& arg, const request_vote_reply& reply);

    void send_append_entries(int target, append_entries_args<command> arg);
    void handle_append_entries_reply(int target, const append_entries_args<command>& arg, const append_entries_reply& reply);

    void send_install_snapshot(int target, install_snapshot_args arg);
    void handle_install_snapshot_reply(int target, const install_snapshot_args& arg, const install_snapshot_reply& reply);


private:
    bool is_stopped();
    int num_nodes() {return rpc_clients.size();}

    // background workers    
    void run_background_ping();
    void run_background_election();
    void run_background_commit();
    void run_background_apply();

    // helper funcs (use within lock!)
    void generate_timeouts();
    inline int last_log_term();
    inline int last_log_index();
    inline int get_log_term_by_index(int index);
    void log_trunc(int end_index);
    void log_trunc_until(int end_index);
    void get_entries(int begin_index, int end_index, std::vector<log_entry<command>>& ret);
    void become_follower(int term);
    void become_candidate();
    void become_leader();
    void heartbeat();

};

template<typename state_machine, typename command>
raft<state_machine, command>::raft(rpcs* server, std::vector<rpcc*> clients, int idx, raft_storage<command> *storage, state_machine *state) :
    storage(storage),
    state(state),   
    rpc_server(server),
    rpc_clients(clients),
    my_id(idx),
    stopped(false),
    role(follower),
    current_term(0),
    background_election(nullptr),
    background_ping(nullptr),
    background_commit(nullptr),
    background_apply(nullptr)
{
    thread_pool = new ThrPool(32);

    // persist states, recover from storage
    if (!storage->restore(current_term, vote_for, log, snapshot)) {
        current_term = 0;
        vote_for = -1;
        log.assign(1, log_entry<command>(0, 0));
        snapshot.clear();

        storage->flush_all(current_term, vote_for, log, snapshot);
    }
    if (!snapshot.empty()) {
        state->apply_snapshot(snapshot);
    }

    // volatile states
    commit_index = log.front().index;
    last_applied = log.front().index;

    // candidate volatile states
    vote_count = 0;
    vote_for_me.assign(num_nodes(), false);

    // leader volatile states
    next_index.assign(num_nodes(), 1);
    match_index.assign(num_nodes(), 0);
    match_count.clear();

    // times
    last_received_RPC_time = system_clock::now();
    generate_timeouts();

    // Register the rpcs.
    rpc_server->reg(raft_rpc_opcodes::op_request_vote, this, &raft::request_vote);
    rpc_server->reg(raft_rpc_opcodes::op_append_entries, this, &raft::append_entries);
    rpc_server->reg(raft_rpc_opcodes::op_install_snapshot, this, &raft::install_snapshot);
    
}

template<typename state_machine, typename command>
raft<state_machine, command>::~raft() {
    if (background_ping) {
        delete background_ping;
    }
    if (background_election) {
        delete background_election;
    }
    if (background_commit) {
        delete background_commit;
    }
    if (background_apply) {
        delete background_apply;
    }
    delete thread_pool;    
}

/******************************************************************

                        Public Interfaces

*******************************************************************/

template<typename state_machine, typename command>
void raft<state_machine, command>::stop() {
    RAFT_LOG("stop");
    stopped.store(true);
    background_ping->join();
    background_election->join();
    background_commit->join();
    background_apply->join();
    thread_pool->destroy();
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::is_stopped() {
    return stopped.load();
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::is_leader(int &term) {
    term = current_term;
    return role == leader;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::start() {
    RAFT_LOG("start");
    this->background_election = new std::thread(&raft::run_background_election, this);
    this->background_ping = new std::thread(&raft::run_background_ping, this);
    this->background_commit = new std::thread(&raft::run_background_commit, this);
    this->background_apply = new std::thread(&raft::run_background_apply, this);
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::new_command(command cmd, int &term, int &index) {
    std::unique_lock<std::mutex> lock(mtx);
    if (role != leader) return false;

    term = current_term;
    index = last_log_index() + 1; // next_index[my_id]

    log_entry<command> entry(index, term, cmd);
    log.push_back(entry);
    next_index[my_id] = index + 1;
    match_index[my_id] = index;
    match_count.push_back(1);

    if (!storage->append_log(entry, log.size())) {
        storage->flush_log(log);
    }

    RAFT_LOG("new command %d", index);
    return true;
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::save_snapshot() {
    std::unique_lock<std::mutex> lock(mtx);

    snapshot = state->snapshot();
    log_trunc_until(last_applied);

    storage->flush_snapshot(snapshot);
    storage->flush_log(log);

    RAFT_LOG("snapshot");
    return true;
}



/******************************************************************

                         RPC Related

*******************************************************************/
template<typename state_machine, typename command>
int raft<state_machine, command>::request_vote(request_vote_args arg, request_vote_reply& reply) {
    std::unique_lock<std::mutex> lock(mtx);
    last_received_RPC_time = system_clock::now();
    reply.term = current_term;
    reply.voteGranted = false;

    if (arg.term < current_term) {
        return 0;
    }

    if (arg.term > current_term) {
        become_follower(arg.term);
    }
    if (vote_for == -1 || vote_for == arg.candidateId) {
        if (arg.lastLogTerm > last_log_term() || (arg.lastLogTerm == last_log_term() && arg.lastLogIndex >= last_log_index())) {
            vote_for = arg.candidateId;
            reply.voteGranted = true;

            storage->flush_metadata(current_term, vote_for);
        }
    }

    return 0;
}


template<typename state_machine, typename command>
void raft<state_machine, command>::handle_request_vote_reply(int target, const request_vote_args& arg, const request_vote_reply& reply) {
    std::unique_lock<std::mutex> lock(mtx);

    if (reply.term > current_term) {
        become_follower(reply.term);
        return;
    }
    if (role != candidate) {
        return;
    }

    if (reply.voteGranted && !vote_for_me[target]) {
        vote_for_me[target] = true;
        ++vote_count;

        if (vote_count > num_nodes() / 2) {
            become_leader();
        }
    }

    return;
}


template<typename state_machine, typename command>
int raft<state_machine, command>::append_entries(append_entries_args<command> arg, append_entries_reply& reply) {
    std::unique_lock<std::mutex> lock(mtx);
    last_received_RPC_time = system_clock::now();
    reply.term = current_term;
    reply.success = false;
    
    if (arg.term < current_term) {
        return 0;
    }

    if (arg.term > current_term || role == candidate) {
        become_follower(arg.term);
    }
    if (arg.prevLogIndex <= last_log_index() && arg.prevLogTerm == get_log_term_by_index(arg.prevLogIndex)) {
        if (!arg.entries.empty()) {
            if (arg.prevLogIndex < last_log_index()) {
                log_trunc(arg.prevLogIndex + 1);
                log.insert(log.end(), arg.entries.begin(), arg.entries.end());
                storage->flush_log(log);
            }
            else {
                log.insert(log.end(), arg.entries.begin(), arg.entries.end());
                if (!storage->append_log(arg.entries, log.size())) {
                    storage->flush_log(log);
                }
            }
        }

        if (arg.leaderCommit > commit_index) {
            commit_index = min(arg.leaderCommit, last_log_index());
        }

        reply.success = true;
    }

    return 0;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::handle_append_entries_reply(int target, const append_entries_args<command>& arg, const append_entries_reply& reply) {
    std::unique_lock<std::mutex> lock(mtx);

    if (reply.term > current_term) {
        become_follower(reply.term);
        return;
    }
    if (role != leader) {
        return;
    }

    if (reply.success) {
        int prev_match_index = match_index[target];
        match_index[target] = max(match_index[target], (int)(arg.prevLogIndex + arg.entries.size()));
        next_index[target] = match_index[target] + 1;

        // check commit
        int end_count_index = max(prev_match_index - commit_index, 0) - 1;
        for (int i = match_index[target] - commit_index - 1; i > end_count_index; --i) {
            ++match_count[i];
            if (match_count[i] > num_nodes() / 2 && get_log_term_by_index(commit_index + i + 1) == current_term) {
                commit_index += i + 1;
                match_count.erase(match_count.begin(), match_count.begin() + i + 1);
                break;
            }
        }
    }
    else {
        next_index[target] = min(next_index[target], arg.prevLogIndex);
    }

    return;
}


template<typename state_machine, typename command>
int raft<state_machine, command>::install_snapshot(install_snapshot_args arg, install_snapshot_reply& reply) {
    std::unique_lock<std::mutex> lock(mtx);
    last_received_RPC_time = system_clock::now();
    reply.term = current_term;

    if (arg.term < current_term) {
        return 0;
    }

    if (arg.term > current_term || role == candidate) {
        become_follower(arg.term);
    }

    if (arg.lastIncludedIndex <= last_log_index() && arg.lastIncludedTerm == get_log_term_by_index(arg.lastIncludedIndex)) {
        log_trunc_until(arg.lastIncludedIndex);
    }
    else {
        log.assign(1, log_entry<command>(arg.lastIncludedIndex, arg.lastIncludedTerm));
    }
    snapshot = arg.snapshot;
    state->apply_snapshot(snapshot);

    last_applied = arg.lastIncludedIndex;
    commit_index = max(commit_index, arg.lastIncludedIndex);

    storage->flush_log(log);
    storage->flush_snapshot(arg.snapshot);

    return 0;
}


template<typename state_machine, typename command>
void raft<state_machine, command>::handle_install_snapshot_reply(int target, const install_snapshot_args& arg, const install_snapshot_reply& reply) {
    std::unique_lock<std::mutex> lock(mtx);

    if (reply.term > current_term) {
        become_follower(reply.term);
        return;
    }
    if (role != leader) {
        return;
    }

    match_index[target] = max(match_index[target], arg.lastIncludedIndex);
    next_index[target] = match_index[target] + 1;

    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_request_vote(int target, request_vote_args arg) {
    request_vote_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_request_vote, arg, reply) == 0) {
        handle_request_vote_reply(target, arg, reply);
    } else {
        // RPC fails
        // send_request_vote(target, arg); // retry?
    }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_append_entries(int target, append_entries_args<command> arg) {
    append_entries_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_append_entries, arg, reply) == 0) {
        handle_append_entries_reply(target, arg, reply);
    } else {
        // RPC fails
        // send_append_entries(target, arg); // retry?
    }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_install_snapshot(int target, install_snapshot_args arg) {
    install_snapshot_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_install_snapshot, arg, reply) == 0) {
        handle_install_snapshot_reply(target, arg, reply);
    } else {
        // RPC fails
        // send_install_snapshot(target, arg); // retry?
    }
}

/******************************************************************

                        Background Workers

*******************************************************************/

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_election() {
    std::unique_lock<std::mutex> lock(mtx, std::defer_lock);
    system_clock::time_point current_time;

    while (true) {
        if (is_stopped()) return;

        lock.lock();
        current_time = system_clock::now();

        switch (role)
        {
        case follower:
            if (current_time - last_received_RPC_time > follower_election_timeout) {
                become_candidate();
            }
            break;
        case candidate:
            if (current_time - last_received_RPC_time > candidate_election_timeout) {
                become_candidate(); 
            }
            break;
        case leader:
            // do nothing
            break;
        }

        lock.unlock();

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }    
    

    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_commit() {
    // Send logs/snapshots to the follower.
    // Only work for the leader.
    
    std::unique_lock<std::mutex> lock(mtx, std::defer_lock);

    while (true) {
        if (is_stopped()) return;

        lock.lock();

        if (role == leader) {
            int last_log_index = this->last_log_index();
            for (int i = 0; i < num_nodes(); ++i) {
                if (i == my_id) continue;
                if (next_index[i] <= last_log_index) {
                    if (next_index[i] > log.front().index) {
                        append_entries_args<command> args;
                        args.term = current_term;
                        args.leaderId = my_id;
                        args.leaderCommit = commit_index;
                        args.prevLogIndex = next_index[i] - 1;
                        args.prevLogTerm = get_log_term_by_index(args.prevLogIndex);
                        get_entries(next_index[i], last_log_index + 1, args.entries);
                        thread_pool->addObjJob(this, &raft::send_append_entries, i, args);
                    }
                    else {
                        install_snapshot_args args;
                        args.term = current_term;
                        args.leaderId = my_id;
                        args.lastIncludedIndex = log.front().index;
                        args.lastIncludedTerm = log.front().term;
                        args.snapshot = snapshot;
                        thread_pool->addObjJob(this, &raft::send_install_snapshot, i, args);
                    }
                }
            }
        }

        lock.unlock();
        
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }    
    
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_apply() {
    // Apply committed logs the state machine
    // Work for all the nodes.

    std::unique_lock<std::mutex> lock(mtx, std::defer_lock);
    std::vector<log_entry<command>> log_to_apply;
    
    while (true) {
        if (is_stopped()) return;
        
        lock.lock();

        if (commit_index > last_applied) {
            get_entries(last_applied + 1, commit_index + 1, log_to_apply);
            for (log_entry<command>& entry : log_to_apply) {
                state->apply_log(entry.cmd);
            }
            last_applied = commit_index;

            // OPT: initiative snapshot
            // if (last_applied - log.front().index > 100) {
            //     save_snapshot();
            // }
        }

        lock.unlock();
        
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }    
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_ping() {
    // Send empty append_entries RPC to the followers.
    // Only work for the leader.

    std::unique_lock<std::mutex> lock(mtx, std::defer_lock);

    while (true) {
        if (is_stopped()) return;

        lock.lock();

        if (role == leader) {
            heartbeat();
        }

        lock.unlock();
        
        std::this_thread::sleep_for(std::chrono::milliseconds(150)); // Adjust param: heartbeat period
    }    
    return;
}


/******************************************************************

                        Other functions

*******************************************************************/
template<typename state_machine, typename command>
void raft<state_machine, command>::generate_timeouts()
{
    static std::random_device rd;
    static std::minstd_rand gen(rd());
    // static std::mt19937 gen(rd()); // Optional strategy
    static std::uniform_int_distribution<int> follower_dis(300, 500); // Adjust param
    static std::uniform_int_distribution<int> candidate_dis(800, 1000); // Adjust param
    follower_election_timeout = std::chrono::duration_cast<system_clock::duration>(std::chrono::milliseconds(follower_dis(gen)));
    candidate_election_timeout = std::chrono::duration_cast<system_clock::duration>(std::chrono::milliseconds(candidate_dis(gen)));
}

template<typename state_machine, typename command>
inline int raft<state_machine, command>::last_log_term()
{
    return log.back().term;
}

template<typename state_machine, typename command>
inline int raft<state_machine, command>::last_log_index()
{
    return log.back().index;
}

template<typename state_machine, typename command>
inline int raft<state_machine, command>::get_log_term_by_index(int index)
{
    return log[index - log.front().index].term;
}

template<typename state_machine, typename command>
inline void raft<state_machine, command>::log_trunc(int end_index)
{
    if (end_index <= last_log_index()) {
        log.erase(log.begin() + end_index - log.front().index, log.end());
    }
}

template<typename state_machine, typename command>
inline void raft<state_machine, command>::log_trunc_until(int end_index)
{
    if (end_index <= last_log_index()) {
        log.erase(log.begin(), log.begin() + end_index - log.front().index);
    }
    else {
        log.clear();
    }
}

template<typename state_machine, typename command>
inline void raft<state_machine, command>::get_entries(int begin_index, int end_index, std::vector<log_entry<command>>& ret)
{
    if (begin_index < end_index) {
        ret.assign(log.begin() + begin_index - log.front().index, log.begin() + end_index - log.front().index);
    }
    else {
        ret.clear();
    }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::become_follower(int term)
{
    role = follower;
    current_term = term;
    vote_for = -1;
    RAFT_LOG("follow");

    storage->flush_metadata(current_term, vote_for);

    // re-randomize timeouts
    generate_timeouts();
}

template<typename state_machine, typename command>
void raft<state_machine, command>::become_candidate()
{
    role = candidate;

    // increment current term
    ++current_term;
    RAFT_LOG("begin election");
    
    // vote for self
    vote_for = my_id;
    vote_count = 1;
    vote_for_me.assign(num_nodes(), false);
    vote_for_me[my_id] = true;

    storage->flush_metadata(current_term, vote_for);

    // re-randomize timeouts
    generate_timeouts();

    // send request vote RPC
    request_vote_args args{};
    args.term = current_term;
    args.candidateId = my_id;
    args.lastLogIndex = last_log_index();
    args.lastLogTerm = last_log_term();
    for (int i = 0; i < num_nodes(); ++i) {
        if (i == my_id) continue;
        thread_pool->addObjJob(this, &raft::send_request_vote, i, args);
    }

    // reset election timer
    last_received_RPC_time = system_clock::now();
}

template<typename state_machine, typename command>
void raft<state_machine, command>::become_leader()
{
    role = leader;
    RAFT_LOG("become leader");

    // reinitialize leader volatile states
    next_index.assign(num_nodes(), last_log_index() + 1);
    match_index.assign(num_nodes(), 0);
    match_index[my_id] = last_log_index();
    match_count.assign(last_log_index() - commit_index, 0);

    // send initial heartbeat
    heartbeat();
}

template<typename state_machine, typename command>
void raft<state_machine, command>::heartbeat()
{
    static append_entries_args<command> args{};
    args.term = current_term;
    args.leaderId = my_id;
    args.leaderCommit = commit_index;
    for (int i = 0; i < num_nodes(); ++i) {
        if (i == my_id) continue;
        args.prevLogIndex = next_index[i] - 1;
        args.prevLogTerm = get_log_term_by_index(args.prevLogIndex);
        thread_pool->addObjJob(this, &raft::send_append_entries, i, args);
    }
}


#endif // raft_h