#ifndef raft_storage_h
#define raft_storage_h

#include "raft_protocol.h"
#include <fcntl.h>
#include <mutex>
#include <string>
#include <vector>
#include <fstream>
#include <sstream>

#ifndef max
#define max(a,b) (((a) > (b)) ? (a) : (b))
#endif

#ifndef min
#define min(a,b) (((a) < (b)) ? (a) : (b))
#endif

template<typename command>
class raft_storage {
public:
    raft_storage(const std::string& file_dir);
    ~raft_storage();

    bool flush_metadata(int term, int vote);
    bool flush_log(const std::vector<log_entry<command>>& log);
    bool append_log(const log_entry<command>& log, int new_size); 
    bool append_log(const std::vector<log_entry<command>>& log, int new_size); 
    bool flush_snapshot(const std::vector<char>& snapshot);
    bool flush_all(int term, int vote, const std::vector<log_entry<command>>& log, const std::vector<char>& snapshot);
    bool restore(int& term, int& vote, std::vector<log_entry<command>>& log, std::vector<char>& snapshot);
    
private:
    std::mutex mtx;
    std::string metadata_filename;
    std::string log_filename;
    std::string snapshot_filename;

    char* buf;
    int buf_size;
};

template<typename command>
raft_storage<command>::raft_storage(const std::string& dir){
    metadata_filename = dir + "/metadata";
    log_filename = dir + "/log";
    snapshot_filename = dir + "/snapshot";
    buf_size = 16;
    buf = new char[buf_size];
}

template<typename command>
raft_storage<command>::~raft_storage() {
    delete[] buf;
}

template<typename command>
bool raft_storage<command>::flush_metadata(int term, int vote)
{
    std::unique_lock<std::mutex> lock(mtx);

    std::fstream fs(metadata_filename, std::ios_base::out | std::ios_base::trunc | std::ios_base::binary);
    if (!fs.is_open()) {
        return false;
    }

    fs.write((const char*)&term, sizeof(int));
    fs.write((const char*)&vote, sizeof(int));

    fs.close();

    return true;
}

template<typename command>
bool raft_storage<command>::flush_log(const std::vector<log_entry<command>>& log)
{
    std::unique_lock<std::mutex> lock(mtx);

    std::fstream fs(log_filename, std::ios_base::out | std::ios_base::trunc | std::ios_base::binary);
    if (!fs.is_open()) {
        return false;
    }

    int size = log.size();
    fs.write((const char*)&size, sizeof(int));

    for (const log_entry<command>& entry : log) {
        fs.write((const char*)&entry.index, sizeof(int));
        fs.write((const char*)&entry.term, sizeof(int));

        size = entry.cmd.size();
        fs.write((const char*)&size, sizeof(int));
        if (size > buf_size) {
            delete[] buf;
            buf_size = max(size, 2 * buf_size);
            buf = new char[buf_size];
        }

        entry.cmd.serialize(buf, size);
        fs.write(buf, size);
    }

    fs.close();

    return true;
}

template<typename command>
bool raft_storage<command>::append_log(const log_entry<command>& entry, int new_size)
{
    std::unique_lock<std::mutex> lock(mtx);

    std::fstream fs(log_filename, std::ios_base::out | std::ios_base::in | std::ios_base::binary);
    if (!fs.is_open()) {
        return false;
    }

    int size = 0;
    fs.seekp(0, std::ios_base::end);
    fs.write((const char*)&entry.index, sizeof(int));
    fs.write((const char*)&entry.term, sizeof(int));

    size = entry.cmd.size();
    fs.write((const char*)&size, sizeof(int));
    if (size > buf_size) {
        delete[] buf;
        buf_size = max(size, 2 * buf_size);
        buf = new char[buf_size];
    }

    entry.cmd.serialize(buf, size);
    fs.write(buf, size);

    fs.seekp(0, std::ios_base::beg);
    fs.write((const char*)&new_size, sizeof(int));

    fs.close();

    return true;
}

template<typename command>
bool raft_storage<command>::append_log(const std::vector<log_entry<command>>& log, int new_size)
{
    std::unique_lock<std::mutex> lock(mtx);

    std::fstream fs(log_filename, std::ios_base::out | std::ios_base::in | std::ios_base::binary);
    if (!fs.is_open()) {
        return false;
    }

    int size = 0;
    fs.seekp(0, std::ios_base::end);
    for (const log_entry<command>& entry : log) {
        fs.write((const char*)&entry.index, sizeof(int));
        fs.write((const char*)&entry.term, sizeof(int));

        size = entry.cmd.size();
        fs.write((const char*)&size, sizeof(int));
        if (size > buf_size) {
            delete[] buf;
            buf_size = max(size, 2 * buf_size);
            buf = new char[buf_size];
        }

        entry.cmd.serialize(buf, size);
        fs.write(buf, size);
    }

    fs.seekp(0, std::ios_base::beg);
    fs.write((const char*)&new_size, sizeof(int));

    fs.close();

    return true;
}

template<typename command>
bool raft_storage<command>::flush_snapshot(const std::vector<char>& snapshot)
{
    std::unique_lock<std::mutex> lock(mtx);

    std::fstream fs(snapshot_filename, std::ios_base::out | std::ios_base::trunc | std::ios_base::binary);
    if (!fs.is_open()) {
        return false;
    }

    int size = snapshot.size();
    fs.write((const char*)&size, sizeof(int));
    fs.write(snapshot.data(), size);

    fs.close();

    return true;
}

template<typename command>
bool raft_storage<command>::flush_all(int term, int vote, const std::vector<log_entry<command>>& log, const std::vector<char>& snapshot)
{
    if (!flush_metadata(term, vote)) {
        return false;
    }
    if (!flush_log(log)) {
        return false;
    }
    if (!flush_snapshot(snapshot)) {
        return false;
    }

    return true;
}

template<typename command>
bool raft_storage<command>::restore(int& term, int& vote, std::vector<log_entry<command>>& log, std::vector<char>& snapshot)
{
    std::unique_lock<std::mutex> lock(mtx);

    std::fstream fs;
    fs.open(metadata_filename, std::ios_base::in | std::ios_base::binary);
    if (!fs.is_open() || fs.eof()) { // no file or empty file
        return false;
    }

    fs.read((char*)&term, sizeof(int));
    fs.read((char*)&vote, sizeof(int));

    fs.close();
    fs.open(log_filename, std::ios_base::in | std::ios_base::binary);
    if (!fs.is_open() || fs.eof()) { // no file or empty file
        return false;
    }

    int size = 0;
    fs.read((char*)&size, sizeof(int));
    log.resize(size);

    for (log_entry<command>& entry : log) {
        fs.read((char*)&entry.index, sizeof(int));
        fs.read((char*)&entry.term, sizeof(int));

        fs.read((char*)&size, sizeof(int));
        if (size > buf_size) {
            delete[] buf;
            buf_size = max(size, 2 * buf_size);
            buf = new char[buf_size];
        }

        fs.read(buf, size);
        entry.cmd.deserialize(buf, size);
    }

    fs.close();
    fs.open(snapshot_filename, std::ios_base::in | std::ios_base::binary);
    if (!fs.is_open() || fs.eof()) { // no file or empty file
        return false;
    }

    fs.read((char*)&size, sizeof(int));
    snapshot.resize(size);

    for (char& c : snapshot) {
        fs.read(&c, sizeof(char));
    }

    fs.close();

    return true;
}

#endif // raft_storage_h