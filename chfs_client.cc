// chfs client.  implements FS operations using extent server
#include "chfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

chfs_client::chfs_client(std::string extent_dst)
{
    ec = new extent_client(extent_dst);
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
}

chfs_client::inum
chfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
chfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
chfs_client::isfile(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    } 
    printf("isfile: %lld is not a file\n", inum);
    return false;
}

bool
chfs_client::isdir(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_DIR) {
        printf("isdir: %lld is a dir\n", inum);
        return true;
    }
    printf("isdir: %lld is not a dir\n", inum);
    return false;
}

bool 
chfs_client::issymlink(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_SYMLINK) {
        printf("issymlink: %lld is a symlink\n", inum);
        return true;
    }
    printf("issymlink: %lld is not a symlink\n", inum);
    return false;
}

int
chfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int
chfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}


#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr
int
chfs_client::setattr(inum ino, size_t size)
{
    int r = OK;
    std::string buf;

    r = ec->get(ino, buf);
    if (r == OK) {
        buf.resize(size);
        r = ec->put(ino, buf);
    }

    return r;
}

int
chfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;
    
    bool found = false;
    r = lookup(parent, name, found, ino_out);
    if (found) r = EXIST;

    if (r == OK) {
        r = ec->create(extent_protocol::T_FILE, ino_out);
        if (r == OK) {
            std::string buf;
            r = ec->get(parent, buf);

            if (r == OK) {
                dirent_n ent;
                ent.inum = ino_out;
                ent.len = strlen(name);
                memcpy(ent.name, name, ent.len);
                buf.append((char*)&ent, sizeof(dirent_n));

                r = ec->put(parent, buf);
            }
        }
    }

    return r;
}

int
chfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    bool found = false;
    r = lookup(parent, name, found, ino_out);
    if (found) r = EXIST;

    if (r == OK) {
        r = ec->create(extent_protocol::T_DIR, ino_out);
        if (r == OK) {
            std::string buf;
            r = ec->get(parent, buf);

            if (r == OK) {
                dirent_n ent;
                ent.inum = ino_out;
                ent.len = strlen(name);
                memcpy(ent.name, name, ent.len);
                buf.append((char*)&ent, sizeof(dirent_n));

                r = ec->put(parent, buf);
            }
        }
    }

    return r;
}

int
chfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;
    found = false;
    extent_protocol::attr a;

    r = ec->getattr(parent, a);
    if (r == OK) {
        std::list<dirent> entries;

        readdir(parent, entries);
        for (dirent de : entries)
        {
            if (de.name == name) {
                found = true;
                ino_out = de.inum;
            }
        }
    }

    return r;
}

int
chfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;
    extent_protocol::attr a;

    r = ec->getattr(dir, a);
    if (r == OK) {
        std::string buf;
        r = ec->get(dir, buf);
        if (r == OK) {
            const char* ptr = buf.data();
            size_t ent_num = buf.size() / sizeof(dirent_n);
            for (size_t i = 0; i < ent_num; ++i) {
                dirent_n ent_n;
                memcpy(&ent_n, ptr + i * sizeof(dirent_n), sizeof(dirent_n));

                dirent ent;
                ent.inum = ent_n.inum;
                ent.name = std::string(ent_n.name, ent_n.len);

                list.push_back(ent);
            }
        }
    }

    return r;
}

int
chfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;
    std::string buf;

    r = ec->get(ino, buf);
    if (r == OK) {
        if ((size_t)off < buf.size()) {
            size = size < buf.size() - (size_t)off ? size : buf.size() - (size_t)off;
            data = buf.substr(off, size);
        }
        else {
            data = std::string();
        }
    }

    return r;
}

int
chfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;
    std::string buf;
    
    r = ec->get(ino, buf);
    if (r == OK) {
        /*if ((size_t)off <= buf.size()) {
            bytes_written = size;
        }
        else {
            bytes_written = (size_t)off + size - buf.size();
        }*/
        if (off + size > buf.size()) {
            buf.resize(off + size, '\0');
        }
        buf.replace(off, size, std::string(data,size));

        r = ec->put(ino, buf);
    }

    return r;
}

int chfs_client::unlink(inum parent,const char *name)
{
    int r = OK;
    inum id;

    bool found = false;
    r = lookup(parent, name, found, id);
    if (r == OK && !found) r = NOENT;

    if (r == OK) {
        r = ec->remove(id);
        if (r == OK) {
            std::list<dirent> entries;
            r = readdir(parent, entries);

            if (r == OK) {
                std::string buf;
                for (dirent ent : entries) {
                    if (ent.inum == id) continue;

                    dirent_n ent_n;
                    ent_n.inum = ent.inum;
                    ent_n.len = ent.name.size();
                    memcpy(ent_n.name, ent.name.data(), ent_n.len);

                    buf.append((char*)&ent_n, sizeof(dirent_n));
                }

                r = ec->put(parent, buf);
            }
        }
    }

    return r;
}

int 
chfs_client::readlink(inum ino, std::string& buf)
{
    int r = OK;

    r = ec->get(ino, buf);

    return r;
}

int
chfs_client::symlink(inum parent, const char* name, const char* link, inum& ino_out) 
{
    int r = OK;

    bool found = false;
    r = lookup(parent, name, found, ino_out);
    if (found) r = EXIST;

    if (r == OK) {
        r = ec->create(extent_protocol::T_SYMLINK, ino_out);
        if (r == OK) {
            r = ec->put(ino_out, std::string(link));

            if (r == OK) {
                std::string buf;
                r = ec->get(parent, buf);

                if (r == OK) {
                    dirent_n ent;
                    ent.inum = ino_out;
                    ent.len = strlen(name);
                    memcpy(ent.name, name, ent.len);
                    buf.append((char*)&ent, sizeof(dirent_n));

                    r = ec->put(parent, buf);
                }
            }
        }
    }

    return r;
}


