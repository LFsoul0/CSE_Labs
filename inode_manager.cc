#include "inode_manager.h"

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf)
{
    memcpy(buf, blocks[id], BLOCK_SIZE);
}

void
disk::write_block(blockid_t id, const char *buf)
{
    memcpy(blocks[id], buf, BLOCK_SIZE);
}

// block layer -----------------------------------------

#define FILEBLOCK (IBLOCK(INODE_NUM, sb.nblocks) + 1)

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
    for (int i = FILEBLOCK; i < BLOCK_NUM; ++i) {
        if (!using_blocks[i]) {
            using_blocks[i] = 1;
            return i;
        }
    }

    return 0;
}

void
block_manager::free_block(uint32_t id)
{
    using_blocks[id] = 0;
  
    return;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;

}

void
block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
    static int inum = 0;

    for (int i = 0; i < INODE_NUM; ++i) {
        inum = (inum + 1) % INODE_NUM;

        inode_t* ino = get_inode(inum);
        if (!ino) {
            ino = (inode_t*)malloc(sizeof(inode_t));

            bzero(ino, sizeof(inode_t));
            ino->type = type;
            ino->size = 0;
            ino->atime = time(nullptr);
            ino->mtime = time(nullptr);
            ino->ctime = time(nullptr);
            
            put_inode(inum, ino);
            free(ino);

            break;
        }
    }

    return inum;
}

void
inode_manager::free_inode(uint32_t inum)
{
    inode_t* ino = get_inode(inum);
    if (!ino) return;

    ino->type = 0;
    put_inode(inum, ino);
    free(ino);

    return;
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino, *ino_disk;
  char buf[BLOCK_SIZE];

  printf("\tim: get_inode %d\n", inum);

  if (inum < 0 || inum >= INODE_NUM) {
    printf("\tim: inum out of range\n");
    return NULL;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  // printf("%s:%d\n", __FILE__, __LINE__);

  ino_disk = (struct inode*)buf + inum%IPB;
  if (ino_disk->type == 0) {
    printf("\tim: inode not exist\n");
    return NULL;
  }

  ino = (struct inode*)malloc(sizeof(struct inode));
  *ino = *ino_disk;

  return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + inum%IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))

blockid_t get_blockid_n(block_manager* bm, inode_t* ino, uint32_t n)
{
    if (n < NDIRECT) {
        return ino->blocks[n];
    }
    else {
        char buf[BLOCK_SIZE]{ 0 };
        bm->read_block(ino->blocks[NDIRECT], buf);
        return ((blockid_t*)buf)[n - NDIRECT];
    }
};

void alloc_block_n(block_manager* bm, inode_t* ino, uint32_t n)
{
    if (n < NDIRECT) {
        ino->blocks[n] = bm->alloc_block();
    }
    else {
        if (!ino->blocks[NDIRECT]) {
            ino->blocks[NDIRECT] = bm->alloc_block();
        }

        char buf[BLOCK_SIZE]{ 0 };
        bm->read_block(ino->blocks[NDIRECT], buf);
        ((blockid_t*)buf)[n - NDIRECT] = bm->alloc_block();
        bm->write_block(ino->blocks[NDIRECT], buf);
    }
};

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
    inode_t* ino = get_inode(inum);
    if (!ino) return;

    *size = ino->size;
    *buf_out = (char*)malloc(*size);

    int block_num = *size / BLOCK_SIZE;
    int remain_size = *size % BLOCK_SIZE;
    

    for (int i = 0; i < block_num; ++i) {
        bm->read_block(get_blockid_n(bm, ino, i), *buf_out + i * BLOCK_SIZE);
    }
    if (remain_size) {
        char midbuf[BLOCK_SIZE]{ 0 };
        bm->read_block(get_blockid_n(bm, ino, block_num), midbuf);
        memcpy(*buf_out + block_num * BLOCK_SIZE, midbuf, remain_size);
    }

    free(ino);

    return;
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
    inode_t* ino = get_inode(inum);
    if (!ino) return;

    int old_blocks = ino->size ? (ino->size - 1) / BLOCK_SIZE + 1 : 0;
    int new_blocks = size ? (size - 1) / BLOCK_SIZE + 1 : 0;
    if (old_blocks <= new_blocks) {
        for (int i = old_blocks; i < new_blocks; ++i) {
            alloc_block_n(bm, ino, i);
        }
    }
    else {
        for (int i = new_blocks; i < old_blocks; ++i) {
            bm->free_block(get_blockid_n(bm, ino, i));
        }
    }

    int block_num = size / BLOCK_SIZE;
    int remain_size = size % BLOCK_SIZE;
    for (int i = 0; i < block_num; ++i) {
        bm->write_block(get_blockid_n(bm, ino, i), buf + i * BLOCK_SIZE);
    }
    if (remain_size) {
        char midbuf[BLOCK_SIZE]{ 0 };
        memcpy(midbuf, buf + block_num * BLOCK_SIZE, remain_size);
        bm->write_block(get_blockid_n(bm, ino, block_num), midbuf);
    }

    ino->size = size;
    ino->atime = time(nullptr);
    ino->mtime = time(nullptr);
    ino->ctime = time(nullptr);

    put_inode(inum, ino);
    free(ino);

    return;
}

void
inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
    inode_t* ino = get_inode(inum);
    if (!ino) return;

    a.type = ino->type;
    a.size = ino->size;
    a.atime = ino->atime;
    a.mtime = ino->mtime;
    a.ctime = ino->ctime;

    free(ino);
  
    return;
}

void
inode_manager::remove_file(uint32_t inum)
{
    inode_t *ino = get_inode(inum);

    int block_num = ino->size ? (ino->size - 1) / BLOCK_SIZE + 1 : 0;
    for (int i = 0; i < block_num; ++i) {
        bm->free_block(get_blockid_n(bm, ino, i));
    }
    if (block_num > NDIRECT) {
        bm->free_block(ino->blocks[NDIRECT]);
    }
    bzero(ino, sizeof(inode_t));

    free_inode(inum);
    free(ino);

    return;
}
