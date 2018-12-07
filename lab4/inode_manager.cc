#include "inode_manager.h"
#include "string.h"
#include "time.h"

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
  for (int i=0;i<BLOCK_NUM;i++){
    memset(blocks[i],0,BLOCK_SIZE);
  }
}

void
disk::read_block(blockid_t id, char *buf)
{
  if (id < 0 || id >= BLOCK_NUM || buf == NULL)
    return;

  memcpy(buf, blocks[id], BLOCK_SIZE);
}

void
disk::write_block(blockid_t id, const char *buf)
{
  if (id < 0 || id >= BLOCK_NUM || buf == NULL)
    return;

  memcpy(blocks[id], buf, BLOCK_SIZE);
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*
   * your code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.
   */
  for (int i=IBLOCK(INODE_NUM,BLOCK_NUM)+1;i<BLOCK_NUM;i++){
    if (using_blocks[i]==0){
      using_blocks[i]=1;
      // printf("\tim:alloc block %d\n",i);
      return i;
    }
  }
  return -1;
}

void
block_manager::free_block(uint32_t id)
{
  /* 
   * your code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
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

  for (int i=0;i<BLOCK_NUM;i++){
    using_blocks[i] = 0;
  }
  
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

#define INODE_MAP (IBLOCK(0,BLOCK_NUM)-2)

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  /* 
   * your code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   */
  for (int i=1;i<INODE_NUM+1;i++){
    int map_block = INODE_MAP+(i-1)/BLOCK_SIZE;
    char* buf = new char[BLOCK_SIZE];
    bm->read_block(map_block,buf);
    if (buf[(i-1)%BLOCK_SIZE]==0){
      buf[(i-1)%BLOCK_SIZE] = 1;
      bm->write_block(map_block,buf);
      free(buf);
      // printf("\tim:alloc inode %d\t",i);
      inode_t* newnode = (inode_t*)malloc(sizeof(inode_t));
      memset(newnode,0,sizeof(inode_t));
      newnode->type = type;
      newnode->mtime = time(0);
      newnode->ctime = time(0);
      newnode->atime = time(0);
      printf("CREATETIME::%d\n",newnode->mtime);
      put_inode(i,newnode);
      free(newnode);
      return i;
    }

  }
  return 0;
}

void
inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */
  inode_t* res = get_inode(inum);
  if (res==NULL){
    printf("\tim: inode already freed %d",inum);
  }else{
    char* buf = new char[BLOCK_SIZE];
    memset(buf,0,BLOCK_SIZE);
    bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
    int map_block = INODE_MAP + (inum-1)/BLOCK_SIZE;
    free(buf);
    buf = new char[BLOCK_SIZE];
    bm->read_block(map_block,buf);
    buf[(inum-1)%BLOCK_SIZE] = 0;
    bm->write_block(map_block,buf);
    free(buf);
    printf("\tim:free inode %d\n",inum);
  }
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

  ino->mtime = time(0);
  ino->ctime = time(0);
  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + inum%IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_Out
   */
  // for(int i=0;i<sizeof(size)/sizeof(int);i++){
  //   printf("%d:%d\t",i,size[i]);
  // }
  inode_t *ino = get_inode(inum);
  *size = ino->size;
  // printf("test:%d\n",ino->blocks[0]);
  int chcnt = 0;
  int blcnt = 0;
  char* buf = new char[BLOCK_SIZE];
  bm->read_block(ino->blocks[blcnt],buf);
  // printf("test:%s\n",buf);
  (*buf_out) = new char[*size];
  for(int i=0;i<*size;i++){
    (*buf_out)[i] = buf[chcnt];
    chcnt++;
    if (chcnt==BLOCK_SIZE){
      chcnt = 0;
      blcnt++;
      if (blcnt<NDIRECT){
        bm->read_block(ino->blocks[blcnt],buf);
      }else{
        char* blist = new char[BLOCK_SIZE];
        bm->read_block(ino->blocks[NDIRECT],blist);
        blockid_t bid = ((blockid_t*)blist)[blcnt-NDIRECT];
        bm->read_block(bid,buf);
      }
    }
  }
  return;
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode
   */
  inode_t* ino = get_inode(inum);
  //free old blocks
  int ori_size = ino->size;
  int blocknum = ori_size/BLOCK_SIZE+1;
  for (int i=0;i<blocknum;i++){
    if (i<NDIRECT){
      blockid_t bid = (ino->blocks)[i];
      bm->free_block(bid);
    }else{
      char* buf = new char[BLOCK_SIZE];
      bm->read_block((ino->blocks)[NDIRECT],buf);
      blockid_t bid = ((int*)buf)[i-NDIRECT];
      bm->free_block(bid);
    }
  }
  //write new file
  ino->size = size;
  blockid_t dirlist;
  blockid_t* blist = new blockid_t[BLOCK_SIZE/sizeof(blockid_t)];
  for (int b=0;b<size/BLOCK_SIZE+1;b++){
    blockid_t bid = bm->alloc_block();
    if (b<NDIRECT){
      bm->write_block(bid,buf+b*BLOCK_SIZE);
      ino->blocks[b] = bid;
    }
    if (b==NDIRECT){
      dirlist = bm->alloc_block();
      ino->blocks[b] = dirlist;
    }
    if (b>=NDIRECT){
      blist[b-NDIRECT] = bid;
      bm->write_block(bid,buf+b*BLOCK_SIZE);
    }
  }
  if (size>BLOCK_SIZE*NDIRECT){
    bm->write_block(dirlist,(char*)blist);
  }
  put_inode(inum,ino);
  free(ino);
  return;
}

void
inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  inode_t* res = get_inode(inum);
  if (res==NULL){
    return;
  }
  a.type = res->type;
  a.mtime = res->mtime;
  a.atime = res->atime;
  a.ctime = res->ctime;
  a.size  = res->size;
  free(res);
  return;
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your code goes here
   * note: you need to consider about both the data block and inode of the file
   */
  inode_t *ino = get_inode(inum);
  int size = ino->size;
  int blocknum = size/BLOCK_SIZE + 1;
  for (int i=0;i<blocknum;i++){
    if (i<NDIRECT){
      blockid_t bid = (ino->blocks)[i];
      bm->free_block(bid);
    }else{
      char* buf = new char[BLOCK_SIZE];
      bm->read_block((ino->blocks)[NDIRECT],buf);
      blockid_t bid = ((int*)buf)[i-NDIRECT];
      bm->free_block(bid);
    }
  }
  free_inode(inum);
  free(ino);
  return;
}

void
inode_manager::append_block(uint32_t inum,blockid_t &bid)
{
  bid = bm->alloc_block();
  struct inode * ino = get_inode(inum);

  // int block_num = ino->size / BLOCK_SIZE + (ino->size % BLOCK_SIZE > 0);
  int block_num = (ino->size + BLOCK_SIZE-1) / BLOCK_SIZE;
  if (block_num < NDIRECT){
    ino->blocks[block_num] = bid;
  }else{
    int IND_NODE[NINDIRECT];
    if (block_num == NDIRECT){
      ino->blocks[NDIRECT] = bm->alloc_block();
    }
    bm->read_block(ino->blocks[NDIRECT], (char *)IND_NODE);
    IND_NODE[block_num-NDIRECT] = bid;
    bm->write_block(ino->blocks[NDIRECT], (char *)IND_NODE);
  }
  // block_num++;
  ino->size += BLOCK_SIZE;
  put_inode(inum, ino);
}

void
inode_manager::get_block_ids(uint32_t inum, std::list<blockid_t> &block_ids)
{
  struct inode * ino = get_inode(inum);
  int sz = ino->size;
  int block_num = (ino->size + BLOCK_SIZE-1) / BLOCK_SIZE;
  blockid_t D_NODE[NDIRECT];
  blockid_t IND_NODE[NINDIRECT];
  int i;
  for (i=0;i<block_num&&i<NDIRECT;i++){
    block_ids.push_back(ino->blocks[i]);
  }
  if (i<block_num){
    bm->read_block(ino->blocks[NDIRECT],(char *)IND_NODE);
  }
  for (;i<block_num;i++){
    block_ids.push_back(IND_NODE[i-NDIRECT]);
  }
}

void 
inode_manager::read_block(blockid_t bid, char block[BLOCK_SIZE])
{
  bm->read_block(bid, block);
}

void 
inode_manager::write_block(blockid_t bid, const char block[BLOCK_SIZE])
{
  bm->write_block(bid,block);
}
  
void 
inode_manager::complete(uint32_t inum, uint32_t size)
{
  struct inode * ino = get_inode(inum);
  ino->size = size;
  put_inode(inum, ino);
}