#include "namenode.h"
#include "extent_client.h"
#include "lock_client.h"
#include <sys/stat.h>
#include <unistd.h>
#include "threader.h"

using namespace std;

void NameNode::init(const string &extent_dst, const string &lock_dst) {
  ec = new extent_client(extent_dst);
  lc = new lock_client_cache(lock_dst);
  yfs = new yfs_client(ec, lc);

  heart = 0;
  NewThread(this, &NameNode::countbeat);
  /* Add your init logic here */
}

void NameNode::countbeat(){
  while(true){
    this->heart++;
    sleep(1);
  }
}

list<NameNode::LocatedBlock> NameNode::GetBlockLocations(yfs_client::inum ino) {
  fprintf(stderr,"locatedblocklist:\n");fflush(stderr);
  // ret value
  list<LocatedBlock> ret;

  // get block_id_list & attr
  list<blockid_t> bid_list;
  extent_protocol::attr attr;
  ec->get_block_ids(ino,bid_list);
  ec->getattr(ino,attr);

  // format bid to BlockLocation
  uint64_t offset = 0;
  list<blockid_t>::iterator it=bid_list.begin();
  int i = 0;
  for (;it!=bid_list.end();it++,i++){
    uint64_t size;
    if (i==bid_list.size()-1){
      size = attr.size - offset;
    }else{
      size = BLOCK_SIZE;
    }
    printf("bid:%d\n",*it);fflush(stdout);
    LocatedBlock item(*it,offset,size,GetDatanodes());
    ret.push_back(item);
    offset += BLOCK_SIZE;
  }
  return ret;
}

bool NameNode::Complete(yfs_client::inum ino, uint32_t new_size) {
  bool ret = !ec->complete(ino, new_size);
  if (ret)
    lc->release(ino);
  return ret;
}

NameNode::LocatedBlock NameNode::AppendBlock(yfs_client::inum ino) {
  blockid_t bid;
  extent_protocol::attr attr;
  ec->getattr(ino, attr);
  ec->append_block(ino, bid);

  uint64_t size;
  if (attr.size%BLOCK_SIZE == 0){
    size = BLOCK_SIZE;
  }else{
    size = attr.size%BLOCK_SIZE;
  }
  modified_blocks.insert(bid);
  LocatedBlock ret(bid, attr.size, size, GetDatanodes());
  return ret;
}

bool NameNode::Rename(yfs_client::inum src_dir_ino, string src_name, yfs_client::inum dst_dir_ino, string dst_name) {
  // get dir content
  string src_buf, dst_buf;
  ec->get(src_dir_ino, src_buf);
  ec->get(dst_dir_ino, dst_buf);

  // search if src_name in src_dir
  string new_src_buf = "";
  bool found = false;
  int i = 0;
  string renamed_ino;
  while (i<src_buf.size()){
    string thname="",ino="";
    //parse name
    while(src_buf[i]!='/'){
      thname+=src_buf[i];
      i++;
    }
    i++;
    printf("thname::%s\n",thname.data());
    //parse ino
    while(src_buf[i]!='/'){
      ino+=src_buf[i];
      i++;
    }
    i++;
    printf("ino::%s\n",ino.data());
    //refresh new_src_buf & found
    if(thname==src_name){
      found = true;
      dst_buf += thname + '/' + ino + '/';
      renamed_ino = ino;
    }else{
      new_src_buf += thname + '/' + ino + '/';
    }
  }

  // put back src_buf & dst_buf
  if (found){
    if (src_dir_ino!=dst_dir_ino){
      ec->put(src_dir_ino,new_src_buf);
      ec->put(dst_dir_ino,dst_buf);
    }else{
      new_src_buf += dst_name+'/'+renamed_ino+'/';
      ec->put(src_dir_ino,new_src_buf);
    }
    
  }
  return found;
}

bool NameNode::Mkdir(yfs_client::inum parent, string name, mode_t mode, yfs_client::inum &ino_out) {
  return !yfs->mkdir(parent,name.c_str(),mode,ino_out);
}

bool NameNode::Create(yfs_client::inum parent, string name, mode_t mode, yfs_client::inum &ino_out) {
  bool ret = !yfs->create(parent,name.c_str(),mode,ino_out);
  if (ret) {
    lc->acquire(ino_out);
  }
  return ret;
}

bool NameNode::Isfile(yfs_client::inum ino) {
  extent_protocol::attr attr;
  ec->getattr(ino,attr);
  if (attr.type == extent_protocol::T_FILE){
    return true;
  }
  return false;
}

bool NameNode::Isdir(yfs_client::inum ino) {
  extent_protocol::attr attr;
  ec->getattr(ino,attr);
  if (attr.type == extent_protocol::T_DIR){
    return true;
  }
  return false;
}

bool NameNode::Getfile(yfs_client::inum ino, yfs_client::fileinfo &info) {
  extent_protocol::attr attr;
  if (ec->getattr(ino, attr) != extent_protocol::OK) {
      return false;
  }
  info.atime = attr.atime;
  info.mtime = attr.mtime;
  info.ctime = attr.ctime;
  info.size = attr.size;
  return true;
}

bool NameNode::Getdir(yfs_client::inum ino, yfs_client::dirinfo &info) {
  extent_protocol::attr attr;
  if (ec->getattr(ino, attr) != extent_protocol::OK) {
      return false;
  }
  info.atime = attr.atime;
  info.mtime = attr.mtime;
  info.ctime = attr.ctime;
  return true;
}

bool NameNode::Readdir(yfs_client::inum ino, std::list<yfs_client::dirent> &dir) {
  std::string dircont;
  ec->get(ino,dircont);
  unsigned int i=0;
  while (i<dircont.size()){
      std::string thname="",ino="";
      //parse name
      while(dircont[i]!='/'){
          thname+=dircont[i];
          i++;
      }
      i++;
      printf("thname::%s\n",thname.data());
      //parse ino
      while(dircont[i]!='/'){
          ino+=dircont[i];
          i++;
      }
      i++;
      printf("ino::%s\n",ino.data());
      yfs_client::inum ino_out;
      std::stringstream ss;
      ss<<ino;
      ss>>ino_out;
      yfs_client::dirent *thent = new yfs_client::dirent;
      thent->name = thname;
      thent->inum = ino_out;
      dir.push_back(*thent);
  }
  return true;
}

bool NameNode::Unlink(yfs_client::inum parent, string name, yfs_client::inum ino) {
  // remove the inode
  ec->remove(ino);

  // update dir content
  string dircont;
  ec->get(parent,dircont);
  bool found = false;
  int i = 0;
  string new_cont = "";
  while (i<dircont.size()){
    string thname="",ino="";
    //parse name
    while(dircont[i]!='/'){
      thname+=dircont[i];
      i++;
    }
    i++;
    printf("thname::%s\n",thname.data());
    //parse ino
    while(dircont[i]!='/'){
      ino+=dircont[i];
      i++;
    }
    i++;
    printf("ino::%s\n",ino.data());
    //refresh new_src_buf & found
    if(thname!=name){
      new_cont += thname + '/' + ino + '/';
    }else{
      found = true;
    }
  }
  ec->put(parent,new_cont);
  return found;
}

void NameNode::DatanodeHeartbeat(DatanodeIDProto id) {
  int m = 0;
  for (auto i : datanodes){
    m = max(m, i.second);
  }
  datanodes[id] = this->heart;
}

void NameNode::RegisterDatanode(DatanodeIDProto id) {
  if (this->heart > 5){
    for(auto b : modified_blocks){
      ReplicateBlock(b, master_datanode, id);
    }
  }
  datanodes.insert(make_pair(id, this->heart));
}

list<DatanodeIDProto> NameNode::GetDatanodes() {
  list<DatanodeIDProto> l;
  for (auto i : datanodes){
    if (i.second >= this->heart - 3){
      l.push_back(i.first);
    }
  }
  return l;
}
