// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
  ec = new extent_client(extent_dst);
//   lc = new lock_client(lock_dst);
  lc = new lock_client_cache(lock_dst);
  if (ec->put(1, "") != extent_protocol::OK)
      printf("error init root dir\n"); // XYB: init root dir
}


yfs_client::inum
yfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
yfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
yfs_client::isfile(inum inum)
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
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 * 
 * */

bool
yfs_client::isdir(inum inum)
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
yfs_client::islink(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_SYM) {
        printf("issymlink: %lld is a symlink\n", inum);
        return true;
    } 
    printf("issymlink: %lld is not a link\n", inum);
    return false;
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
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
yfs_client::getdir(inum inum, dirinfo &din)
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
yfs_client::setattr(inum ino, size_t size)
{
    int r = OK;

    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */
    std::string buf;
    if (ec->get(ino,buf)!=OK){
        return IOERR;
    }
    buf.resize(size,0);
    if (ec->put(ino,buf)!=OK){
        return IOERR;
    }
    return r;
}

int
yfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    lc->acquire(parent);
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    bool found;
    inum ino;
    yfs_client::lookup(parent,name,found,ino);
    if (found==true){
        printf("CREATE::the file is existed\n");
        r = EXIST;
    }else{
        std::string content;
        ec->get(parent,content);
        std::string sname = name;
        content += sname + '/';
        ec->create(extent_protocol::T_FILE,ino);
        ino_out = ino;
        std::stringstream ss;
        ss<<ino;
        std::string inobuf;
        ss>>inobuf;
        content += inobuf + '/';
        ec->put(parent,content);
        printf("ino out:%d\n",ino);
        
    }
    printf("YFSMKFILE:%d %d %s\n",r,ino_out,name);
    lc->release(parent);
    return r;
}

int
yfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    lc->acquire(parent);
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    bool found;
    inum ino;
    yfs_client::lookup(parent,name,found,ino);
    if (found==true){
        r = EXIST;
    }else{
        std::string content;
        ec->get(parent,content);
        std::string sname = name;
        content += sname + '/';
        ec->create(extent_protocol::T_DIR,ino);
        ino_out = ino;
        std::stringstream ss;
        ss<<ino;
        std::string inobuf;
        ss>>inobuf;
        content += inobuf + '/';
        ec->put(parent,content);
        printf("ino out:%d\n",ino);
        
    }
    printf("YFSMKDIR:%d %d %s\n",r,ino_out,name);
    lc->release(parent);
    return r;
}

int
yfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
    printf("lookup::%d\t%s\n",parent,name);
    std::string sname = name;
    printf("nameis::%s\n",sname.data());
    std::string dircont;
    ec->get(parent,dircont);
    printf("dircont::\n%s\n--------\n",dircont.data());
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
        if (thname==name){
            std::stringstream ss;
            ss<<ino;
            inum res = 0;
            ss>>res;
            ino_out = res;
            found = true;
            return r;
        }
    }
    found = false;
    return r;
}

int
yfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */
    printf("READDIR::%d\n",dir);
    std::string dircont;
    ec->get(dir,dircont);
    printf("dircont::\n%s\n--------\n",dircont.data());
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
        inum ino_out;
        std::stringstream ss;
        ss<<ino;
        ss>>ino_out;
        dirent *thent = new dirent;
        thent->name = thname;
        thent->inum = ino_out;
        list.push_back(*thent);
    }
    return r;
}

int
yfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;

    /*
     * your code goes here.
     * note: read using ec->get().
     */
    std::string content;
    if (ec->get(ino,content)!=OK){
        return IOERR;
    }
    if (off>content.size()){
        data = "";
        return OK;
    }
    data = content.substr(off,size);
    return r;
}

int
yfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    lc->acquire(ino);
    int r = OK;

    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */
    bytes_written = size;
    std::string content;
    if (ec->get(ino,content)!=OK){
        printf("WRITE::get error\n");
        lc->release(ino);
        return IOERR;
    }
    std::string copyc = content;
    if (off+size>content.size()){
        if (off>content.size()){
            bytes_written = off+size-content.size();
        }
        content.resize(off+size,'\0');
    }
    for (int i=0;i<size;i++){
        content[off+i] = data[i];
    }
    if (ec->put(ino,content)!=OK){
        printf("WRITE::put error\n");
        lc->release(ino);
        return IOERR;
    }
    lc->release(ino);
    return r;
}

int yfs_client::unlink(inum parent,const char *name)
{
    lc->acquire(parent);
    int r = OK;

    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */
    
    //lookup for the file
    bool found;
    inum ino;
    lookup(parent,name,found,ino);
    if (found==false){
        printf("UNLINK::no such file or dir\n");
        lc->release(parent);
        return IOERR;
    }
    //remove the file
    if (ec->remove(ino)!=OK){
        printf("UNLINK:can't remove\n");
        lc->release(parent);
        return IOERR;
    }
    //update the dir content
    std::list<dirent> dirlist;
    if (readdir(parent,dirlist)!=OK){
        printf("UNLINK:readdir error\n");
        lc->release(parent);
        return IOERR;
    }
    std::string new_dir_cont = "";
    std::string removed_name = name;
    for (std::list<dirent>::iterator it=dirlist.begin();it!=dirlist.end();it++){
        if (it->name!=name){
            new_dir_cont += it->name + '/';
            std::stringstream ss;
            ss<<it->inum;
            std::string strino;
            ss>>strino;
            new_dir_cont += strino + '/';
        }
    }
    if (ec->put(parent,new_dir_cont)!=OK){
        printf("UNLINK:update dir content error\n");
        lc->release(parent);
        return IOERR;
    }
    printf("UNLINK::success\n");
    lc->release(parent);
    return r;
}

int yfs_client::symlink(inum parent,const char *name, const char *link, inum &ino_out)
{
    lc->acquire(parent);
    printf("SYMLINK::%s -> %s\n",name,link);
    int r = OK;
    bool found;
    inum ino;
    yfs_client::lookup(parent,name,found,ino);
    if (found==true){
        r = EXIST;
    }else{
        std::string content;
        ec->get(parent,content);
        std::string sname = name;
        content += sname + '/';
        ec->create(extent_protocol::T_SYM,ino);
        ino_out = ino;
        ec->put(ino,link);
        std::stringstream ss;
        ss<<ino;
        std::string inobuf;
        ss>>inobuf;
        content += inobuf + '/';
        ec->put(parent,content);
        printf("ino out:%d\n",ino);
    }
    lc->release(parent);
    return r;
}

int yfs_client::readlink(inum ino, std::string &link){
    int r = OK;
    r = ec->get(ino,link);
    printf("linkcontent::%s\n",link.data());
    return r;
}

