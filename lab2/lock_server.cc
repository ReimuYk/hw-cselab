// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

// area for lock
std::map<lock_protocol::lockid_t,int> locks;
static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t cond = PTHREAD_COND_INITIALIZER;

lock_server::lock_server():
  nacquire (0)
{
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
  printf("LOCKBGN(ACQ):%d\n",lid);
  lock_protocol::status ret = lock_protocol::OK;
	// Your lab2 part2 code goes here
  pthread_mutex_lock(&mutex);
  
  while(locks[lid] == 1)
    pthread_cond_wait(&cond, &mutex);
  
  r=lock_protocol::OK;
  locks[lid] = 1;
  pthread_mutex_unlock(&mutex);
  printf("LOCKEND(ACQ):%d\n",lid);
  return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  printf("LOCKBGN(RLS):%d\n",lid);
  lock_protocol::status ret = lock_protocol::OK;
	// Your lab2 part2 code goes here
  pthread_mutex_lock(&mutex);
  if (locks[lid] == 0)
    r=lock_protocol::NOENT;
  else{
    locks[lid] = 0;
    r=lock_protocol::OK;
    pthread_cond_signal(&cond);
  }
  pthread_mutex_unlock(&mutex);
  printf("LOCKEND(RLS):%d\n",lid);
  return ret;
}
