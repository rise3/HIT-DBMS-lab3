/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb {

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

	int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}

//BufMgr类的析构函数。将缓冲池中所有脏页写回磁盘，然后释放缓冲池、BufDesc表和哈希表占用的
//内存。
// BufMgr 类的析构函数，用于释放缓冲池管理器的资源
BufMgr::~BufMgr() {
    // 遍历缓冲池中的每一页
    for (FrameId i = 0; i < numBufs; i++) {
        // 检查当前页是否被修改过（dirty标志）
        if (bufDescTable[i].dirty) {
            // 如果页被修改过，则将修改后的内容写回到对应的文件中
            bufDescTable[i].file->writePage(bufPool[i]);
            bufDescTable[i].dirty = false; // 将dirty标志重置为false，表示页已经写回
        }
    }
    // 释放缓冲池哈希表的内存空间
    delete hashTable;
    // 释放缓冲描述符表的内存空间
    delete[] bufDescTable;
    // 释放缓冲池的内存空间
    delete[] bufPool;
}



//顺时针旋转时钟算法中的表针，将其指向缓冲池中下一个页框。
void BufMgr::advanceClock()
{
    clockHand = (clockHand + 1) % numBufs;
}



//使用时钟算法分配一个空闲页框。如果页框中的页面是脏的，则需要将脏页先写回磁盘。如果缓冲池
//中所有页框都被固定了(pinned)，则抛出BufferExceededException异常。allocBuf()是一个私有方
//法，它会被下面介绍的readPage()和allocPage()方法调用。请注意，如果被分配的页框中包含一个
//有效页面，则必须将该页面从哈希表中删除。最后，分配的页框的编号通过参数frame返回。
//用于分配缓冲帧
void BufMgr::allocBuf(FrameId & frame) 
{
    while (true){
        //遍历每一页
			for (int i = 0; i != (signed)numBufs; i++) {
				advanceClock();
				//当某个帧的valid位为false时，说明这个页面不可用，
				//它就可以被清理掉从而腾出需要的空闲帧，函数返回
				if (!bufDescTable[clockHand].valid) {
					frame = clockHand;
					return;
				}
				//当某个帧的refbit位为true时，说明这个页面最近被使用过并且未被替换，
				//因此该位置不是空闲帧，进入下一次循环
				if (bufDescTable[clockHand].refbit) {
					bufDescTable[clockHand].refbit = false;
					continue;
				}
				//当某个帧的pinCnt>0时，说明这个页面正在被使用，不是空闲帧
				if (bufDescTable[clockHand].pinCnt > 0) {
					continue;
				}
				//当某个帧的dirty位为true时，说明这个页面是脏的，应当将该页面写回磁盘
				if (bufDescTable[clockHand].dirty) {
					bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
				}

				try {
					if (bufDescTable[clockHand].file) {
						hashTable->remove(bufDescTable[clockHand].file,
						bufDescTable[clockHand].pageNo);
						bufDescTable[clockHand].Clear();
					}
				}catch (HashNotFoundException &e) {
					}
				frame = clockHand;
				return;
			}

			//当所有的页面都被占用时，检查是否所有页面都被锁定
			bool allPinned = false;
			for (int i = 0; i < (signed)numBufs; i++) {
				if (!bufDescTable[i].pinCnt) {
					allPinned = true;
					break;
				}
			}
            //如果所有页面都被锁定，抛出缓冲池溢出异常
			if (!allPinned) {
				throw BufferExceededException();
			}
		}
}




//首先调用哈希表的lookup()方法检查待读取的页面(file, PageNo)是否已经在缓冲池中。如果该页面
//已经在缓冲池中，则通过参数page返回指向该页面所在的页框的指针；如果该页面不在缓冲池中，则
//哈希表的lookup()方法会抛出HashNotFoundException异常。根据lookup()的返回结果，我们处理以
//下两种情况。
//– 情况1: 页面不在缓冲池中。在这种情况下，调用allocBuf()方法分配一个空闲的页框。然后，
//调用file->readPage()方法将页面从磁盘读入刚刚分配的空闲页框。接下来，将该页面插入到
//哈希表中，并调用Set()方法正确设置页框的状态，Set()会将页面的pinCnt置为1。最后，通过
//参数page返回指向该页框的指针。
//– 情况2: 页面在缓冲池中。在这种情况下，将页框的refbit置为true，并将pinCnt加1。最后，通
//过参数page返回指向该页框的指针。

	
// BufMgr 类的 readPage 函数，用于从文件中读取页到缓冲池
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page) {
    FrameId id;
    try {
        // 尝试在缓冲池中查找对应的页面
        hashTable->lookup(file, pageNo, id);
        bufDescTable[id].pinCnt++; // 将对应缓冲帧的引用计数加一
    } catch (HashNotFoundException e) {
        // 页面不在缓冲池中
        // 分配一个缓冲帧，从磁盘读取页面
        // 将页面插入哈希表，设置缓冲帧信息
        Page pageTemp = file->readPage(pageNo);
        this->allocBuf(id);
        bufPool[id] = pageTemp;
        hashTable->insert(file, pageNo, id);
        bufDescTable[id].Set(file, pageNo);
    }

    bufDescTable[id].refbit = true; // 设置 refbit 为 true，表示页面最近被访问过

    // 返回指向包含该页面的缓冲帧的指针
    page = &bufPool[id];
}




//将缓冲区中包含(file, PageNo)表示的页面所在的页框的pinCnt值减1。如果参数dirty等于true，则
//将页框的dirty位置为true。如果pinCnt值已经是0，则抛出PAGENOTPINNED异常。如果该页面不在哈
//希表中，则什么都不用做。
// BufMgr 类的 unPinPage 函数，用于取消对页面的引用
void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) {
    FrameId frameId;
    try {
        // 尝试在哈希表中查找对应的页面
        hashTable->lookup(file, pageNo, frameId);
    } catch (HashNotFoundException e) {
        // 如果页面不在哈希表中，什么都不做，直接返回
        return;
    }
    // 如果引用计数已经为 0，则抛出 PageNotPinnedException 异常
    if (bufDescTable[frameId].pinCnt == 0) {
        throw PageNotPinnedException(file->filename(), pageNo, frameId);
    }
    bufDescTable[frameId].pinCnt--; // 减少对应缓冲帧的引用计数
    // 如果 dirty 为 true，则设置对应缓冲帧的 dirty 位为 true，表示页面已经被修改过
    if (dirty) {
        bufDescTable[frameId].dirty = true;
    }
}



//扫描bufTable，检索缓冲区中所有属于文件file的页面。对每个检索到的页面，进行如下操作：(a)
//如果页面是脏的，则调用file->writePage()将页面写回磁盘，并将dirty位置为false；(b) 将页面
//从哈希表中删除；(c) 调用BufDesc类的Clear()方法将页框的状态进行重置。
//如果文件file的某些页面被固定住(pinned)，则抛出BadBufferException异常。如果检索到文件file的
//某个无效页，则抛出BadBufferException异常。
// BufMgr 类的 flushFile 函数，用于刷新指定文件的所有页面到磁盘
void BufMgr::flushFile(const File* file) {
    // 遍历缓冲池中的每一页
    for (FrameId k = 0; k < numBufs; k++) {
        // 如果缓冲帧对应的文件为目标文件
        if (bufDescTable[k].file == file) {
            // 如果缓冲帧被锁定（引用计数大于0），抛出 PagePinnedException 异常
            if (bufDescTable[k].pinCnt > 0) {
                throw PagePinnedException(file->filename(), bufDescTable[k].pageNo, k);
            }
            // 如果缓冲帧无效，抛出 BadBufferException 异常
            else if (!bufDescTable[k].valid) {
                throw BadBufferException(k, bufDescTable[k].dirty, bufDescTable[k].valid, bufDescTable[k].refbit);
            }
            // 缓冲帧可用且未被锁定
            else {
                // 如果缓冲帧是脏的，将其内容写回磁盘
                if (bufDescTable[k].dirty) {
                    bufDescTable[k].file->writePage(bufPool[k]);
                    bufDescTable[k].dirty = false;
                }
                // 从哈希表中移除缓冲帧对应的文件和页号
                hashTable->remove(file, bufDescTable[k].pageNo);
                // 清空缓冲帧的信息
                bufDescTable[k].Clear();
            }
        }
    }
}


//首先调用file->allocatePage()方法在file文件中分配一个空闲页面，file->allocatePage()返回
//这个新分配的页面。然后，调用allocBuf()方法在缓冲区中分配一个空闲的页框。接下来，在哈希表
//中插入一条项目，并调用Set()方法正确设置页框的状态。该方法既通过pageNo参数返回新分配的页
//面的页号，还通过page参数返回指向缓冲池中包含该页面的页框的指针。
// BufMgr 类的 allocPage 函数，用于在指定文件中分配一个空白页
void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) {
    FrameId frameId;
    // 在指定文件中分配一个空白页
    PageId newPageId = file->allocatePage().page_number();
    // 分配一个缓冲帧
    allocBuf(frameId);
    // 将新分配的页内容读入到缓冲帧中
    bufPool[frameId] = file->readPage(newPageId);
    // 将新页的信息插入哈希表
    hashTable->insert(file, newPageId, frameId);
    // 设置缓冲帧的信息
    bufDescTable[frameId].Set(file, newPageId);
    // 返回新分配的页号和指向缓冲帧的指针
    pageNo = newPageId;
    page = &bufPool[frameId];
}




//该方法从文件file中删除页号为pageNo的页面。在删除之前，如果该页面在缓冲池中，需要将该页面
//所在的页框清空并从哈希表中删除该页面。
// BufMgr 类的 disposePage 函数，用于释放指定文件中的一页
void BufMgr::disposePage(File* file, const PageId PageNo) {
    FrameId frameId;
    try {
        // 确保要删除的页面在缓冲池中有分配对应的缓冲帧
        hashTable->lookup(file, PageNo, frameId);
        // 清空对应缓冲帧的信息
        bufDescTable[frameId].Clear();
        // 从哈希表中移除对应的文件和页号
        hashTable->remove(file, PageNo);
    } catch (HashNotFoundException e) {
        // 捕获哈希表异常，忽略
    }
    // 调用文件对象的 deletePage 方法删除指定页
    file->deletePage(PageNo);
}


void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
