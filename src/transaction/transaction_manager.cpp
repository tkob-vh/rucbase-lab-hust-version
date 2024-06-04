#include "transaction_manager.h"
#include "record/rm_file_handle.h"

std::unordered_map<txn_id_t, Transaction *> TransactionManager::txn_map = {};

/**
 * 事务的开始方法
 * @param txn 事务指针
 * @param log_manager 日志管理器，用于日志lab
 * @return 当前事务指针
 * @tips: 事务的指针可能为空指针
 */
Transaction * TransactionManager::Begin(Transaction *txn, LogManager *log_manager) {
    // Todo:
    //**提示**：如果是新事务，需要创建一个`Transaction`对象，并把该对象的指针加入到全局事务表中。
    // 1. 判断传入事务参数是否为空指针
    // 2. 如果为空指针，创建新事务
    // 3. 把开始事务加入到全局事务表中
    // 4. 返回当前事务指针
  
    //std::cout << "Start Transaction " << next_txn_id_ << std::endl;

    if (txn == nullptr){
        txn = new Transaction(next_txn_id_, IsolationLevel::SERIALIZABLE);
        next_txn_id_ += 1;  
    }
    txn_map[txn->GetTransactionId()] = txn;

    return txn;
}

/**
 * 事务的提交方法
 * @param txn 事务指针
 * @param log_manager 日志管理器，用于日志lab
 * @param sm_manager 系统管理器，用于commit，后续会删掉
 */
void TransactionManager::Commit(Transaction * txn, LogManager *log_manager) {
    // Todo:
    //**提示**：如果并发控制算法需要申请和释放锁，那么你需要在提交阶段完成锁的释放。
    // 1. 如果存在未提交的写操作，提交所有的写操作
    // 2. 释放所有锁
    // 3. 释放事务相关资源，eg.锁集
    // 4. 更新事务状态

    if(txn == nullptr) {
      return;
    }

    // 1. 如果存在未提交的写操作，提交所有的写操作
    auto write_set = txn->GetWriteSet();
    while(!write_set->empty()) {
      // todo
      // log_manager->add_log_to_buffer(new log_record);
      write_set->pop_front();
    }
    // 2. 释放所有锁
    auto lock_set = txn->GetLockSet();
    for(auto&lock: *lock_set) {
      lock_manager_->Unlock(txn, lock);
    }
    // 3. 释放事务相关资源，eg.锁集
    for(auto& write:*txn->GetWriteSet()) {
      delete write;
    }
    txn->GetWriteSet()->clear();
    txn->GetLockSet()->clear();
    // 4. 把事务日志刷入磁盘中
    // log_manager->

    // 5. 更新事务状态
    txn->SetState(TransactionState::COMMITTED);




/*      std::unordered_set<LockDataId>* setPtr =txn->GetLockSet().get();
     if(setPtr){
      for(const auto& element:*setPtr){
        lock_manager_->Unlock(txn,element);
      }
     }
    sm_manager_->get_bpm()->FlushAllPages();
    txn->GetWriteSet()->clear();
    txn->GetDeletedPageSet()->clear();
    //txn-get_index_latch_page_set()->clear();
    txn->GetLockSet()->clear();
    //log_manager->

    
    txn->SetState(TransactionState::COMMITTED);
 */
    // Perform all deletes before we commit.
/*     auto write_set = txn->GetWriteSet();
    
    while (!write_set->empty()) {
        auto &item = write_set->back();
        auto table = item->GetTableName(); //item.table_;
        
        if (item->GetWriteType() == WType::DELETE_TUPLE) {
            // Note that this also releases the lock when holding the page latch.
            table->ApplyDelete(item.rid_, txn);
        }
        write_set->pop_back();
    }
    write_set->clear();
 */
    // Release all the locks.
    //txn->GetLockSet().
    // ReleaseLocks(txn);
    // Release the global transaction latch.
    // global_txn_latch_.RUnlock();
}

/**
 * 事务的终止方法
 * @param txn 事务指针
 * @param log_manager 日志管理器，用于日志lab
 * @param sm_manager 系统管理器，用于rollback，后续会删掉
 */
void TransactionManager::Abort(Transaction * txn, LogManager *log_manager) {
    // Todo:
    // 1. 回滚所有写操作
    // 2. 释放所有锁
    // 3. 清空事务相关资源，eg.锁集
    // 4. 更新事务状态

    if (txn == nullptr) {
      return;
    }
    // 1. 回滚所有写操作
    std::cout << "Aborting... "; // << std::endl;
    auto write_set = txn->GetWriteSet();
    std::cout << write_set->size() << " records " << std::endl;
    for(auto r_write_iter = write_set->rbegin();r_write_iter!=write_set->rend();++r_write_iter){//改成倒着读取record内容
        auto write = *r_write_iter;
//    }
//    for(auto&write:*write_set) {
      auto context = new Context(lock_manager_, log_manager, txn);
      auto tab_name = write->GetTableName();
      auto &table =  sm_manager_->fhs_.at(tab_name);
      switch (write->GetWriteType()) {
      case WType::INSERT_TUPLE: {
        auto rec = table->get_record(write->GetRid(),context); // 获取到插入的记录
        std::cout << tab_name << ": inserted record is deleted ..." << std::endl;
        
/*         for(const auto& index:sm_manager_->db_.get_table(tab_name).indexes) {
          auto index_name = sm_manager_->get_ix_manager()->get_index_name(tab_name,index.cols);
          auto &index_handler = sm_manager_->ihs_.at(index_name);
          index_handler->delete_entry(rec->key_from_rec(index.cols)->data,txn); // check 这里是否能将之前的txn传入
        }
  */
        table->delete_record(write->GetRid(),context);
        break;
      }
      case WType::DELETE_TUPLE: {
        auto old_rec = write->GetRecord();
        auto rid = table->insert_record(old_rec.data,context);
        std::cout << tab_name << ": deleted record is inserted ..." << std::endl;

/*         for(const auto& index:sm_manager_->db_.get_table(tab_name).indexes) {
          auto index_name = sm_manager_->get_ix_manager()->get_index_name(tab_name,index.cols);
          auto &index_handler = sm_manager_->ihs_.at(index_name);
          index_handler->insert_entry(old_rec.key_from_rec(index.cols)->data,rid,txn); // check 这里是否能将之前的txn传入
        }
 */        break;
      }

      case WType::UPDATE_TUPLE:
        auto old_rec = write->GetRecord();
        auto new_rec = table->get_record(write->GetRid(),context);
        std::cout << tab_name << ": updated record changed backward ..." << std::endl;

/*         for(const auto& index:sm_manager_->db_.get_table(tab_name).indexes) {
          auto index_name = sm_manager_->get_ix_manager()->get_index_name(tab_name,index.cols);
          auto &index_handler = sm_manager_->ihs_.at(index_name);
          index_handler->delete_entry(new_rec->key_from_rec(index.cols)->data,txn);
        }
 */        auto rid = write->GetRid();
        table->update_record(rid, old_rec.data, context);
 /*        for(const auto& index:sm_manager_->db_.get_table(tab_name).indexes) {
          auto index_name = sm_manager_->get_ix_manager()->get_index_name(tab_name,index.cols);
          auto &index_handler = sm_manager_->ihs_.at(index_name);
          index_handler->insert_entry(old_rec.key_from_rec(index.cols)->data,rid,txn);
        }
 */        break;
      }
      delete write;
    }
    write_set->clear();

    // 2. 释放所有锁
    auto lock_set = txn->GetLockSet();
    for(auto&lock: *lock_set) {
      lock_manager_->Unlock(txn, lock);
    }
    lock_set->clear();
    // 3. 清空事务相关资源，eg.锁集
    for(auto& write:*txn->GetWriteSet()) {
      delete write;
    }
    
    txn->GetLockSet()->clear();
    txn->GetPageSet()->clear();
    // 4. 把事务日志刷入磁盘中
    // 5. 更新事务状态
    txn->SetState(TransactionState::ABORTED);


/*     while(!txn->GetWriteSet()->empty()){
        WriteRecord * write_recode=txn->GetWriteSet()->back();
        RmFileHandle *fh=sm_manager_->fhs_[write_recode->GetTableName()].get();
        bool is_index = write_recode->GetIsIndex();
     if(write_recode->GetWriteType()==WType::INSERT_TUPLE && !is_index){
          fh->delete_record(write_recode->GetRid(),nullptr);
     }
     else if(write_recode->GetWriteType()==WType::DELETE_TUPLE && !is_index){
       fh->insert_record(write_recode->GetRecord().data,nullptr);
     }
     else if(write_recode->GetWriteType()==WType::UPDATE_TUPLE && !is_index){
        fh->update_record(write_recode->GetRid(),write_recode->GetRecord().data,nullptr);
        
     }
     else if(write_recode->GetWriteType()==WType::INSERT_TUPLE && is_index){
           IxIndexHandle *ix=sm_manager_->ihs_[write_recode->GetIndexName()].get();
           ix->delete_entry(write_recode->GetRecord().data,nullptr);
     }
     else if(write_recode->GetWriteType()==WType::DELETE_TUPLE && is_index){
           IxIndexHandle *ix=sm_manager_->ihs_[write_recode->GetIndexName()].get();
           ix->insert_entry(write_recode->GetRecord().data,write_recode->GetRid(),nullptr);
     }
        txn->get_write_set()->pop_back();
        }
 */
    //释放锁

/*     std::unordered_set<LockDataId>* setPtr =txn->get_lock_set().get();
     if(setPtr){
      for(const auto& element:*setPtr){
        lock_manager_->unlock(txn,element);
      }
     }
    txn->get_write_set()->clear();
     txn->get_index_deleted_page_set()->clear();
    txn->get_index_latch_page_set()->clear();
    txn->get_lock_set()->clear();
    log_manager->flush_log_to_disk();
    txn->set_state(TransactionState::ABORTED);
 */
}

/** 以下函数用于日志实验中的checkpoint */
void TransactionManager::BlockAllTransactions() {}

void TransactionManager::ResumeAllTransactions() {}

