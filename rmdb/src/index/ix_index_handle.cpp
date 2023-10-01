#include "ix_index_handle.h"

#include "ix_scan.h"

IxIndexHandle::IxIndexHandle(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, int fd)
    : disk_manager_(disk_manager), buffer_pool_manager_(buffer_pool_manager), fd_(fd) {
    // init file_hdr_
    disk_manager_->read_page(fd, IX_FILE_HDR_PAGE, (char *)&file_hdr_, sizeof(file_hdr_));
    // disk_manager管理的fd对应的文件中，设置从原来编号+1开始分配page_no
    disk_manager_->set_fd2pageno(fd, disk_manager_->get_fd2pageno(fd) + 1);
}

/**
 * @brief 用于查找指定键所在的叶子结点
 *
 * @param key 要查找的目标key值
 * @param operation 查找到目标键值对后要进行的操作类型
 * @param transaction 事务参数，如果不需要则默认传入nullptr
 * @return 返回目标叶子结点
 * @note need to Unpin the leaf node outside!
 */
IxNodeHandle *IxIndexHandle::FindLeafPage(const char *key, Operation operation, Transaction *transaction) {
    // Todo:
    // 1. 获取根节点
    // 2. 从根节点开始不断向下查找目标key
    // 3. 找到包含该key值的叶子结点停止查找，并返回叶子节点
    IxNodeHandle * cur_node = FetchNode(file_hdr_.root_page);//获得根节点
    while(!cur_node->page_hdr->is_leaf){//直到找到了对应的叶子节点
        // printf("当前的根节点是%d\n",file_hdr_.root_page);
        // printf("当前的page_no是%d\n",cur_node->GetPageNo());
        page_id_t page_no_now = cur_node->InternalLookup(key);
        // printf("得到的page_no是%d\n",page_no_now);
        //unpin page
        buffer_pool_manager_->UnpinPage({fd_,page_no_now},false);
        //更新cur_node
        cur_node = FetchNode(page_no_now);        
    }
    //注意这里现在不用unpin 叶子节点，因为叶子节点现在还没有用完，
    // buffer_pool_manager_->UnpinPage(PageId(fd_,page_no_now),false);
    return cur_node;
}

/**
 * @brief 用于查找指定键在叶子结点中的对应的值result
 *
 * @param key 查找的目标key值
 * @param result 用于存放结果的容器
 * @param transaction 事务指针
 * @return bool 返回目标键值对是否存在
 */
bool IxIndexHandle::GetValue(const char *key, std::vector<Rid> *result, Transaction *transaction) {
    // Todo:
    // 1. 获取目标key值所在的叶子结点
    // 2. 在叶子节点中查找目标key值的位置，并读取key对应的rid
    // 3. 把rid存入result参数中
    // 提示：使用完buffer_pool提供的page之后，记得unpin page；记得处理并发的上锁
    std::scoped_lock lock{root_latch_};
    IxNodeHandle * target_leaf = FindLeafPage(key,Operation::FIND,transaction);
    Rid *rid_now = new Rid;
    bool is_find = target_leaf->LeafLookup(key,&rid_now);
    if(is_find)
        result->push_back(*rid_now);
    buffer_pool_manager_->UnpinPage(target_leaf->GetPageId(),false);
    return is_find;
}

/**
 * @brief 将指定键值对插入到B+树中
 *
 * @param (key, value) 要插入的键值对
 * @param transaction 事务指针
 * @return 是否插入成功
 */
bool IxIndexHandle::insert_entry(const char *key, const Rid &value, Transaction *transaction) {
    // Todo:
    // 1. 查找key值应该插入到哪个叶子节点
    // 2. 在该叶子节点中插入键值对
    // 3. 如果结点已满，分裂结点，并把新结点的相关信息插入父节点
    // 提示：记得unpin page；若当前叶子节点是最右叶子节点，则需要更新file_hdr_.last_leaf；记得处理并发的上锁
    std::scoped_lock lock{root_latch_};
    IxNodeHandle *insert_node = FindLeafPage(key,Operation::INSERT,transaction);//注意我们招到的这个节点还在被pin住，没有释放
    // printf("过了InsertEntry的findleafpage\n");
    int num_after_insert = insert_node->Insert(key,value);
    // printf("插入后的num为%d\n",num_after_insert);
    if( insert_node->IsLeafPage() && (insert_node->GetSize() >= (insert_node->GetMaxSize() - 1))){//如果叶子节点大于等于btree_order，则分裂
        IxNodeHandle *new_node = Split(insert_node);
        InsertIntoParent(insert_node,new_node->get_key(0),new_node,transaction);
        if(file_hdr_.last_leaf == insert_node->GetPageNo()){
            file_hdr_.last_leaf = new_node->GetPageNo();
        }
        buffer_pool_manager_->UnpinPage(new_node->GetPageId(),true);
    }
    if( ! insert_node->IsLeafPage() && (insert_node->GetSize() >= insert_node->GetMaxSize())){//如果非叶子节点大于等于MaxSize，则分裂
        IxNodeHandle *new_node = Split(insert_node);
        InsertIntoParent(insert_node,new_node->get_key(0),new_node,transaction);
        buffer_pool_manager_->UnpinPage(new_node->GetPageId(),true);
    }
    if(insert_node->GetPageNo() == file_hdr_.first_leaf && ix_compare(insert_node->get_key(0),key,file_hdr_.col_type,file_hdr_.col_len) == 0){
        maintain_parent(insert_node);
    }
    buffer_pool_manager_->UnpinPage(insert_node->GetPageId(),true);

    return true;
}

/**
 * @brief 将传入的一个node拆分(Split)成两个结点，在node的右边生成一个新结点new node
 *
 * @param node 需要拆分的结点
 * @return 拆分得到的new_node
 * @note 本函数执行完毕后，原node和new node都需要在函数外面进行unpin
 */
IxNodeHandle *IxIndexHandle::Split(IxNodeHandle *node) {
    // Todo:
    // 1. 将原结点的键值对平均分配，右半部分分裂为新的右兄弟结点
    //    需要初始化新节点的page_hdr内容
    // 2. 如果新的右兄弟结点是叶子结点，更新新旧节点的prev_leaf和next_leaf指针
    //    为新节点分配键值对，更新旧节点的键值对数记录
    // 3. 如果新的右兄弟结点不是叶子结点，更新该结点的所有孩子结点的父节点信息(使用IxIndexHandle::maintain_child())
    IxNodeHandle * new_node = CreateNode();//新建的节点
    int old_num = node->GetSize();
    new_node->page_hdr->next_free_page_no = IX_NO_PAGE;
    new_node->page_hdr->parent = IX_NO_PAGE;
    new_node->page_hdr->num_key = 0;
    int left_num = -1;
    if(node->IsLeafPage()){
        new_node->page_hdr->is_leaf = true;
        left_num = (node->GetMaxSize() - 1)/2;
    }else{
        new_node->page_hdr->is_leaf = false;
        left_num = node->GetMinSize();
    }
    
    //开始分配
    new_node->insert_pairs(0,node->get_key(left_num),node->get_rid(left_num),node->GetMaxSize()-node->GetMinSize());
    // printf("分裂后的new_node的第一个值是%d,第二个值是%d\n",*new_node->get_key(0),*new_node->get_key(1));
    node->SetSize(left_num);
    // printf("Split之后oldnode的值Size是%d\n",node->GetSize());
    new_node->SetSize(old_num-left_num);
    // printf("Split之后newnode的值Size是%d\n",new_node->GetSize());
    if(node->IsLeafPage()){
        // printf("应该进入是叶子的情况\n");
        new_node->page_hdr->is_leaf = true;
        new_node->SetNextLeaf(node->GetNextLeaf());
        new_node->SetPrevLeaf(node->GetPageNo());
        IxNodeHandle *node_next = FetchNode(node->GetNextLeaf());
        node_next->SetPrevLeaf(new_node->GetPageNo());
        node->SetNextLeaf(new_node->GetPageNo());
        
    }else{
        new_node->page_hdr->is_leaf = false;
        for(int i = 0; i < new_node->GetSize(); ++i){
            maintain_child(new_node,i);
        }

    }
    new_node->SetParentPageNo(node->GetParentPageNo());
    return new_node;
}

/**
 * @brief Insert key & value pair into internal page after split
 * 拆分(Split)后，向上找到old_node的父结点
 * 将new_node的第一个key插入到父结点，其位置在 父结点指向old_node的孩子指针 之后
 * 如果插入后>=maxsize，则必须继续拆分父结点，然后在其父结点的父结点再插入，即需要递归
 * 直到找到的old_node为根结点时，结束递归（此时将会新建一个根R，关键字为key，old_node和new_node为其孩子）
 *
 * @param (old_node, new_node) 原结点为old_node，old_node被分裂之后产生了新的右兄弟结点new_node
 * @param key 要插入parent的key
 * @note 一个结点插入了键值对之后需要分裂，分裂后左半部分的键值对保留在原结点，在参数中称为old_node，
 * 右半部分的键值对分裂为新的右兄弟节点，在参数中称为new_node（参考Split函数来理解old_node和new_node）
 * @note 本函数执行完毕后，new node和old node都需要在函数外面进行unpin
 */
void IxIndexHandle::InsertIntoParent(IxNodeHandle *old_node, const char *key, IxNodeHandle *new_node,
                                     Transaction *transaction) {
    // Todo:
    // 1. 分裂前的结点（原结点, old_node）是否为根结点，如果为根结点需要分配新的root
    // 2. 获取原结点（old_node）的父亲结点
    // 3. 获取key对应的rid，并将(key, rid)插入到父亲结点
    // 4. 如果父亲结点仍需要继续分裂，则进行递归插入
    // 提示：记得unpin page
    if(file_hdr_.root_page == old_node->GetPageId().page_no){
        // printf("进入创建了新的根节点\n");
        //初始化，所有的新创建节点都要按照此初始化
        IxNodeHandle *new_root = CreateNode();
        new_root->page_hdr->next_free_page_no = IX_NO_PAGE;
        new_root->page_hdr->parent = IX_NO_PAGE;
        new_root->page_hdr->num_key = 0;
        new_root->page_hdr->is_leaf = false;

        //建立关系
        Rid old_node_rid = {old_node->GetPageNo(),-1};//rid后面这个slot num貌似不太重要？因为在索引页里面我们只需要知道page_no能够找到那个页就够了，在leaf里面貌似才需要rid?
        Rid new_node_rid = {new_node->GetPageNo(),-1};
        // new_root->insert_pair(0,old_node->get_key(0),old_node_rid);
        new_root->Insert(old_node->get_key(0),old_node_rid);
        // new_root->insert_pair(1,new_node->get_key(0),new_node_rid);
        new_root->Insert(new_node->get_key(0),new_node_rid);
        new_node->SetParentPageNo(new_root->GetPageNo());
        old_node->SetParentPageNo(new_root->GetPageNo());
        file_hdr_.root_page = new_root->GetPageNo();
        buffer_pool_manager_->UnpinPage(new_root->GetPageId(),true);
        // printf("新根的num_key是%d\n",new_root->GetSize());
        return ;
    }
    // printf("未创建新节点，直接往\n");
    IxNodeHandle *parent_node = FetchNode(old_node->GetParentPageNo());
    int pos = parent_node->find_child(old_node);
    Rid new_node_rid = {new_node->GetPageNo(),-1};
    parent_node->insert_pair(pos+1,new_node->get_key(0),new_node_rid);
    parent_node->SetSize(parent_node->GetSize() + 1);
    if(parent_node->GetSize() >= parent_node->GetMaxSize()){
        // printf("----------------进入了递归过程-------------------------\n");
        IxNodeHandle * new_parent_node = Split(parent_node);
        InsertIntoParent(parent_node,new_node->get_key(0),new_parent_node,transaction);
    }

    buffer_pool_manager_->UnpinPage(parent_node->GetPageId(),true);
    return ;

}

/**
 * @brief 用于删除B+树中含有指定key的键值对
 *
 * @param key 要删除的key值
 * @param transaction 事务指针
 * @return 是否删除成功
 */
bool IxIndexHandle::delete_entry(const char *key, Transaction *transaction) {
    // Todo:
    // 1. 获取该键值对所在的叶子结点
    // 2. 在该叶子结点中删除键值对
    // 3. 如果删除成功需要调用CoalesceOrRedistribute来进行合并或重分配操作，并根据函数返回结果判断是否有结点需要删除
    // 4. 如果需要并发，并且需要删除叶子结点，则需要在事务的delete_page_set中添加删除结点的对应页面；记得处理并发的上锁
    std::scoped_lock lock{root_latch_};
    IxNodeHandle *delete_node = FindLeafPage(key, Operation::DELETE, transaction);
    int num_before_delete = delete_node->GetSize();
    char * first_key_before_delete = delete_node->get_key(0);
    int before = *first_key_before_delete;
    delete_node->Remove(key);
    int num_after_delete = delete_node->GetSize();
    if(num_before_delete == num_after_delete){
        buffer_pool_manager_->UnpinPage(delete_node->GetPageId(),true);
        return false;
    }
    int is_delete = false;
    if(( (delete_node->IsLeafPage()) && (delete_node->GetSize() < (delete_node->GetMinSize() - 1)) && (!delete_node->IsRootPage()) ) || ( (delete_node->IsLeafPage()) && (delete_node->GetSize() < 2) && (delete_node->IsRootPage()) ) ){//叶子节点，且少于了最少节点，那么就要合并或者。。
        is_delete = CoalesceOrRedistribute(delete_node,transaction);
        
    }else{//如果删除了之后没事的话，那么就看删除的是不是第一个节点
        //不在这里做，在下面统一做
    }
    //最后都要看一下是不是第一个节点变了，并且保持一下
    //先说明一下，不一定对
    char * first_key_after_delete = delete_node->get_key(0);
    int after = *first_key_after_delete;
    if((before != after) && !is_delete){
        // printf("进入了entry的保持parent\n");
        maintain_parent(delete_node);
        // printf("过了delete_entry的保持parent\n");
    }
    buffer_pool_manager_->UnpinPage(delete_node->GetPageId(),true);
    return true;
}

/**
 * @brief 用于处理合并和重分配的逻辑，用于删除键值对后调用
 *
 * @param node 执行完删除操作的结点
 * @param transaction 事务指针
 * @param root_is_latched 传出参数：根节点是否上锁，用于并发操作
 * @return 是否需要删除结点
 * @note User needs to first find the sibling of input page.
 * If sibling's size + input page's size >= 2 * page's minsize, then redistribute.
 * Otherwise, merge(Coalesce).
 */
bool IxIndexHandle::CoalesceOrRedistribute(IxNodeHandle *node, Transaction *transaction) {
    // Todo:
    // 1. 判断node结点是否为根节点
    //    1.1 如果是根节点，需要调用AdjustRoot() 函数来进行处理，返回根节点是否需要被删除
    //    1.2 如果不是根节点，并且不需要执行合并或重分配操作，则直接返回false，否则执行2
    // 2. 获取node结点的父亲结点
    // 3. 寻找node结点的兄弟结点（优先选取前驱结点）
    // 4. 如果node结点和兄弟结点的键值对数量之和，能够支撑两个B+树结点（即node.size+neighbor.size >=
    // NodeMinSize*2)，则只需要重新分配键值对（调用Redistribute函数）
    // 5. 如果不满足上述条件，则需要合并两个结点，将右边的结点合并到左边的结点（调用Coalesce函数）
    if(node->IsRootPage()){
        return AdjustRoot(node);
    }else{//不是根节点
        if(((node->GetSize() < (node->GetMinSize() - 1)) && (node->IsLeafPage())) || ((node->GetSize() < (node->GetMinSize())) && (!node->IsLeafPage()) )){//表示需要进行合并或者借兄弟
            //获得父节点
            // printf("进入了不是根节点\n");
            IxNodeHandle * parent_node = FetchNode(node->GetParentPageNo());
            int child_id = parent_node->find_child(node);
            // printf("被删除的节点是父母的第%d个孩子\n",child_id);
            bool flag = 0;//表示有左兄弟
            if(child_id > 0 ){//表明他有左兄弟,
                // printf("它有左兄弟\n");
                flag = 1;
                int brother_id  = child_id - 1;
                IxNodeHandle *left_brother = FetchNode(parent_node->ValueAt(brother_id));
                if(((left_brother->GetSize() <= (left_brother->GetMinSize() - 1)) && (left_brother->IsLeafPage())) || ((left_brother->GetSize() <= (left_brother->GetMinSize())) && (!left_brother->IsLeafPage()) )){
                    //表示brother的节点也不够用了
                    //先不管，跳过这里，看看后面右兄弟够不够
                    //注意，这里加了等号
                }else{//表示左兄弟的节点够用，那么合并他们
                    // printf("重分配左兄弟\n");
                    Redistribute(left_brother,node,parent_node,child_id);//合并
                    buffer_pool_manager_->UnpinPage(parent_node->GetPageId(),true);
                    buffer_pool_manager_->UnpinPage(left_brother->GetPageId(),true);
                    return false;
                }

            }
            if(child_id < (parent_node->GetSize()-1)){//表明他有右兄弟,无左兄弟
                // printf("五左兄弟，有右兄弟\n");
                int brother_id = child_id + 1;
                IxNodeHandle *right_brother = FetchNode(parent_node->ValueAt(brother_id));
                if(((right_brother->GetSize() <= (right_brother->GetMinSize() - 1)) && (right_brother->IsLeafPage())) || ((right_brother->GetSize() <= (right_brother->GetMinSize())) && (!right_brother->IsLeafPage()) )){
                    //表示right brother也不够用了，先不管，后面看

                }else{
                    // printf("重分配右兄弟\n");
                    Redistribute(right_brother,node,parent_node,child_id);
                    buffer_pool_manager_->UnpinPage(parent_node->GetPageId(),true);
                    buffer_pool_manager_->UnpinPage(right_brother->GetPageId(),true);
                    return false;
                }

            }
            // if{//表示他只有自己一个节点，貌似不会存在这样的情况？先放着把，但可能会出现这样的中间状态，不过不稳定
            //     printf("难道应该到这里？？？？？？？？？？？？？？？？？？？、\n");
            //     return false;//应该不会出现这种情况，因为无论如何父节点一定是非叶子，那么它至少有两个孩子，不会出现孩子无兄弟的情况
            // }
            //前面的两个分支判断完了只合并不用下拉父母的情况,下面看下拉父母的情况，有左兄弟有限合并左兄弟
            if(flag){//如果有左兄弟,优先去合并左兄弟
                // printf("合并左兄弟\n");
                // printf("去合并左兄弟\n");
                int brother_id  = child_id - 1;
                IxNodeHandle *left_brother = FetchNode(parent_node->ValueAt(brother_id));
                bool is_parent_need_delete = Coalesce(&left_brother, &node, &parent_node,child_id,transaction);
                buffer_pool_manager_->UnpinPage(parent_node->GetPageId(),true);
                buffer_pool_manager_->UnpinPage(left_brother->GetPageId(),true);
                //应该还要继续处理删除parent,但是还未处理

                // printf("合并完左兄弟\n");

                return true;
            }else{//无左兄弟，则去合并右兄弟
                // printf("去合并右兄弟\n");
                int brother_id = child_id + 1;
                IxNodeHandle *right_brother = FetchNode(parent_node->ValueAt(brother_id));
                bool is_parent_need_delete = Coalesce(&right_brother, &node, &parent_node,child_id,transaction);
                buffer_pool_manager_->UnpinPage(parent_node->GetPageId(),true);
                buffer_pool_manager_->UnpinPage(right_brother->GetPageId(),true);
                //应该要继续处理删除parent,但是还没有处理

                // printf("合并完右兄弟\n");


                return true;
            }

        }else{//不需要合并或者借兄弟直接完事
        // printf("无需合并\n");
            return false;
        }
    }
}

/**
 * @brief 用于当根结点被删除了一个键值对之后的处理
 *
 * @param old_root_node 原根节点
 * @return bool 根结点是否需要被删除
 * @note size of root page can be less than min size and this method is only called within coalesceOrRedistribute()
 */
bool IxIndexHandle::AdjustRoot(IxNodeHandle *old_root_node) {
    // Todo:
    // 1. 如果old_root_node是内部结点，并且大小为1，则直接把它的孩子更新成新的根结点
    // 2. 如果old_root_node是叶结点，且大小为0，则直接更新root page
    // 3. 除了上述两种情况，不需要进行操作
    if( (!old_root_node->IsLeafPage()) && (old_root_node->GetSize() == 1) ){
        // printf("根是内部节点\n");
        IxNodeHandle *new_root = FetchNode(old_root_node->ValueAt(0));
        //将它更新成新根
        new_root->SetParentPageNo(-1);//置为根
        file_hdr_.root_page = new_root->GetPageNo();
        // file_hdr_.first_free_page_no = old_root_node->GetPageNo();
        if(old_root_node->IsLeafPage() && (old_root_node->GetPageNo() == file_hdr_.first_leaf)){
            file_hdr_.first_leaf = old_root_node->GetNextLeaf();
        }
        if(old_root_node->IsLeafPage() && (old_root_node->GetPageNo() == file_hdr_.last_leaf)){
            file_hdr_.last_leaf = old_root_node->GetPrevLeaf();
        }
        release_node_handle(*old_root_node);

        buffer_pool_manager_->UnpinPage(new_root->GetPageId(),true);
        return true;

    }else if(old_root_node->IsLeafPage() && old_root_node->GetSize() == 0){
        // printf("根是叶子节点\n");
        erase_leaf(old_root_node);
        file_hdr_.root_page = -1;//表示没有根了
        if(old_root_node->IsLeafPage() && (old_root_node->GetPageNo() == file_hdr_.first_leaf)){
            file_hdr_.first_leaf = old_root_node->GetNextLeaf();
        }
        if(old_root_node->IsLeafPage() && (old_root_node->GetPageNo() == file_hdr_.last_leaf)){
            file_hdr_.last_leaf = old_root_node->GetPrevLeaf();
        }
        release_node_handle(*old_root_node);
        return true;
    }else{
        return false;
    }
}

/**
 * @brief 重新分配node和兄弟结点neighbor_node的键值对
 * Redistribute key & value pairs from one page to its sibling page. If index == 0, move sibling page's first key
 * & value pair into end of input "node", otherwise move sibling page's last key & value pair into head of input "node".
 *
 * @param neighbor_node sibling page of input "node"
 * @param node input from method coalesceOrRedistribute()
 * @param parent the parent of "node" and "neighbor_node"
 * @param index node在parent中的rid_idx
 * @note node是之前刚被删除过一个key的结点
 * index=0，则neighbor是node后继结点，表示：node(left)      neighbor(right)
 * index>0，则neighbor是node前驱结点，表示：neighbor(left)  node(right)
 * 注意更新parent结点的相关kv对
 */
void IxIndexHandle::Redistribute(IxNodeHandle *neighbor_node, IxNodeHandle *node, IxNodeHandle *parent, int index) {
    // Todo:
    // 1. 通过index判断neighbor_node是否为node的前驱结点
    // 2. 从neighbor_node中移动一个键值对到node结点中
    // 3. 更新父节点中的相关信息，并且修改移动键值对对应孩字结点的父结点信息（maintain_child函数）
    // 注意：neighbor_node的位置不同，需要移动的键值对不同，需要分类讨论
    if(index < parent->find_child(neighbor_node)){//表示它与右兄弟重分配
    // printf("与右兄弟重新分配\n");
        memcpy(node->keys + file_hdr_.col_len * node->GetSize(), neighbor_node->keys, file_hdr_.col_len);
        memcpy(node->rids + node->GetSize(), neighbor_node->rids,sizeof(Rid));
        node->SetSize(node->GetSize() + 1);
        memmove(neighbor_node->keys, neighbor_node->keys + file_hdr_.col_len, file_hdr_.col_len * (neighbor_node->GetSize() - 1));
        memmove(neighbor_node->rids, neighbor_node->rids + 1, sizeof(Rid) * (neighbor_node->GetSize() - 1));
        neighbor_node->SetSize(neighbor_node->GetSize() - 1);
        maintain_parent(neighbor_node);
        // printf("neighbor page_no:%d, parent page_no %d\n",neighbor_node->GetPageNo(),parent->GetPageNo());
        // printf("可以通过maintain_parent\n");
        for (int i = 0; i < node->GetSize(); ++i){
            maintain_child(node,i);
        }
    }else{//表示它与左兄弟重分配
    // printf("与左兄弟重分配\n");
        memmove(node->keys + file_hdr_.col_len, node->keys, file_hdr_.col_len * node->GetSize());
        memmove(node->rids + 1, node->rids, sizeof(Rid) * node->GetSize());
        memcpy(node->keys, neighbor_node->keys + file_hdr_.col_len * (neighbor_node->GetSize() - 1), file_hdr_.col_len);
        memcpy(node->rids, neighbor_node->rids + (neighbor_node->GetSize() - 1), sizeof(Rid));
        node->SetSize(node->GetSize() + 1);
        neighbor_node->SetSize(neighbor_node->GetSize() - 1);
        maintain_parent(node);
        // printf("neighbor page_no:%d, parent page_no %d\n",neighbor_node->GetPageNo(),parent->GetPageNo());
        // printf("可以通过maintain_parent\n");
        for (int i = 0; i < node->GetSize(); ++i){
            maintain_child(node,i);
        }
    }
    // printf("通过了redistribute\n");
}

/**
 * @brief 合并(Coalesce)函数是将node和其直接前驱进行合并，也就是和它左边的neighbor_node进行合并；
 * 假设node一定在右边。如果上层传入的index=0，说明node在左边，那么交换node和neighbor_node，保证node在右边；合并到左结点，实际上就是删除了右结点；
 * Move all the key & value pairs from one page to its sibling page, and notify buffer pool manager to delete this page.
 * Parent page must be adjusted to take info of deletion into account. Remember to deal with coalesce or redistribute
 * recursively if necessary.
 *
 * @param neighbor_node sibling page of input "node" (neighbor_node是node的前结点)
 * @param node input from method coalesceOrRedistribute() (node结点是需要被删除的)
 * @param parent parent page of input "node"
 * @param index node在parent中的rid_idx
 * @return true means parent node should be deleted, false means no deletion happend
 * @note Assume that *neighbor_node is the left sibling of *node (neighbor -> node)
 */
bool IxIndexHandle::Coalesce(IxNodeHandle **neighbor_node, IxNodeHandle **node, IxNodeHandle **parent, int index,
                             Transaction *transaction) {
    // Todo:
    // 1. 用index判断neighbor_node是否为node的前驱结点，若不是则交换两个结点，让neighbor_node作为左结点，node作为右结点
    // 2. 把node结点的键值对移动到neighbor_node中，并更新node结点孩子结点的父节点信息（调用maintain_child函数）
    // 3. 释放和删除node结点，并删除parent中node结点的信息，返回parent是否需要被删除
    // 提示：如果是叶子结点且为最右叶子结点，需要更新file_hdr_.last_leaf
    if(index < (*parent)->find_child((*neighbor_node))){//如果Node为左节点，则交换
        // printf("交换了一下\n");
        IxNodeHandle ** tmp = neighbor_node;
        neighbor_node = node;
        node = tmp;
        index ++;
    }
    //前面已经交换完了，只是换了个名字，这就是为什么这个函数要传入二级指针的原因
    //现在，我们都默认为左边是neighbor 右边是node
    int col_len = file_hdr_.col_len;
    memcpy((*neighbor_node)->keys + (*neighbor_node)->GetSize() * col_len, (*node)->keys, col_len * (*node)->GetSize());
    memcpy((*neighbor_node)->rids + (*neighbor_node)->GetSize(), (*node)->rids, sizeof(Rid) * (*node)->GetSize());
    (*neighbor_node)->SetSize((*neighbor_node)->GetSize() + (*node)->GetSize());
    // printf("neighbor的最新Size为%d\n",(*neighbor_node)->GetSize());
    for(int i = 0; i < (*neighbor_node)->GetSize(); ++i){
        // printf("%d ",i);
        maintain_child(*neighbor_node, i);
    }
    // printf("\n过了maintain_child\n");
    if((*node)->IsLeafPage()){
        erase_leaf(*node);
    }
    // printf("过了eraseleaf\n");
    (*node)->SetSize(0);
    
    //下面删除和释放node

    // IxNodeHandle *realease_node_parent = FetchNode((*node)->GetParentPageNo());
    // int child_idx = (*parent)->find_child(*node);
    if((*node)->IsLeafPage() && ((*node)->GetPageNo() == file_hdr_.first_leaf)){
        file_hdr_.first_leaf = (*node)->GetNextLeaf();
    }
     if((*node)->IsLeafPage() && ((*node)->GetPageNo() == file_hdr_.last_leaf)){
        file_hdr_.last_leaf = (*node)->GetPrevLeaf();
    }
    release_node_handle(*(*node));//释放node
    if(((*parent)->GetPageNo() == 7 && (*parent)->GetSize() == 3)){
        // printf("删除7是理应到**********************************************\n");
        (*parent)->erase_pair(index);
    }else{
        (*parent)->erase_pair(index);
    }
    //不知道加不加maintain_parent
    maintain_parent(*neighbor_node);
    // printf("过了maintain_parent\n");
    //下面判断他是不是需要递归的进行合并和借兄弟，由于他是父母节点，故而一定是非叶子
    if((*parent)->GetSize() < (*parent)->GetMinSize()){
        // printf("进入了递归\n");
        return CoalesceOrRedistribute(*parent,transaction);
    }
    return false;
}

/** -- 以下为辅助函数 -- */
/**
 * @brief 获取一个指定结点
 *
 * @param page_no
 * @return IxNodeHandle*
 * @note pin the page, remember to unpin it outside!
 */
IxNodeHandle *IxIndexHandle::FetchNode(int page_no) const {
    // assert(page_no < file_hdr_.num_pages); // 不再生效，由于删除操作，page_no可以大于个数
    Page *page = buffer_pool_manager_->FetchPage(PageId{fd_, page_no});
    IxNodeHandle *node = new IxNodeHandle(&file_hdr_, page);
    return node;
}

/**
 * @brief 创建一个新结点
 *
 * @return IxNodeHandle*
 * @note pin the page, remember to unpin it outside!
 * 注意：对于Index的处理是，删除某个页面后，认为该被删除的页面是free_page
 * 而first_free_page实际上就是最新被删除的页面，初始为IX_NO_PAGE
 * 在最开始插入时，一直是create node，那么first_page_no一直没变，一直是IX_NO_PAGE
 * 与Record的处理不同，Record将未插入满的记录页认为是free_page
 */
IxNodeHandle *IxIndexHandle::CreateNode() {
    file_hdr_.num_pages++;
    PageId new_page_id = {.fd = fd_, .page_no = INVALID_PAGE_ID};
    // 从3开始分配page_no，第一次分配之后，new_page_id.page_no=3，file_hdr_.num_pages=4
    Page *page = buffer_pool_manager_->NewPage(&new_page_id);
    // 注意，和Record的free_page定义不同，此处【不能】加上：file_hdr_.first_free_page_no = page->GetPageId().page_no
    IxNodeHandle *node = new IxNodeHandle(&file_hdr_, page);
    return node;
}

/**
 * @brief 从node开始更新其父节点的第一个key，一直向上更新直到根节点
 *
 * @param node
 */
void IxIndexHandle::maintain_parent(IxNodeHandle *node) {
    IxNodeHandle *curr = node;
    while (curr->GetParentPageNo() != IX_NO_PAGE) {
        // Load its parent
        IxNodeHandle *parent = FetchNode(curr->GetParentPageNo());
        int rank = parent->find_child(curr);
        char *parent_key = parent->get_key(rank);
        // char *child_max_key = curr.get_key(curr.page_hdr->num_key - 1);
        char *child_first_key = curr->get_key(0);
        if (memcmp(parent_key, child_first_key, file_hdr_.col_len) == 0) {
            assert(buffer_pool_manager_->UnpinPage(parent->GetPageId(), true));
            break;
        }
        memcpy(parent_key, child_first_key, file_hdr_.col_len);  // 修改了parent node
        curr = parent;

        assert(buffer_pool_manager_->UnpinPage(parent->GetPageId(), true));
    }
}

/**
 * @brief 要删除leaf之前调用此函数，更新leaf前驱结点的next指针和后继结点的prev指针
 *
 * @param leaf 要删除的leaf
 */
void IxIndexHandle::erase_leaf(IxNodeHandle *leaf) {
    assert(leaf->IsLeafPage());

    IxNodeHandle *prev = FetchNode(leaf->GetPrevLeaf());
    prev->SetNextLeaf(leaf->GetNextLeaf());
    buffer_pool_manager_->UnpinPage(prev->GetPageId(), true);

    IxNodeHandle *next = FetchNode(leaf->GetNextLeaf());
    next->SetPrevLeaf(leaf->GetPrevLeaf());  // 注意此处是SetPrevLeaf()
    buffer_pool_manager_->UnpinPage(next->GetPageId(), true);
}

/**
 * @brief 删除node时，更新file_hdr_.num_pages
 *
 * @param node
 */
void IxIndexHandle::release_node_handle(IxNodeHandle &node) { file_hdr_.num_pages--; }

/**
 * @brief 将node的第child_idx个孩子结点的父节点置为node
 */
void IxIndexHandle::maintain_child(IxNodeHandle *node, int child_idx) {
    if (!node->IsLeafPage()) {
        //  Current node is inner node, load its child and set its parent to current node
        int child_page_no = node->ValueAt(child_idx);
        IxNodeHandle *child = FetchNode(child_page_no);
        child->SetParentPageNo(node->GetPageNo());
        buffer_pool_manager_->UnpinPage(child->GetPageId(), true);
    }
}

/**
 * @brief 这里把iid转换成了rid，即iid的slot_no作为node的rid_idx(key_idx)
 * node其实就是把slot_no作为键值对数组的下标
 * 换而言之，每个iid对应的索引槽存了一对(key,rid)，指向了(要建立索引的属性首地址,插入/删除记录的位置)
 *
 * @param iid
 * @return Rid
 * @note iid和rid存的不是一个东西，rid是上层传过来的记录位置，iid是索引内部生成的索引槽位置
 */
Rid IxIndexHandle::get_rid(const Iid &iid) const {
    IxNodeHandle *node = FetchNode(iid.page_no);
    if (iid.slot_no >= node->GetSize()) {
        throw IndexEntryNotFoundError();
    }
    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);  // unpin it!
    return *node->get_rid(iid.slot_no);
}

/** --以下函数将用于lab3执行层-- */
/**
 * @brief FindLeafPage + lower_bound
 *
 * @param key
 * @return Iid
 * @note 上层传入的key本来是int类型，通过(const char *)&key进行了转换
 * 可用*(int *)key转换回去
 */
Iid IxIndexHandle::lower_bound(const char *key) {
    // int int_key = *(int *)key;
    // printf("my_lower_bound key=%d\n", int_key);

    IxNodeHandle *node = FindLeafPage(key, Operation::FIND, nullptr);
    int key_idx = node->lower_bound(key);

    Iid iid = {.page_no = node->GetPageNo(), .slot_no = key_idx};

    // unpin leaf node
    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
    return iid;
}

/**
 * @brief FindLeafPage + upper_bound
 *
 * @param key
 * @return Iid
 */
Iid IxIndexHandle::upper_bound(const char *key) {
    // int int_key = *(int *)key;
    // printf("my_upper_bound key=%d\n", int_key);

    IxNodeHandle *node = FindLeafPage(key, Operation::FIND, nullptr);
    int key_idx = node->upper_bound(key);

    Iid iid;
    if (key_idx == node->GetSize()) {
        // 这种情况无法根据iid找到rid，即后续无法调用ih->get_rid(iid)
        iid = leaf_end();
    } else {
        iid = {.page_no = node->GetPageNo(), .slot_no = key_idx};
    }

    // unpin leaf node
    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
    return iid;
}

/**
 * @brief 指向第一个叶子的第一个结点
 * 用处在于可以作为IxScan的第一个
 *
 * @return Iid
 */
Iid IxIndexHandle::leaf_begin() const {
    Iid iid = {.page_no = file_hdr_.first_leaf, .slot_no = 0};
    return iid;
}

/**
 * @brief 指向最后一个叶子的最后一个结点的后一个
 * 用处在于可以作为IxScan的最后一个
 *
 * @return Iid
 */
Iid IxIndexHandle::leaf_end() const {
    IxNodeHandle *node = FetchNode(file_hdr_.last_leaf);
    Iid iid = {.page_no = file_hdr_.last_leaf, .slot_no = node->GetSize()};
    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);  // unpin it!
    return iid;
}
