#include "ix_node_handle.h"

/**
 * @brief 在当前node中查找第一个>=target的key_idx
 *
 * @return key_idx，范围为[0,num_key)，如果返回的key_idx=num_key，则表示target大于最后一个key
 * @note 返回key index（同时也是rid index），作为slot no
 */
int IxNodeHandle::lower_bound(const char *target) const {
    // Todo:
    // 查找当前节点中第一个大于等于target的key，并返回key的位置给上层
    // 提示: 可以采用多种查找方式，如顺序遍历、二分查找等；使用ix_compare()函数进行比较
    int num_key_now = page_hdr->num_key;
    ColType type = file_hdr->col_type;
    int col_len = file_hdr->col_len;
    int idx = -1;
    if(binary_search){
        //待写的二分查找
        for(int i = 0; i < num_key_now; ++i){
            // printf("当前的i为%d,get_key(i)为%d,target为%d\n",i,*get_key(i),*target);
            if(ix_compare(get_key(i),target,type,col_len) >= 0){
                idx = i; //表示找到了
                break;
            }
        }
        if(idx == -1)
            idx = num_key_now;
    }else{//顺序查找
        // printf("当前的num_key为%d\n",num_key_now); 
        for(int i = 0; i < num_key_now; ++i){
            // printf("当前的i为%d,get_key(i)为%d,target为%d\n",i,*get_key(i),*target);
            if(ix_compare(get_key(i),target,type,col_len) >= 0){
                idx = i; //表示找到了
                break;
            }
        }
        if(idx == -1)
            idx = num_key_now;
    }

    return idx;
}

/**
 * @brief 在当前node中查找第一个>target的key_idx
 *
 * @return key_idx，范围为[1,num_key)，如果返回的key_idx=num_key，则表示target大于等于最后一个key
 * @note 注意此处的范围从1开始
 */
int IxNodeHandle::upper_bound(const char *target) const {
    // Todo:
    // 查找当前节点中第一个大于target的key，并返回key的位置给上层
    // 提示: 可以采用多种查找方式：顺序遍历、二分查找等；使用ix_compare()函数进行比较
    int num_key_now = page_hdr->num_key;
    ColType type = file_hdr->col_type;
    int col_len = file_hdr->col_len;
    int idx = -1;
    if(binary_search){
        //待写的二分查找
        for(int i = 1; i < num_key_now; ++i){
            if(ix_compare(get_key(i),target,type,col_len) > 0){
                idx = i; //表示找到了
                break;
            }
        }
        if(idx == -1)
            idx = num_key_now;
    }else{//顺序查找
        for(int i = 1; i < num_key_now; ++i){
            if(ix_compare(get_key(i),target,type,col_len) > 0){
                idx = i; //表示找到了
                break;
            }
        }
        if(idx == -1)
            idx = num_key_now;
    }

    return idx;
}

/**
 * @brief 用于叶子结点根据key来查找该结点中的键值对
 * 值value作为传出参数，函数返回是否查找成功
 *
 * @param key 目标key
 * @param[out] value 传出参数，目标key对应的Rid
 * @return 目标key是否存在
 */
bool IxNodeHandle::LeafLookup(const char *key, Rid **value) {
    // Todo:
    // 1. 在叶子节点中获取目标key所在位置
    // 2. 判断目标key是否存在
    // 3. 如果存在，获取key对应的Rid，并赋值给传出参数value
    // 提示：可以调用lower_bound()和get_rid()函数。
    int num_key_now = page_hdr->num_key;
    ColType type = file_hdr->col_type;
    int col_len = file_hdr->col_len;
    //因为貌似不含有重复值，所以注意利用
    int idx = lower_bound(key);
    if(ix_compare(get_key(idx),key,type,col_len) == 0){//表示找到了，否则没有找到
        *value = get_rid(idx);
        return true;
    }
    return false;
}

/**
 * 用于内部结点（非叶子节点）查找目标key所在的孩子结点（子树）
 * @param key 目标key
 * @return page_id_t 目标key所在的孩子节点（子树）的存储页面编号
 */
page_id_t IxNodeHandle::InternalLookup(const char *key) {
    // Todo:
    // 1. 查找当前非叶子节点中目标key所在孩子节点（子树）的位置
    // 2. 获取该孩子节点（子树）所在页面的编号
    // 3. 返回页面编号
    int num_key_now = page_hdr->num_key;
    ColType type = file_hdr->col_type;
    int col_len = file_hdr->col_len;
    //因为貌似不含有重复值，所以注意利用
    int idx = lower_bound(key);
    if(ix_compare(get_key(idx),key,type,col_len) == 0){//表示找到了，否则没有找到
        return ValueAt(idx);
    }else if(idx == 0){//表示新来的这个节点小于目前最小的节点
        return ValueAt(idx);
    }else{//如果索引中没有正好等于的，那么就找它最后一个小于它的
        return ValueAt(idx-1);//不会存在最小值不在的情况，因为我们会不断维持最小的值
    }
}

/**
 * @brief 在指定位置插入n个连续的键值对
 * 将key的前n位插入到原来keys中的pos位置；将rid的前n位插入到原来rids中的pos位置
 *
 * @param pos 要插入键值对的位置
 * @param (key, rid) 连续键值对的起始地址，也就是第一个键值对，可以通过(key, rid)来获取n个键值对
 * @param n 键值对数量
 * @note [0,pos)           [pos,num_key)
 *                            key_slot
 *                            /      \
 *                           /        \
 *       [0,pos)     [pos,pos+n)   [pos+n,num_key+n)
 *                      key           key_slot
 */
void IxNodeHandle::insert_pairs(int pos, const char *key, const Rid *rid, int n) {
    // Todo:
    // 1. 判断pos的合法性
    // 2. 通过key获取n个连续键值对的key值，并把n个key值插入到pos位置
    // 3. 通过rid获取n个连续键值对的rid值，并把n个rid值插入到pos位置
    // 4. 更新当前节点的键数量
    //在这里我实现的时候默认了连续的数组都是有序的，并且保证了插入的位置也是对的位置，并且进行了去重
    int num_key_now = page_hdr->num_key;
    ColType type = file_hdr->col_type;
    int col_len = file_hdr->col_len;
    //去重，先去重再判断，注意，只要key重复那么rid就一定重复，因为key一定是不重复的，换句话说，key为码
    char *tmp_key = new char[col_len*n];
    Rid *tmp_rid = new Rid[n];
    int real_n = 0;//真正要插入的n个数
    // printf("要插入的n是%d\n",n);
    //无论如何第一个值一定要进入待插区
    memcpy(tmp_key,key,col_len);
    memcpy(tmp_rid,rid,sizeof(Rid));
    real_n++;
    for(int i = 1; i < n; ++i){
        const char * key_i = key + i;
        const char * key_i2 = key + i -1;
        // printf("key_i,key_i2 : %d, %d\n",*key_i,*key_i2);
        if(ix_compare(key_i,key_i2,type,col_len) == 0){
            //如果二者相等，那么我就不插
        }else{//如果不等，那么我
            // printf("在insert_pairs力，应该进入一次\n");
            memcpy(tmp_key + col_len * real_n, key + col_len * i, col_len);
            memcpy(tmp_rid + real_n, rid + i, sizeof(Rid));
            real_n ++;
        }
    }
        //此时的real_n是数量而不是坐标了
    // printf("real_n为%d\n",real_n);
    if( (pos + real_n)*col_len > file_hdr->keys_size){//判断如果不合法，那么就直接不插入，或许还有其他的
        // printf("不应该到达这里啊啊啊啊----------------------\n");
    }else{//如果合法的话，那么我就插入,注意，此处仅仅考虑了插入一个值的情况，所以不排序，默认位置找的是对的，也不再这里对pos之前和之后的值去重了，在Insert那里去重
        memmove(keys+(pos + real_n)*col_len, keys+pos*col_len, col_len*(num_key_now-pos));
        memmove(rids+(pos + real_n),rids+pos, sizeof(Rid)*(num_key_now-pos));
        memcpy(keys+pos*col_len, tmp_key, col_len*real_n);
        memcpy(rids+pos, tmp_rid, sizeof(Rid)*real_n);
        // for(int i = 0; i <= num_key_now; ++i){
        //     printf("插完后的第%d个值是%d\n",i,*get_key(i));
        // }
    }
    
    return ;

}

/**
 * @brief 用于在结点中的指定位置插入单个键值对
 */
void IxNodeHandle::insert_pair(int pos, const char *key, const Rid &rid) { insert_pairs(pos, key, &rid, 1); };

/**
 * @brief 用于在结点中插入单个键值对。
 * 函数返回插入后的键值对数量
 *
 * @param (key, value) 要插入的键值对
 * @return int 键值对数量
 */
int IxNodeHandle::Insert(const char *key, const Rid &value) {
    // Todo:
    // 1. 查找要插入的键值对应该插入到当前节点的哪个位置
    // 2. 如果key重复则不插入
    // 3. 如果key不重复则插入键值对
    // 4. 返回完成插入操作之后的键值对数量
    int num_key_now = page_hdr->num_key;
    ColType type = file_hdr->col_type;
    int col_len = file_hdr->col_len;
    int pos = lower_bound(key);
    // printf("在Insert函数中，将要插入的key值为%d,应当插入的pos为%d\n",*key,pos);
    if(pos == num_key_now){  //插入最后即可
        insert_pair(pos,key,value);
        page_hdr->num_key ++;
        
        return page_hdr->num_key;
    }else if(ix_compare(get_key(pos),key,type,col_len) == 0){//如果找到了一个大于等于它的数，先看看是否是等于他，如果等于，那么就不插了
        //相等，不插入了
        // printf("%d,  %d\n",*get_key(pos),*key);
        // printf("键值对相等----------------------不该到这里\n");
        return num_key_now;
    }else{//否则，既不是找不到，又不是有等于它的，那么pos位置一定比key要大，pos-1比key要小，那么就把他插在pos位置
        // printf("在中间插入应当要到达这里的\n");
        insert_pair(pos,key,value);
        page_hdr->num_key ++;
        
        return page_hdr->num_key;
    }
}

/**
 * @brief 用于在结点中的指定位置删除单个键值对
 *
 * @param pos 要删除键值对的位置
 */
void IxNodeHandle::erase_pair(int pos) {
    // Todo:
    // 1. 删除该位置的key
    // 2. 删除该位置的rid
    // 3. 更新结点的键值对数量
    int num_key_now = page_hdr->num_key;
    ColType type = file_hdr->col_type;
    int col_len = file_hdr->col_len;
    // printf("要删除的pos是%d,移动的数量是%d,num_key_now是%d\n",pos,(num_key_now - pos - 1),num_key_now);
    if(pos < 0 || pos >=GetSize()){
        return ;
    }else{
        memmove( keys + pos * col_len, keys + (pos + 1) * col_len, col_len * (num_key_now - pos - 1));
        memmove( rids + pos, rids + (pos + 1), sizeof(Rid) * (num_key_now - pos - 1));
        page_hdr->num_key --;
    }
    // printf("删除完后的num_key是%d,所有的key分别是:\n",page_hdr->num_key);
    // for(int i =0; i < page_hdr->num_key; ++i){
    //     printf("%d\n",*get_key(i));
    // }
}

/**
 * @brief 用于在结点中删除指定key的键值对。函数返回删除后的键值对数量
 *
 * @param key 要删除的键值对key值
 * @return 完成删除操作后的键值对数量
 */
int IxNodeHandle::Remove(const char *key) {
    // Todo:
    // 1. 查找要删除键值对的位置
    // 2. 如果要删除的键值对存在，删除键值对
    // 3. 返回完成删除操作后的键值对数量

    int num_key_now = page_hdr->num_key;
    ColType type = file_hdr->col_type;
    int col_len = file_hdr->col_len;
    int pos = lower_bound(key);
    if(ix_compare(get_key(pos),key,type,col_len) == 0){
        erase_pair(pos);
        return GetSize();
    }
    return num_key_now;
}

/**
 * @brief 由parent调用，寻找child，返回child在parent中的rid_idx∈[0,page_hdr->num_key)
 *
 * @param child
 * @return int
 */
int IxNodeHandle::find_child(IxNodeHandle *child) {
    int rid_idx;
    for (rid_idx = 0; rid_idx < page_hdr->num_key; rid_idx++) {
        if (get_rid(rid_idx)->page_no == child->GetPageNo()) {
            break;
        }
    }
    assert(rid_idx < page_hdr->num_key);
    return rid_idx;
}

/**
 * @brief used in internal node to remove the last key in root node, and return the last child
 *
 * @return the last child
 */
page_id_t IxNodeHandle::RemoveAndReturnOnlyChild() {
    assert(GetSize() == 1);
    page_id_t child_page_no = ValueAt(0);
    erase_pair(0);
    assert(GetSize() == 0);
    return child_page_no;
}