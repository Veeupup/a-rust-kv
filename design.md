# log 设计

第一个需要设计的就是每一条日志实际所存储的形式，因为 `serde` 只是一个 序列化和反序列化库，并不能够从 `log` 中区分出每一条日志，所以需要设计 `log` 的格式来区分，这里参考 `bitcask` 的设计，将每条 `log` 设计成下面的形式：

```
| kv_size(4B) | serialized kv |
```

当 `version` 为 0 的时候代表被删除，正常的 `version` 都是大于 0 的正整数，读取每一条记录的时候，先读取 8B 的内容，判断要读取多少内容到 buffer 中来，然后再反序列化

现在要关注的就是是否把前面的 `meta` 信息也放到 `struct` 中去，如果放到其中去，那么就不能够正常的区分不同的 `log`，也就是先有鸡还是先有蛋的问题，所以只能每次都序列化的外面，
但是 `version` 是可以放到 序列化的内容中去的，这是因为放到这个里面的话，也和序列化本身无关了

# get set rm passed 1.0

1.0 跑通测试，非常拙的一个版本实现，我的想法是先跑起来，然后再进行优化，其实也主要是对 Rust 也不太熟悉，所以先跑通测试，然后再优化，现在的实现是这样的：

1. 日志存储格式为上述的 log 格式
2. set：
    每次写入的时候，直接 `append` 到文件的最后，先写入序列化之后的长度 4B，然后写入序列化之后的 kv, 每次都只是写入新的，version 为 1，大于 0 的一个整数
3. rm：
    和 set 一样，只不过将 `version` 设置为 0
4. get：
    每次都将所有的 kv 读入到内存的 map 中，如果遇到 `version` 为 0 的，那么就认为是删除的，将这个 kv 从 map 中删除掉

目前的实现非常拙，来进行第四步的优化，内存 map 中只保存所有的 key 以及 文件中的 `offset`，每次更新的时候，先写入到 log 中，成功之后再更新内存中的 map 数据结构

注意当前只有一个 `file handler` 来控制所有的读写操作，现在其实可以将读写区分开，写的永远只写到文件的末尾，读的可以随时更改 `offset`
