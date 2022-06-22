COS Repository for Elasticsearch
================================

COS Repository plugin 是一个可以将Elasticsearch数据通过腾讯云对象存储服务COS进行备份恢复的插件。

编译安装
-------

7.x 之前的版本，使用maven编译
```
mvn clean package
#在release中找到zip压缩包
#执行插件安装
/$you_elasticsearch_dir/bin/elasticsearch-plugin install file:///$you_plugin_path/elasticsearch-cos-x.x.zip
```
7.x 之后的版本，使用gradle编译
```
gradle build
```

使用方法
-------

### 创建仓库

```
PUT _snapshot/my_cos_backup
{
    "type": "cos",
    "settings": {
        "access_key_id": "xxxxxx", // 可选
        "access_key_secret": "xxxxxxx", // 可选
        "account": "xxxxxxx", // 可选
        "bucket": "不带appId后缀的bucket名",
        "region": "ap-guangzhou",
        "compress": true,
        "chunk_size": "500mb",
        "base_path": "",
        "app_id": "xxxxxxx" 
    }
}
```
* account：可选，secret账户名，匹配keystore中的 `<account>`
* access_key_id & access_key_secret：可选，指定cos秘钥，**优先于通过account获取的secret**
* bucket: COS Bucket 名字，**不要带-{appId}后缀**。
* region：COS Bucket 地域，建议与 ES 集群同地域。
* base_path：备份目录，形式如/dir1/dir2/dir3，需要写最开头的’/‘，目录最后不需要'/'。
* app_id: 腾讯云账号 APPID，将在6.8之后的版本废弃，app_id 已包含在bucket参数中。

#### 使用 elasticsearch-keystore 存储secret (可选)
由于settings的设置内容会被`GET _snapshot`接口完全获取，导致secret数据暴露。如果需要对secret加密，可选用此方式存储秘钥    
* settings中如果指定了access_key_id & access_key_key，则**settings配置优先，keystore配置不生效**    
* `<account>`为自定义账户名，匹配`settings.account`     
* 如果是修改secret，关联了此账户的仓库需要删除后重建，变更才会生效

设置步骤：    
1. 添加账户    
`./bin/elasticsearch-keystore add qcloud.cos.client.<account>.account`   
不需要输入密码
2. elasticsearch-keystore中添加 secret_id & secret_key    
`./bin/elasticsearch-keystore add qcloud.cos.client.<account>.secret_id`    
`./bin/elasticsearch-keystore add qcloud.cos.client.<account>.secret_key`    
或者  
`./bin/elasticsearch-keystore add-file qcloud.cos.client.<account>.secret_id <secret_id_file_full_path>`    
`./bin/elasticsearch-keystore add-file qcloud.cos.client.<account>.secret_key <secret_key_file_full_path>`   
3. 刷新keystore    
`bin/elasticsearch-keystore upgrade`   
4. reload secure    
`POST /_nodes/reload_secure_settings `

### 列出仓库信息
```
GET _snapshot
```

也可以使用```GET _snapshot/my_cos_backup```获取指定的仓库信息


### 创建快照

#### 备份所有索引

```
PUT _snapshot/my_cos_backup/snapshot_1
```
这个命令会将ES集群内所有索引备份到```my_cos_backup```仓库下，并命名为```snapshot_1```。这个命令会立刻返回，并在后台异步执行直到结束。   
如果希望创建快照命令阻塞执行，可以添加```wait_for_completion```参数：
```
PUT _snapshot/my_cos_backup/snapshot_1?wait_for_completion=true
```
注意，命令执行的时间与索引大小相关。

#### 备份指定索引
可以在创建快照的时候指定要备份哪些索引：
```
PUT _snapshot/my_cos_backup/snapshot_2
{
    "indices": "index_1,index_2"
}
```

### 查询快照

查询单个快照信息
```
GET _snapshot/my_cos_backup/snapshot_1
```
这个命令会返回快照的相关信息:
```
{
    "snapshots": [
        {
            "snapshot": "snapshot_1",
            "uuid": "zUSugNiGR-OzH0CCcgcLmQ",
            "version_id": 5060499,
            "version": "5.6.4",
            "indices": [
                "sonested"
            ],
            "state": "SUCCESS",
            "start_time": "2018-05-04T11:44:15.975Z",
            "start_time_in_millis": 1525434255975,
            "end_time": "2018-05-04T11:45:29.395Z",
            "end_time_in_millis": 1525434329395,
            "duration_in_millis": 73420,
            "failures": [],
            "shards": {
                "total": 3,
                "failed": 0,
                "successful": 3
            }
        }
    ]
}
```

### 删除快照
删除指定的快照
```
DELETE _snapshot/my_cos_backup/snapshot_1
```
注意，如果还未完成的快照，删除快照命令依旧会执行，并取消快照创建进程

### 从快照恢复
```
POST _snapshot/my_cos_backup/snapshot_1/_restore
```

执行快照恢复命令会把把这个快照里的备份的所有索引都恢复到ES集群中。如果 snapshot_1 包括五个索引，这五个都会被恢复到我们集群里。   
还有附加的选项用来重命名索引。这个选项允许你通过模式匹配索引名称，然后通过恢复进程提供一个新名称。如果你想在不替换现有数据的前提下，恢复老数据来验证内容，或者做其他处理，这个选项很有用。让我们从快照里恢复单个索引并提供一个替换的名称：
```
POST /_snapshot/my_cos_backup/snapshot_1/_restore
{
    "indices": "index_1",
    "rename_pattern": "index_(.+)",
    "rename_replacement": "restored_index_1"
}
```
* 只恢复 index_1 索引，忽略快照中存在的其余索引。
* 查找所提供的模式能匹配上的正在恢复的索引。
* 然后把它们重命名成替代的模式。
### 查询快照恢复状态
通过执行_recovery命令，可以查看快照恢复的状态，监控快照恢复的进度。   
这个 API 可以为你在恢复的指定索引单独调用：
```
GET index_1/_recovery
```
这个命令会返回指定索引各分片的恢复状况：
```
{
    "sonested": {
        "shards": [
            {
                "id": 1,
                "type": "SNAPSHOT",
                "stage": "INDEX",
                "primary": true,
                "start_time_in_millis": 1525766148333,
                "total_time_in_millis": 8718,
                "source": {
                    "repository": "my_backup2",
                    "snapshot": "snapshot",
                    "version": "5.6.4",
                    "index": "sonested"
                },
                "target": {
                    "id": "TlzmxJHwSqyv4rhyQfRkow",
                    "host": "10.0.0.6",
                    "transport_address": "10.0.0.6:9300",
                    "ip": "10.0.0.6",
                    "name": "node-1"
                },
                "index": {
                    "size": {
                        "total_in_bytes": 1374967573,
                        "reused_in_bytes": 0,
                        "recovered_in_bytes": 160467084,
                        "percent": "11.7%"
                    },
                    "files": {
                        "total": 132,
                        "reused": 0,
                        "recovered": 20,
                        "percent": "15.2%"
                    },
                    "total_time_in_millis": 8716,
                    "source_throttle_time_in_millis": 0,
                    "target_throttle_time_in_millis": 0
                },
                "translog": {
                    "recovered": 0,
                    "total": 0,
                    "percent": "100.0%",
                    "total_on_start": 0,
                    "total_time_in_millis": 0
                },
                "verify_index": {
                    "check_index_time_in_millis": 0,
                    "total_time_in_millis": 0
                }
            },
            {
                "id": 0,
                "type": "SNAPSHOT",
                "stage": "INDEX",
                "primary": true,
                "start_time_in_millis": 1525766148296,
                "total_time_in_millis": 8748,
                "source": {
                    "repository": "my_backup2",
                    "snapshot": "snapshot",
                    "version": "5.6.4",
                    "index": "sonested"
                },
                "target": {
                    "id": "rOupcFi7Rn-kc2PzEoRMMQ",
                    "host": "10.0.0.15",
                    "transport_address": "10.0.0.15:9300",
                    "ip": "10.0.0.15",
                    "name": "node-3"
                },
                "index": {
                    "size": {
                        "total_in_bytes": 1362775831,
                        "reused_in_bytes": 0,
                        "recovered_in_bytes": 155162131,
                        "percent": "11.4%"
                    },
                    "files": {
                        "total": 125,
                        "reused": 0,
                        "recovered": 27,
                        "percent": "21.6%"
                    },
                    "total_time_in_millis": 8736,
                    "source_throttle_time_in_millis": 0,
                    "target_throttle_time_in_millis": 0
                },
                "translog": {
                    "recovered": 0,
                    "total": 0,
                    "percent": "100.0%",
                    "total_on_start": 0,
                    "total_time_in_millis": 0
                },
                "verify_index": {
                    "check_index_time_in_millis": 0,
                    "total_time_in_millis": 0
                }
            }
        ]
    }
}
```
* <1> type 字段告诉你恢复的本质；这个分片是在从一个快照恢复。
* <2> source 哈希描述了作为恢复来源的特定快照和仓库。
* <3> percent 字段让你对恢复的状态有个概念。这个特定分片目前已经恢复了 94% 的文件；它就快完成了。   

输出会列出所有目前正在经历恢复的索引，然后列出这些索引里的所有分片。每个分片里会有启动/停止时间、持续时间、恢复百分比、传输字节数等统计值。

### 取消快照恢复
```
DELETE /restored_index_3
```
如果 restored\_index\_3 正在恢复中，这个删除命令会停止恢复，同时删除所有已经恢复到集群里的数据。

常见问题
-------
1. 提示找不到bucket？
6.x版本中，COS的bucket名中已经包括了appid，如果使用形如buceket1-11212121的bucket名，请不要再传递appid参数。如使用早期不包含appid的bucket名，请传递appid参数。
2. 创建快照时报找不到index？
请确认indices参数中传递的索引列表中索引名是否正确，且不要包含空格。

反馈
----

如在使用中有相关的问题，欢迎提交issue
