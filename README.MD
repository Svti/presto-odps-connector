# presto-odps-connector

根据prestodb.io的情况，进行版本更新

```
FROM https://github.com/aliyun/aliyun-maxcompute-data-collectors
```


- odps.properties 内容如下：
```text
connector.name=odps 
odps.project.name=XXXXXX # maxcompute 项目名字
odps.access.id=XXXXXX # 阿里云鉴权参数
odps.access.key=XXXXXX # 阿里云鉴权参数
odps.end.point=XXXXXX # maxcompute 服务endpoint
odps.tunnel.end.point=xxx # maxcompute tunnel endpoint
odps.case-insensitive-name-matching=true # 是否表名忽略大小写,默认:否
odps.case-insensitive-name-matching.cache-ttl=1000 # 大小写表名缓存时间,单位:ms，默认:0ms
odps.input.split.size=64 # 切分表的split大小

```