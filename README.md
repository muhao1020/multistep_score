# 融合相关度和热度的查询
内部使用 改造的 BM25 算法，使得相关度得分呈阶梯型，然后将 热度作为二级排序，相关度一样的情况下，热度越高，排名越靠前。
```json

# 必传参数 query ， 其他非必传
# query 搜索内容
# base 调控因子,默认值 Math.E 自然数，为大于 1 的double 类型值，越大 阶梯型 梯度越平滑， 否则越陡峭， match(1.00001)  <- multistep_score(base) <- constant_score(非常大的数) 
# analyzer 分词器，默认为创建mapping时指定的分词器，这里可以指定想要使用的分词器
# zero_terms_query 表示如果query被synonym_analyzer分次之后为0个term，全都是停用词，那么召回策略是什么，参考 https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-query.html#query-dsl-match-query-zero
# 该查询要和 sort 组合使用
PUT test_001
{
  "mappings": {
    "properties" :{
      "name" :{
        "type" :"text",
        "analyzer" :"whitespace"
      },
      "read_num" :{
        "type" :"integer"
      }
    }
  },
  "settings": {
    "index.number_of_replicas": 0,
    "index.number_of_shards": 1
  }
}


PUT test_001/_doc/1
{
  "name" :"a b",
  "read_num": 12
}

PUT test_001/_doc/2
{
  "name" :"a a c",
  "read_num": 12
}

PUT test_001/_doc/3
{
  "name" :"a b d",
  "read_num": 72
}

PUT test_001/_doc/4
{
  "name" :"a b c a",
  "read_num": 34
}

# 可以改变 base 的大小，查看 score 的变化
GET test_001/_search
{
  "explain": false, 
  "query": {
    "multistep_score":{
      "name": {
        "query": "a c d b",
        "base": 1.002,
        "zero_terms_query": "none"
      }
    }
  }
}

# 将 score 粗粒度化之后，使用二级sort 排序热度度量字段

GET test_001/_search
{
  "explain": false,
  "query": {
    "multistep_score": {
      "name": {
        "query": "a c d b",
        "base": 1.002,
        "zero_terms_query": "none"
      }
    }
  },
  "sort": [
    {
      "_score": {
        "order": "desc"
      }
    },
    {
      "read_num": {
        "order": "desc"
      }
    }
  ]
}

```