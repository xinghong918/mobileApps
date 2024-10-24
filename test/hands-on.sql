-- 1. 向量搜索
SELECT VECTOR_EMBEDDING(bge_base_zh USING 'hello, embedding here' as input) AS embedding;

select * from lab23ai_files;

select dbms_vector_chain.utl_to_text(file_blob) file_content from lab23ai_files where id = 4;

SELECT d.*, C.*,VECTOR_EMBEDDING(bge_base_zh USING c.chunk_text as input) vec
FROM lab23ai_files D, 
VECTOR_CHUNKS(dbms_vector_chain.utl_to_text(d.file_blob) BY chars MAX 500 OVERLAP 0 SPLIT BY recursively LANGUAGE "simplified chinese" NORMALIZE all) C 
where d.id = 4;

select fe.id, fe.file_id, VECTOR_DISTANCE(fe.vector_data, VECTOR_EMBEDDING(bge_base_zh USING '该公司的法人是谁' as input), COSINE) as vect_distance, fe.chunk_text 
 from lab23ai_company c, lab23ai_files f, lab23ai_files_embedding fe
 where  f.id = fe.file_id and f.company_id = c.id and c.company_name = 'ABC公司' and f.active_yn = 'Y' 
 order by vect_distance
FETCH FIRST 3 ROWS ONLY;

select fe.id, fe.file_id, VECTOR_DISTANCE(fe.vector_data, VECTOR_EMBEDDING(bge_base_zh USING '公司是何时成立的' as input), COSINE) as vect_distance, fe.chunk_text 
 from lab23ai_company c, lab23ai_files f, lab23ai_files_embedding fe
 where  f.id = fe.file_id and f.company_id = c.id 
 and c.company_name = 'ABC公司' and f.active_yn = 'Y' 
 order by vect_distance
FETCH FIRST 3 ROWS ONLY;


select fe.id, fe.file_id, VECTOR_DISTANCE(fe.vector_data, VECTOR_EMBEDDING(bge_base_zh USING '成立日期' as input), COSINE) as vect_distance, fe.chunk_text 
 from lab23ai_company c, lab23ai_files f, lab23ai_files_embedding fe
 where  f.id = fe.file_id and f.company_id = c.id and c.company_name = 'ABC公司' and f.active_yn = 'Y' 
 order by vect_distance
FETCH FIRST 5 ROWS ONLY;

-- 2. Oracle Text 搜索
grant execute on CTXSYS.CTX_DDL to USER0;

exec CTX_DDL.CREATE_PREFERENCE('my_zh_lexer','CHINESE_VGRAM_LEXER');
CREATE INDEX lab23ai_files_zh_content_idx ON lab23ai_files_embedding(CHUNK_TEXT) INDEXTYPE IS ctxsys.CONTEXT PARAMETERS('LEXER my_zh_lexer');

select index_name, index_type, status from user_indexes
where index_name = 'LAB23AI_FILES_ZH_CONTENT_IDX';

select idx_name, idx_table, idx_text_name from ctx_user_indexes;

select * from DR$LAB23AI_FILES_ZH_CONTENT_IDX$I;

SELECT score(10) as score , fe.*
FROM lab23ai_files_embedding fe, lab23ai_files f, lab23ai_company c
WHERE fe.file_id = f.id and f.COMPANY_ID=c.id and c.company_name = 'ABC公司' and f.active_yn = 'Y' 
and contains(fe.CHUNK_TEXT, regexp_replace('成立日期','\s+', ' or '), 10) > 0
--and score(10) > 10
ORDER BY score DESC FETCH FIRST 5 ROWS ONLY;

-- 3. 混合搜索
select * from 
(
    select id, score, 'text' search_type from (SELECT fe.id, score(10) as score
    FROM lab23ai_files_embedding fe, lab23ai_files f, lab23ai_company c
    WHERE fe.file_id = f.id and f.COMPANY_ID=c.id and c.company_name = :company_name and f.active_yn = 'Y' 
    and contains(fe.CHUNK_TEXT, regexp_replace(:keywords,'\s+', ' or '), 10) > 0
    --and score(10) > 10
    ORDER BY score DESC FETCH FIRST 5 ROWS ONLY)
    union
    select id, vect_distance, 'vector' from (
     select fe.id, VECTOR_DISTANCE(fe.vector_data, VECTOR_EMBEDDING(bge_base_zh USING :keywords as input), COSINE) as vect_distance
     from lab23ai_files f, lab23ai_files_embedding fe, lab23ai_company c 
     where  f.id = fe.file_id and c.id = f.company_id and c.company_name = :company_name and f.active_yn = 'Y' 
     order by vect_distance
    FETCH FIRST 3 ROWS ONLY)
) s
left join lab23ai_files_embedding e on e.id = s.id;

-- 4. Graph
CREATE or replace PROPERTY GRAPH lab23ai_transfer_graph
VERTEX TABLES (
    lab23ai_company key (ID)
     LABEL company PROPERTIES (ID, company_name as name, company_type as "公司类型", lsrxydj as "纳税信用等级",is_sxqy as "是否失信企业", biz_status as "经营状态", reg_capital as "注册资本")
)
EDGE TABLES (
    lab23ai_BANK_TRANSFERS KEY (TXN_ID) 
    SOURCE KEY (src_acct_id) REFERENCES lab23ai_company(ID)
    DESTINATION KEY (dst_acct_id) REFERENCES lab23ai_company(ID)
    PROPERTIES (src_acct_id, dst_acct_id, amount)
);

SELECT * FROM user_property_graphs;

SELECT dbms_metadata.get_ddl('PROPERTY_GRAPH', 'LAB23AI_TRANSFER_GRAPH') from dual;

SELECT * FROM user_pg_elements WHERE graph_name='LAB23AI_TRANSFER_GRAPH';

SELECT * FROM user_pg_label_properties WHERE graph_name='LAB23AI_TRANSFER_GRAPH';

-- 图数据查询
-- 找到转入交易最多的前10个(公司)账户
SELECT account_name, COUNT(1) AS Num_Transfers 
FROM graph_table ( lab23ai_transfer_graph 
    MATCH (src) - [IS lab23ai_BANK_TRANSFERS] -> (dst) 
    COLUMNS ( dst.name AS account_name )
) GROUP BY account_name ORDER BY Num_Transfers DESC FETCH FIRST 10 ROWS ONLY;

-- 们在2次跳转的交易链中找到中间交易次数最的前10名的账户
SELECT account_name, COUNT(1) AS Num_In_Middle 
FROM graph_table ( lab23ai_transfer_graph 
    MATCH (src) - [IS lab23ai_BANK_TRANSFERS] -> (via) - [IS lab23ai_BANK_TRANSFERS] -> (dst) 
    COLUMNS ( via.name AS account_name )
) GROUP BY account_name ORDER BY Num_In_Middle DESC FETCH FIRST 10 ROWS ONLY;

-- 我们列出从账号'ABC公司'转出并且中间经过1、2、3次中转交易的记录：
SELECT account_name1, account_name2 
FROM graph_table(lab23ai_transfer_graph
    MATCH (v1)-[IS lab23ai_BANK_TRANSFERS]->{1,3}(v2) 
    WHERE v1.name = 'ABC公司' 
    COLUMNS (v1.name AS account_name1, v2.name AS account_name2)
);

-- 经过3次中间交易任何循环支付链
SELECT acct_name, COUNT(1) AS Num_Triangles 
FROM graph_table (lab23ai_transfer_graph 
    MATCH (src) - []->{3} (src) 
    COLUMNS (src.name AS acct_name) 
) GROUP BY acct_name ORDER BY Num_Triangles DESC;

-- 经过4次中间交易任何循环支付链
SELECT acct_name, COUNT(1) AS Num_4hop_Chains 
FROM graph_table (lab23ai_transfer_graph 
    MATCH (src) - []->{4} (src) 
    COLUMNS (src.name AS acct_name) 
) GROUP BY acct_name ORDER BY Num_4hop_Chains DESC;

-- 查询中转次数为5次的循环支付
SELECT acct_name, COUNT(1) AS Num_5hop_Chains 
FROM graph_table (lab23ai_transfer_graph 
    MATCH (src) - []->{5} (src) 
    COLUMNS (src.name AS acct_name) 
) GROUP BY acct_name ORDER BY Num_5hop_Chains DESC;

-- 经过了3、4、5次的循环交易，让我们列出排名前10名的存在这样类似交易的账号，排序按存在这样循环交易次数越多的越排前
SELECT DISTINCT(account_name), COUNT(1) AS Num_Cycles 
FROM graph_table(lab23ai_transfer_graph
    MATCH (v1)-[IS lab23ai_BANK_TRANSFERS]->{3, 5}(v1) 
    COLUMNS (v1.name AS account_name) 
) GROUP BY account_name ORDER BY Num_Cycles DESC FETCH FIRST 10 ROWS ONLY;

-- Demo
select f.*, to_char((f.金额/f.交易总额)*100, 'FM999990.00')||'%' 金额占比 from (WITH acc1 AS (select t.*, SUM(t.金额) OVER () 交易总额 from (
SELECT src_name AS 源公司名称, source_account 源账号 , dst_name 目标公司名称, target_account 目标账号 , COUNT(1) 交易次数 , sum(amount) 金额
FROM graph_table ( lab23ai_transfer_graph
    MATCH (src) - [t1 IS lab23ai_BANK_TRANSFERS] -> (dst) 
    COLUMNS ( src.name as src_name, src.id as source_account, dst.name AS dst_name, dst.id as target_account, t1.amount as amount )
) where src_name = 'ABC公司' 
GROUP BY src_name,source_account, dst_name, target_account 
ORDER BY 交易次数 DESC 
FETCH FIRST 5 ROWS ONLY) t)
SELECT * FROM acc1
) f


-- 5. 普适搜索索引
BEGIN
    DBMS_SEARCH.CREATE_INDEX('LAB23AI_MYIDX');
END;

SELECT * FROM LAB23AI_MYIDX;

BEGIN
    DBMS_SEARCH.ADD_SOURCE(index_name =>'LAB23AI_MYIDX', source_name => 'LAB23AI_NEWS');
END;

SELECT JSON_SERIALIZE(metadata), metadata FROM LAB23AI_MYIDX idx where idx.metadata.SOURCE = 'LAB23AI_NEWS';

SELECT json_serialize(DBMS_SEARCH.GET_DOCUMENT('LAB23AI_MYIDX',metadata) PRETTY) FROM LAB23AI_MYIDX idx where idx.metadata.SOURCE = 'LAB23AI_NEWS' AND  idx.metadata.KEY.ID = 2;

-- 简单的搜索
SELECT json_serialize(metadata) FROM LAB23AI_MYIDX
WHERE CONTAINS (data, '%合同%') > 0;

-- select json_serialize(DBMS_SEARCH.GET_DOCUMENT('LAB23AI_MYIDX',metadata) ) FROM LAB23AI_MYIDX idx;

-- 仅搜索"ABC公司"相关的新闻信息： 由于JSON结构的不同，可以针对不同的结构来做条件：
select * FROM LAB23AI_MYIDX idx where JSON_TEXTCONTAINS(idx.data, '$.*.LAB23AI_NEWS.JSON_CONTENT.company', 'ABC公司' ) or JSON_TEXTCONTAINS(idx.data, '$.*.LAB23AI_NEWS.JSON_CONTENT.*.company', 'ABC公司' );

-- 搜索"ABC公司"是否有"负面新闻或是不实信息"等tag的新闻信息：
select n.* FROM LAB23AI_MYIDX idx, LAB23AI_NEWS n 
where idx.metadata.KEY.ID = n.id 
and (JSON_TEXTCONTAINS(idx.data, '$.*.LAB23AI_NEWS.JSON_CONTENT.*', 'company=ABC公司' )  
    and (json_textcontains(idx.data, '$.*.LAB23AI_NEWS.JSON_CONTENT.tags',  ' %负面新闻% or %不实信息%' )
    or json_textcontains(idx.data, '$.*.LAB23AI_NEWS.JSON_CONTENT.*.tags',  ' %负面新闻% or %不实信息%' )
    ) );


-- 使用DBMS_SEARCH.FIND
with temp as (
    select DBMS_SEARCH.FIND('LAB23AI_MYIDX',JSON('
      {
        "$query": { "$and" : [
                              {"*.LAB23AI_NEWS.JSON_CONTENT.tags" : { "$contains" : "%不实信息% or %负面新闻%" }},
                              {"*.LAB23AI_NEWS.JSON_CONTENT.company" : { "$contains" : "ABC公司" }}
                             ] 
                    },
    "$search" : { "start" : 1,  "end" : 4 }
    }'))  as m from dual
)
select t.m."$count", t.* from temp t

-- 【可选】针对JSON字段
CREATE SEARCH INDEX LAB23AI_NEWS_json_idx ON LAB23AI_NEWS (json_content) FOR JSON;

select * FROM LAB23AI_NEWS where json_textcontains(JSON_CONTENT, '$.*' , 'company=ABC公司') and json_textcontains(JSON_CONTENT, '$.tags' , '%负面新闻% or %不实信息%' );

-- 6. 融合SQL
-- 第一步：查询前3的循环交易账号有哪些【属性图数据】
with top_3_cycles as (
    SELECT DISTINCT(account_name), COUNT(1) AS Num_Cycles 
    FROM graph_table(lab23ai_transfer_graph
        MATCH (v1)-[IS lab23ai_BANK_TRANSFERS]->{3, 5}(v1) 
        COLUMNS (v1.name AS account_name) 
    ) GROUP BY account_name ORDER BY Num_Cycles DESC FETCH FIRST 3 ROWS ONLY
)
select * from top_3_cycles g, LAB23AI_company c0 where c0.company_name = g.account_name;

-- 第二步：且注册地址为北京的，结果只有2个【关系型】
with top_3_cycles as (
    SELECT DISTINCT(account_name), COUNT(1) AS Num_Cycles 
    FROM graph_table(lab23ai_transfer_graph
        MATCH (v1)-[IS lab23ai_BANK_TRANSFERS]->{3, 5}(v1) 
        COLUMNS (v1.name AS account_name) 
    ) GROUP BY account_name ORDER BY Num_Cycles DESC FETCH FIRST 3 ROWS ONLY
)
select * from top_3_cycles g, LAB23AI_company c0 where c0.company_name = g.account_name and instr(c0.reg_addr, '北京') > 0;


-- 第三步：对其纳税情况进行查询【向量数据】
with top_3_cycles as (
    SELECT DISTINCT(account_name), COUNT(1) AS Num_Cycles 
    FROM graph_table(lab23ai_transfer_graph
        MATCH (v1)-[IS lab23ai_BANK_TRANSFERS]->{3, 5}(v1) 
        COLUMNS (v1.name AS account_name) 
    ) GROUP BY account_name ORDER BY Num_Cycles DESC FETCH FIRST 3 ROWS ONLY
)
select c0.company_name, 
    (select fe.chunk_text 
     from lab23ai_company c, lab23ai_files f, lab23ai_files_embedding fe
     where  f.id = fe.file_id and f.company_id = c.id and c.company_name = c0.company_name and f.active_yn = 'Y' 
     order by VECTOR_DISTANCE(fe.vector_data, VECTOR_EMBEDDING(bge_base_zh USING '纳税情况' as input), COSINE)
    FETCH FIRST 1 ROWS ONLY) as vector_search 
from top_3_cycles g, LAB23AI_company c0 where c0.company_name = g.account_name and instr(c0.reg_addr, '北京') > 0;

-- 第4步：且找出其中存在有负面新闻的公司【JSON查询/普适搜索】
with top_3_cycles as (
    SELECT DISTINCT(account_name), COUNT(1) AS Num_Cycles 
    FROM graph_table(lab23ai_transfer_graph
        MATCH (v1)-[IS lab23ai_BANK_TRANSFERS]->{3, 5}(v1) 
        COLUMNS (v1.name AS account_name) 
    ) GROUP BY account_name ORDER BY Num_Cycles DESC FETCH FIRST 3 ROWS ONLY
)
select c0.company_name, 
    (select fe.chunk_text 
     from lab23ai_company c, lab23ai_files f, lab23ai_files_embedding fe
     where  f.id = fe.file_id and f.company_id = c.id and c.company_name = c0.company_name and f.active_yn = 'Y' 
     order by VECTOR_DISTANCE(fe.vector_data, VECTOR_EMBEDDING(bge_base_zh USING '纳税情况' as input), COSINE)
    FETCH FIRST 1 ROWS ONLY) as vector_search,
    (select t.m."$count" from (select DBMS_SEARCH.FIND('LAB23AI_MYIDX',JSON('
      {
        "$query": { "$and" : [
                              {"*.LAB23AI_NEWS.JSON_CONTENT.tags" : { "$contains" : "%不实信息% or %负面新闻%" }},
                              {"*.LAB23AI_NEWS.JSON_CONTENT.company" : { "$contains" : "'||c0.company_name||'" }}
                             ] 
                    },
    "$search" : { "start" : 1,  "end" : 4 }
    }'))  as m) t ) news
from top_3_cycles g, LAB23AI_company c0 where c0.company_name = g.account_name and instr(c0.reg_addr, '北京') > 0;

-- 完成：对news字段进行过滤，只选大于0的,即是最终的融合SQL了：
with top_3_cycles as (
    SELECT DISTINCT(account_name), COUNT(1) AS Num_Cycles 
    FROM graph_table(lab23ai_transfer_graph
        MATCH (v1)-[IS lab23ai_BANK_TRANSFERS]->{3, 5}(v1) 
        COLUMNS (v1.name AS account_name) 
    ) GROUP BY account_name ORDER BY Num_Cycles DESC FETCH FIRST 3 ROWS ONLY
)
select * from (
select c0.company_name, 
    (select fe.chunk_text 
     from lab23ai_company c, lab23ai_files f, lab23ai_files_embedding fe
     where  f.id = fe.file_id and f.company_id = c.id and c.company_name = c0.company_name and f.active_yn = 'Y' 
     order by VECTOR_DISTANCE(fe.vector_data, VECTOR_EMBEDDING(bge_base_zh USING '纳税情况' as input), COSINE)
    FETCH FIRST 1 ROWS ONLY) as vector_search,
    (select t.m."$count" from (select DBMS_SEARCH.FIND('LAB23AI_MYIDX',JSON('
      {
        "$query": { "$and" : [
                              {"*.LAB23AI_NEWS.JSON_CONTENT.tags" : { "$contains" : "%不实信息% or %负面新闻%" }},
                              {"*.LAB23AI_NEWS.JSON_CONTENT.company" : { "$contains" : "'||c0.company_name||'" }}
                             ] 
                    },
    "$search" : { "start" : 1,  "end" : 4 }
    }'))  as m) t ) news
from top_3_cycles g, LAB23AI_company c0 where c0.company_name = g.account_name and instr(c0.reg_addr, '北京') > 0
) t0 where to_number(t0.news) > 0;



-- ************
SELECT src_name, via_name, dst_name, COUNT(1) AS Num_In_Middle 
FROM graph_table ( lab23ai_transfer_graph 
    MATCH (src) - [] -> (via) - [IS lab23ai_BANK_TRANSFERS] -> {1,2}(dst) 
    WHERE src.name = 'ABC公司' and dst.name='DEF6公司'
    COLUMNS ( src.name AS src_name,via.name AS via_name, dst.name AS dst_name )
) GROUP BY src_name, via_name, dst_name ORDER BY Num_In_Middle DESC FETCH FIRST 10 ROWS ONLY;

SELECT src_name, via_name, dst_name, COUNT(1) AS Num_In_Middle 
FROM graph_table ( lab23ai_transfer_graph 
    MATCH (src) - [] -> (via) - [IS lab23ai_BANK_TRANSFERS] -> {1}(dst) 
    WHERE src.name = 'ABC公司' and dst.name='DEF6公司'
    COLUMNS ( src.name AS src_name,via.name AS via_name, dst.name AS dst_name )
) GROUP BY src_name, via_name, dst_name ORDER BY Num_In_Middle DESC FETCH FIRST 10 ROWS ONLY;

SELECT src_name, via_name, dst_name, COUNT(1) AS Num_In_Middle 
FROM graph_table ( lab23ai_transfer_graph 
    MATCH (src) - [] -> (via) - [IS lab23ai_BANK_TRANSFERS] -> {2}(dst) 
    WHERE src.name = 'ABC公司' and dst.name='DEF6公司'
    COLUMNS ( src.name AS src_name,via.name AS via_name, dst.name AS dst_name )
) GROUP BY src_name, via_name, dst_name ORDER BY Num_In_Middle DESC FETCH FIRST 10 ROWS ONLY;