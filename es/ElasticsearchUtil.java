package com.common.utils.es;

import com.alibaba.fastjson.JSONObject;
import com.common.message.dto.PageDto;
import com.epro.utils.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.formula.functions.T;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequest;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.snapshots.SnapshotShardsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Author admin
 * @Date 2020/7/15
 * @Description
 */
@Component
public class ElasticsearchUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchUtil.class);
    public static final int STATUS = 200;

    @Autowired
    private TransportClient transportClient;

    private static TransportClient client;


    private static String PRETAG = "<font>";
    private static String PRETAG_RED = "<font color='red'>";
    private static String POSTAG = "</font>";

    /**
     * @PostContruct是spring框架的注解 spring容器初始化的时候执行该方法
     */
    @PostConstruct
    public void init() {
        client = this.transportClient;
    }

    /**
     * 创建索引
     *
     * @param index
     * @return
     */
    public static boolean createIndex(String index) {
        if (!isIndexExist(index)) {
            LOGGER.info("Index is not exits!");
        }
        //1:settings
        Map<String, Object> settings_map = new HashMap<String, Object>(2);
        //指定索引分区的数量
        settings_map.put("number_of_shards", 3);
        //指定索引副本的数量
        settings_map.put("number_of_replicas", 2);
        XContentBuilder builder = null;
        try {
            //2:mappings（映射、schema）
            builder = XContentFactory.jsonBuilder()
                    .startObject()
                    /**
                     *
                     * 你可以通过dynamic设置来控制这一行为，它能够接受以下的选项：
                     * true：默认值。动态添加字段
                     * false：忽略新字段
                     * strict：如果碰到陌生字段，抛出异常
                     */
                    .field("dynamic", "true")
                    //设置type中的属性
                    .startObject("properties")
                    //id属性     每设置一个 .startObject   下面的配置就是重新开始的   所以标题不用分词器 注释掉 .field("analyzer", "ik_max_word")
                    .startObject("title")
                    //类型是integer
                    .field("type", "text")
                    //不分词，但是建索引
                    /**
                     * index这个属性，no代表不建索引
                     * not_analyzed，建索引不分词
                     * analyzed 即分词，又建立索引
                     * expected [no], [not_analyzed] or [analyzed]
                     */
                    .field("index", "true")
//                    .field("analyzer", "ik_max_word")
                    //在文档中存储
                    .field("store", "true")
                    .endObject()
                    //name属性
                    .startObject("content")
                    //string类型
                    .field("type", "text")
                    //在文档中存储
                    .field("store", "true")
                    //建立索引
                    .field("index", "true")
                    //使用ik_smart进行分词
                    .field("analyzer", "ik_max_word")
                    .endObject()
                    .startObject("createDate")
                    //string类型
                    .field("type", "date")
                    .field("format", "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis")
                    //在文档中存储
                    .field("store", "true")
                    //建立索引
                    .field("index", "true")
                    .endObject()
                    .endObject()
                    .endObject();
        } catch (Exception e) {
            LOGGER.error("error", e);
            return false;
        }

        CreateIndexRequestBuilder createIndexRequestBuilder = client.admin().indices().prepareCreate(index);
//        CreateIndexResponse indexResponse = client.admin().indices().prepareCreate(index).execute().actionGet();
        //管理索引（user_info）然后关联type（user）
        CreateIndexResponse user = createIndexRequestBuilder.setSettings(settings_map).addMapping("law_info", builder).get();
        LOGGER.info("执行建立成功？" + user.isAcknowledged());
        return user.isAcknowledged();
    }

    public static boolean createQuestionIndex(String index,String type) {
        if (!isIndexExist(index)) {
            LOGGER.info("Index is not exits!");
        }
        //1:settings
        Map<String, Object> settings_map = new HashMap<String, Object>(2);
        //指定索引分区的数量
        settings_map.put("number_of_shards", 3);
        //指定索引副本的数量
        settings_map.put("number_of_replicas", 2);
        XContentBuilder builder = null;
        try {
            //2:mappings（映射、schema）
            builder = XContentFactory.jsonBuilder()
                    .startObject()
                    /**
                     *
                     * 你可以通过dynamic设置来控制这一行为，它能够接受以下的选项：
                     * true：默认值。动态添加字段
                     * false：忽略新字段
                     * strict：如果碰到陌生字段，抛出异常
                     */
                    .field("dynamic", "true")
                    //设置type中的属性
                    .startObject("properties")
                    //id属性     每设置一个 .startObject   下面的配置就是重新开始的   所以标题不用分词器 注释掉 .field("analyzer", "ik_max_word")
                    .startObject("title")
                    //类型是integer
                    .field("type", "text")
                    //不分词，但是建索引
                    /**
                     * index这个属性，no代表不建索引
                     * not_analyzed，建索引不分词
                     * analyzed 即分词，又建立索引
                     * expected [no], [not_analyzed] or [analyzed]
                     */
                    .field("index", "true")
//                    .field("analyzer", "ik_max_word")
                    //在文档中存储
                    .field("store", "true")
                    .endObject()
                    //name属性
                    .startObject("content")
                    //string类型
                    .field("type", "text")
                    //在文档中存储
                    .field("store", "true")
                    //建立索引
                    .field("index", "true")
                    //使用ik_smart进行分词
                    .field("analyzer", "ik_max_word")
                    .endObject()
                    .startObject("createDate")
                    //string类型
                    .field("type", "date")
                    .field("format", "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis")
                    //在文档中存储
                    .field("store", "true")
                    //建立索引
                    .field("index", "true")
                    .endObject()
                    .endObject()
                    .endObject();
        } catch (Exception e) {
            LOGGER.error("error", e);
            return false;
        }

        CreateIndexRequestBuilder createIndexRequestBuilder = client.admin().indices().prepareCreate(index);
//        CreateIndexResponse indexResponse = client.admin().indices().prepareCreate(index).execute().actionGet();
        //管理索引（user_info）然后关联type（user）
        CreateIndexResponse user = createIndexRequestBuilder.setSettings(settings_map).addMapping( type, builder).get();
        LOGGER.info("执行建立成功？" + user.isAcknowledged());
        return user.isAcknowledged();
    }

    /**
     * 删除索引
     *
     * @param index
     * @return
     */
    public static boolean deleteIndex(String index) {
        if (!isIndexExist(index)) {
            LOGGER.info("Index is not exits!");
        }
        DeleteIndexResponse dResponse = client.admin().indices().prepareDelete(index).execute().actionGet();
        if (dResponse.isAcknowledged()) {
            LOGGER.info("delete index " + index + "  successfully!");
        } else {
            LOGGER.info("Fail to delete index " + index);
        }
        return dResponse.isAcknowledged();
    }

    /**
     * 判断索引是否存在
     *
     * @param index
     * @return
     */
    public static boolean isIndexExist(String index) {
        IndicesExistsResponse inExistsResponse = client.admin().indices().exists(new IndicesExistsRequest(index)).actionGet();
        if (inExistsResponse.isExists()) {
            LOGGER.info("Index [" + index + "] is exist!");
        } else {
            LOGGER.info("Index [" + index + "] is not exist!");
        }
        return inExistsResponse.isExists();
    }

    /**
     * 数据添加，正定ID
     *
     * @param jsonObject 要增加的数据
     * @param index      索引，类似数据库
     * @param type       类型，类似表
     * @param id         数据ID
     * @return
     */
    public static String addData(JSONObject jsonObject, String index, String type, String id) {
        IndexResponse response = client.prepareIndex(index, type, id).setSource(jsonObject).get();
//        LOGGER.info("addData response status:{},id:{}", response.status().getStatus(), response.getId());
        return response.getId();
    }

    /**
     * 数据添加
     *
     * @param jsonObject 要增加的数据
     * @param index      索引，类似数据库
     * @param type       类型，类似表
     * @return
     */
    public static String addData(JSONObject jsonObject, String index, String type) {
        return addData(jsonObject, index, type, UUID.randomUUID().toString().replaceAll("-", "").toUpperCase());
    }

    /**
     * 通过ID删除数据
     *
     * @param index 索引，类似数据库
     * @param type  类型，类似表
     * @param id    数据ID
     */
    public static void deleteDataById(String index, String type, String id) {
        DeleteResponse response = client.prepareDelete(index, type, id).execute().actionGet();
        LOGGER.info("deleteDataById response status:{},id:{}", response.status().getStatus(), response.getId());
    }

    /**
     * 通过ID 更新数据
     *
     * @param jsonObject 要增加的数据
     * @param index      索引，类似数据库
     * @param type       类型，类似表
     * @param id         数据ID
     * @return
     */
    public static void updateDataById(JSONObject jsonObject, String index, String type, String id) {
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index(index).type(type).id(id).doc(jsonObject);
        client.update(updateRequest);
    }

    /**
     * 通过ID获取数据
     *
     * @param index  索引，类似数据库
     * @param type   类型，类似表
     * @param id     数据ID
     * @param fields 需要显示的字段，逗号分隔（缺省为全部字段）
     * @return
     */
    public static Map<String, Object> searchDataById(String index, String type, String id, String fields) {
        GetRequestBuilder getRequestBuilder = client.prepareGet(index, type, id);
        if (StringUtils.isNotEmpty(fields)) {
            getRequestBuilder.setFetchSource(fields.split(","), null);
        }
        GetResponse getResponse = getRequestBuilder.execute().actionGet();
        return getResponse.getSource();
    }

    /**
     * 使用分词查询,并分页
     *
     * @param shouldMap      模糊查询（字段：值  的形式）
     * @param mustMap        精确查询（字段：值  的形式）
     * @param startPage      当前页
     * @param pageSize       每页显示条数
     * @param isHight        是否高亮
     * @param index          索引名称
     * @param type           类型名称,可传入多个type逗号分隔
     * @param fields         返回字段，逗号分隔
     * @param sortField      排序字段
     * @param highlightField 高亮字段,逗号分隔
     * @return
     */
    public static PageDto<T> searchDataPage(Map<String, Object> shouldMap, Map<String, Object> mustMap, int startPage, int pageSize, boolean isHight, String index, String type, String fields, String sortField, String highlightField) {
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index);
        if (StringUtils.isNotEmpty(type)) {
            searchRequestBuilder.setTypes(type.split(","));
        }
        searchRequestBuilder.setSearchType(SearchType.QUERY_THEN_FETCH);

        // 需要显示的字段，逗号分隔（缺省为全部字段）
        if (StringUtils.isNotEmpty(fields)) {
            searchRequestBuilder.setFetchSource(fields.split(","), null);
        }

        // 高亮（xxx=111,aaa=222）
        if (StringUtils.isNotEmpty(highlightField)) {
            HighlightBuilder highlightBuilder = new HighlightBuilder();

            if (isHight) {
                //设置前缀
                highlightBuilder.preTags(PRETAG_RED);
            } else {
                //设置前缀
                highlightBuilder.preTags(PRETAG);
            }
            //设置后缀
            highlightBuilder.postTags(POSTAG);

            // 设置高亮字段
            String[] highlightFields = highlightField.split(",");
            for (String hight : highlightFields) {
                highlightBuilder.field(new HighlightBuilder.Field(hight));
            }
            searchRequestBuilder.highlighter(highlightBuilder);
        }
        //模糊查询条件
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        if (null != shouldMap && shouldMap.size() > 0) {
            BoolQueryBuilder query = QueryBuilders.boolQuery();
            for (Map.Entry<String, Object> entry : shouldMap.entrySet()) {
                if ("title".equals(entry.getKey())) {
                    query.should(QueryBuilders.matchPhrasePrefixQuery(entry.getKey(), entry.getValue()).boost(100f));
                } else {
               /*     query.should(QueryBuilders.multiMatchQuery(entry.getValue().toString().trim(), entry.getKey()).minimumShouldMatch("100%"))
                            .should(QueryBuilders.wildcardQuery(entry.getKey(), "*" + entry.getValue().toString() + "*"));*/
                    query.should(QueryBuilders.wildcardQuery(entry.getKey(),"*"+entry.getValue().toString()+"*"));
                    query.should(QueryBuilders.matchQuery(entry.getKey(),entry.getValue().toString()));
                }
            }
            boolQuery.must(query);
        } else {
            //排序字段
            if (StringUtils.isNotEmpty(sortField)) {
                searchRequestBuilder.addSort(sortField, SortOrder.DESC);
            }
        }
        //精确查询条件
        if (null != mustMap && mustMap.size() > 0) {
            for (Map.Entry<String, Object> entry : mustMap.entrySet()) {
                boolQuery.must(QueryBuilders.multiMatchQuery(entry.getValue(), entry.getKey()));
            }
        }

        searchRequestBuilder.setQuery(boolQuery);

        // 分页应用
        if (pageSize != -1){
            searchRequestBuilder.setFrom((startPage - 1) * pageSize).setSize(pageSize);
        }

        // 设置是否按查询匹配度排序
        searchRequestBuilder.setExplain(true);
        searchRequestBuilder.addSort(new ScoreSortBuilder());
        searchRequestBuilder.setTrackScores(true);

        //打印的内容 可以在 Elasticsearch head 和 Kibana  上执行查询
//        LOGGER.info("\n{}", searchRequestBuilder);

        // 执行搜索,返回搜索响应信息
        if (!isIndexExist(index)) {
            createIndex(index);
        }
        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();

        long totalHits = searchResponse.getHits().totalHits;
        //如果未查询到数据，重新加载一次
        if (pageSize == -1 ){
            //查询所有
            pageSize = (int)totalHits;
            searchRequestBuilder.setSize((int)totalHits);
        }
        searchResponse = searchRequestBuilder.execute().actionGet();
        long length = searchResponse.getHits().getHits().length;

//        LOGGER.info("共查询到[{}]条数据,处理数据条数[{}]", totalHits, length);

        if (searchResponse.status().getStatus() == STATUS) {
            // 解析对象
            List<Map<String, Object>> sourceList = setSearchResponse(searchResponse, highlightField);
            PageDto pageDto = new PageDto();
            pageDto.setData(sourceList);
            pageDto.setPageNo(startPage);
            pageDto.setPageSize(pageSize);
            pageDto.setTotalCount((int) totalHits);
            if(pageSize == 0){
                pageDto.setTotalPages(0);
            }else {
                pageDto.setTotalPages((int) totalHits / pageSize);
            }
            return pageDto;
        }

        return null;

    }

    /**
     * 使用分词查询
     * shouldMap 模糊查询（字段：值  的形式）
     *
     * @param mustMap        精确查询（字段：值  的形式）
     * @param index          索引名称
     * @param type           类型名称,可传入多个type逗号分隔
     * @param fields         返回字段，逗号分隔
     * @param sortField      排序字段
     * @param highlightField 高亮字段,逗号分隔
     * @return
     */
    public static List<Map<String, Object>> searchListData(Map<String, Object> shouldMap, Map<String, Object> mustMap, boolean isHight, String index, String type, String fields, String sortField, String highlightField) {
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index);
        if (StringUtils.isNotEmpty(type)) {
            searchRequestBuilder.setTypes(type.split(","));
        }
        searchRequestBuilder.setSearchType(SearchType.QUERY_THEN_FETCH);

        // 需要显示的字段，逗号分隔（缺省为全部字段）
        if (StringUtils.isNotEmpty(fields)) {
            searchRequestBuilder.setFetchSource(fields.split(","), null);
        }

        //排序字段
        if (StringUtils.isNotEmpty(sortField)) {
            searchRequestBuilder.addSort(sortField, SortOrder.DESC);
        }

        // 高亮（xxx=111,aaa=222）
        if (StringUtils.isNotEmpty(highlightField)) {
            HighlightBuilder highlightBuilder = new HighlightBuilder();

            if (isHight) {
                //设置前缀
                highlightBuilder.preTags(PRETAG_RED);
            } else {
                //设置前缀
                highlightBuilder.preTags(PRETAG);
            }
            //设置后缀
            highlightBuilder.postTags(POSTAG);

            // 设置高亮字段
            String[] highlightFields = highlightField.split(",");
            for (String hight : highlightFields) {
                highlightBuilder.field(new HighlightBuilder.Field(hight));
            }
            searchRequestBuilder.highlighter(highlightBuilder);
        }
        //排序字段
        if (StringUtils.isNotEmpty(sortField)) {
            searchRequestBuilder.addSort(sortField, SortOrder.DESC);
        }
        //模糊查询条件
        String key = "";
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        if (null != shouldMap && shouldMap.size() > 0) {
            BoolQueryBuilder query = QueryBuilders.boolQuery();
            for (Map.Entry<String, Object> entry : shouldMap.entrySet()) {
                key = entry.getValue().toString();
                query.should(QueryBuilders.multiMatchQuery(entry.getValue().toString().trim(), entry.getKey()).minimumShouldMatch("100%"))
                        .should(QueryBuilders.wildcardQuery(entry.getKey(), "*" + entry.getValue().toString() + "*"));
            }
            boolQuery.must(query);
        }
        //精确查询条件
        if (null != mustMap && mustMap.size() > 0) {
            for (Map.Entry<String, Object> entry : mustMap.entrySet()) {
                boolQuery.must(QueryBuilders.matchQuery(entry.getKey(), entry.getValue()).analyzer("ik_max_word"));
            }
        }

        searchRequestBuilder.setQuery(boolQuery);
        searchRequestBuilder.setFrom(0).setSize(10000);

        // 设置是否按查询匹配度排序
        searchRequestBuilder.setExplain(true);

        //打印的内容 可以在 Elasticsearch head 和 Kibana  上执行查询
        LOGGER.info("\n{}", searchRequestBuilder);

        // 执行搜索,返回搜索响应信息
        if (!isIndexExist(index)) {
            createIndex(index);
        }
        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();

        long totalHits = searchResponse.getHits().totalHits;
        //如果未查询到数据，重新加载一次
        if (totalHits <= 0) {
            searchResponse = searchRequestBuilder.execute().actionGet();
        }
        long length = searchResponse.getHits().getHits().length;

        LOGGER.info("共查询到[{}]条数据,处理数据条数[{}]", totalHits, length);

        if (searchResponse.status().getStatus() == STATUS) {
            return setSearchResponse(searchResponse, highlightField);
        }
        return null;
    }

    /**
     * 使用分词查询,并分页
     *@param key          查询关键字
     * @param fields         查询的字段（逗号分隔）
     * @param index          索引名称
     * @param type           类型名称,可传入多个type逗号分隔
     * @param startPage    当前页
     * @param pageSize       每页显示条数
     * @param sortField      排序字段
     * @param highlightField 高亮字段,逗号分隔
     * @return
     */
    /*public static PageDto<T> searchDataPage(String key,int startPage, int pageSize,String fields,String index, String type, String sortField, String highlightField) {
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index);
        if (StringUtils.isNotEmpty(type)) {
            searchRequestBuilder.setTypes(type.split(","));
        }
        searchRequestBuilder.setSearchType(SearchType.QUERY_THEN_FETCH);

        // 需要显示的字段，逗号分隔（缺省为全部字段）
       *//* if (StringUtils.isNotEmpty(fields)) {
            searchRequestBuilder.setFetchSource(fields.split(","), null);
        }*//*

        //排序字段
        if (StringUtils.isNotEmpty(sortField)) {
            searchRequestBuilder.addSort(sortField, SortOrder.DESC);
        }
        //查询条件
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        if(StringUtils.isNotBlank(fields)){
            String[] filds = fields.split(",");
            for(String fild : filds){
                boolQuery.should(QueryBuilders.matchPhraseQuery(fild, key));
                boolQuery.should(QueryBuilders.wildcardQuery(fild,"*"+ key + "*"));
            }
        }
        searchRequestBuilder.setQuery(boolQuery);

        // 分页应用
        searchRequestBuilder.setFrom(startPage).setSize(pageSize);

        // 设置是否按查询匹配度排序
        searchRequestBuilder.setExplain(true);

        //打印的内容 可以在 Elasticsearch head 和 Kibana  上执行查询
        LOGGER.info("\n{}", searchRequestBuilder);

        SearchResponse searchResponse = new SearchResponse();
        try {
            // 执行搜索,返回搜索响应信息
            searchResponse = searchRequestBuilder.execute().actionGet();
        }catch (Exception e){
            addData(index,type);
            searchResponse = searchRequestBuilder.execute().actionGet();
        }

        long totalHits = searchResponse.getHits().totalHits;
        //如果未查询到数据，重新加载一次
        if(totalHits <= 0){
            addData(index,type);
            searchResponse = searchRequestBuilder.execute().actionGet();
        }
        long length = searchResponse.getHits().getHits().length;

        LOGGER.info("共查询到[{}]条数据,处理数据条数[{}]", totalHits, length);

        if (searchResponse.status().getStatus() == 200) {
            // 解析对象
            List<Map<String, Object>> sourceList = highlight(key,searchResponse, highlightField);
            //new EsPage(startPage, pageSize, (int) totalHits, sourceList);

            return PageUtils.setPageDto(startPage,pageSize,sourceList.size(),sourceList);
        }

        return null;

    }*/

    /**
     * 使用分词查询
     *
     * @param index          索引名称
     * @param type           类型名称,可传入多个type逗号分隔
     * @param query          查询条件
     * @param size           文档大小限制
     * @param fields         需要显示的字段，逗号分隔（缺省为全部字段）
     * @param sortField      排序字段
     * @param highlightField 高亮字段
     * @return
     */
    /*public static List<Map<String, Object>> searchListData(String key,int startPage, int pageSize,String fields,String index, String type, String sortField, String highlightField) {

        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index);
        if (StringUtils.isNotEmpty(type)) {
            searchRequestBuilder.setTypes(type.split(","));
        }

        if (StringUtils.isNotEmpty(highlightField)) {

            HighlightBuilder highlightBuilder = new HighlightBuilder();

            highlightBuilder.preTags("<span style='color:yellow' >");//设置前缀
            highlightBuilder.postTags("</span>");//设置后缀

            // 设置高亮字段
            String[] highlightFields = highlightField.split(",");
            for(String hight : highlightFields){
                highlightBuilder.field(new HighlightBuilder.Field(hight));
            }
            highlightBuilder.fragmentSize(800000).numOfFragments(0).requireFieldMatch(false);
            searchRequestBuilder.highlighter(highlightBuilder);

        }

        //查询条件
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        if(StringUtils.isNotBlank(fields)){
            String[] filds = fields.split(",");
            for(String fild : filds){
                boolQuery.should(QueryBuilders.matchPhraseQuery(fild, key));
                boolQuery.should(QueryBuilders.wildcardQuery(fild,"*"+ key + "*"));
            }
        }

        searchRequestBuilder.setQuery(boolQuery);

        if (StringUtils.isNotEmpty(fields)) {
            searchRequestBuilder.setFetchSource(fields.split(","), null);
        }
        searchRequestBuilder.setFetchSource(true);

        if (StringUtils.isNotEmpty(sortField)) {
            searchRequestBuilder.addSort(sortField, SortOrder.DESC);
        }

        //打印的内容 可以在 Elasticsearch head 和 Kibana  上执行查询
        LOGGER.info("\n{}", searchRequestBuilder);

        SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();

        long totalHits = searchResponse.getHits().totalHits;
        long length = searchResponse.getHits().getHits().length;

        LOGGER.info("共查询到[{}]条数据,处理数据条数[{}]", totalHits, length);

        if (searchResponse.status().getStatus() == 200) {
            // 解析对象
            return highlight(key,searchResponse, highlightField);
        }

        return null;

    }*/

    /**
     * 高亮结果集 特殊处理
     *
     * @param searchResponse
     * @param highlightField
     */
    private static List<Map<String, Object>> setSearchResponse(SearchResponse searchResponse, String highlightField) {
        List<Map<String, Object>> sourceList = new ArrayList<Map<String, Object>>();

        SearchHits hits = searchResponse.getHits();
        for (SearchHit searchHit : searchResponse.getHits().getHits()) {
            Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
            searchHit.getSourceAsMap().put("id", searchHit.getId());

            if (StringUtils.isNotEmpty(highlightField)) {
//                LOGGER.info("遍历 高亮结果集，覆盖 正常结果集" + searchHit.getSourceAsMap());
                String[] highlightFields = highlightField.split(",");
                for (String hight : highlightFields) {
                    StringBuffer stringBuffer = new StringBuffer();
                    if (null == searchHit.getHighlightFields().get(hight)) {
                        continue;
                    }
                    Text[] text = searchHit.getHighlightFields().get(hight).getFragments();

                    if (text != null) {
                        for (Text str : text) {
                            stringBuffer.append(str.string());
                        }
                        //遍历 高亮结果集，覆盖 正常结果集
                        searchHit.getSourceAsMap().put(hight, stringBuffer.toString());
                    }
                }
            }
            sourceList.add(searchHit.getSourceAsMap());
        }

        return sourceList;
    }

    /**
     * 设置高亮
     *
     * @param key（查询关键字）
     * @param searchResponse（查询结果）
     * @param highlightField（高亮字段）
     * @return
     */
    private static List<Map<String, Object>> highlight(String key, SearchResponse searchResponse, String highlightField) {
        List<Map<String, Object>> sourceList = new ArrayList<Map<String, Object>>();


        for (SearchHit searchHit : searchResponse.getHits().getHits()) {
            searchHit.getSourceAsMap().put("id", searchHit.getId());

            if (StringUtils.isNotEmpty(highlightField)) {
//                LOGGER.info("遍历 高亮结果集，覆盖 正常结果集" + searchHit.getSourceAsMap());
                String[] highlightFields = highlightField.split(",");
                for (String hight : highlightFields) {
                    StringBuffer stringBuffer = new StringBuffer();
                    if (null == searchHit.getHighlightFields().get(hight)) {
                        continue;
                    }
                    Text[] text = searchHit.getHighlightFields().get(hight).getFragments();

                    if (text != null) {
                        if (text != null) {
                            for (Text str : text) {
                                stringBuffer.append(str.string());
                            }
                        }
                        String hightContent = ObjectUtils.delHtmlTag(stringBuffer.toString());
                        if (hightContent.contains(key)) {
                            searchHit.getSourceAsMap().put(hight, hightContent.replace(key, PRETAG_RED + key + POSTAG));
                        } else {
                            //遍历 高亮结果集，覆盖 正常结果集
                            searchHit.getSourceAsMap().put(hight, stringBuffer.toString());
                        }

                    }

                }

            }
            sourceList.add(searchHit.getSourceAsMap());
        }

        return sourceList;
    }

    public static List<String> get(String key) {
        List<String> list = new ArrayList();
        AnalyzeRequest analyzeRequest = new AnalyzeRequest("file")
                .text(key)
                .analyzer("ik_max_word");

        List<AnalyzeResponse.AnalyzeToken> tokens = client.admin().indices()
                .analyze(analyzeRequest)
                .actionGet()
                .getTokens();

        for (AnalyzeResponse.AnalyzeToken token : tokens) {
            list.add(token.getTerm());
            LOGGER.info(token.getTerm());
        }
        return list;
    }

    /**
     * 去掉字符中的html标签
     *
     * @param
     * @return
     */
    public static String StripHTML(String str) {
        //如果有双引号将其先转成单引号
        String htmlStr = str.replaceAll("\"", "'");
        String regEx_script = "<script[^>]*?>[\\s\\S]*?<\\/script>"; // 定义script的正则表达式
        String regEx_style = "<style[^>]*?>[\\s\\S]*?<\\/style>"; // 定义style的正则表达式
//        String regEx_html = "<[^>]+>"; // 定义HTML标签的正则表达式

        Pattern p_script = Pattern.compile(regEx_script, Pattern.CASE_INSENSITIVE);
        Matcher m_script = p_script.matcher(htmlStr);
        htmlStr = m_script.replaceAll(""); // 过滤script标签

        Pattern p_style = Pattern.compile(regEx_style, Pattern.CASE_INSENSITIVE);
        Matcher m_style = p_style.matcher(htmlStr);
        htmlStr = m_style.replaceAll(""); // 过滤style标签

//        Pattern p_html = Pattern.compile(regEx_html, Pattern.CASE_INSENSITIVE);
//        Matcher m_html = p_html.matcher(htmlStr);
//        htmlStr = m_html.replaceAll(""); // 过滤html标签

        return htmlStr.trim(); //返回文本字符串
    }

}
