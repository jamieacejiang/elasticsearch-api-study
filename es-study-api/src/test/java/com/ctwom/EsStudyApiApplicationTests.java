package com.ctwom;

import com.alibaba.fastjson.JSON;
import com.ctwom.pojo.User;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

/**
 * es-api-7.10.1
 */
@SpringBootTest
class EsStudyApiApplicationTests {

	//通过面向对象来操作
	@Autowired
	@Qualifier("restHighLevelClient")
	private RestHighLevelClient client;

	//测试索引的创建 RestRequest 对应：PUT jiang-index
	@Test
	void testCreateIndex() throws IOException {
		//1.创建索引请求
		CreateIndexRequest request = new CreateIndexRequest("jiang-index");
		//2.执行创建请求 IndicesClient.create(), 请求后获得响应CreateIndexResponse
		CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
		System.out.println(createIndexResponse);
	}

	//测试获取索引 对比：GET jiang-index
	@Test
	void testExistIndex() throws IOException {
		GetIndexRequest request = new GetIndexRequest("jiang-index");
		boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
		System.out.println(exists);
	}

	//测试删除索引 对比 DELETE jiang-index
	@Test
	void testDeleteIndex() throws IOException {
		DeleteIndexRequest request = new DeleteIndexRequest("jiang-index");
		AcknowledgedResponse delete = client.indices().delete(request, RequestOptions.DEFAULT);
		System.out.println(delete.isAcknowledged());

	}

	//测试添加文档 对比：PUT jiang-index/_doc/1
	@Test
	void testAddDocument() throws IOException {
		//创建对象
		User user = new User("狂神说", 3);
		//创建请求
		IndexRequest request = new IndexRequest("jiang-index");
		//设置规则
		request.id("1");
		request.timeout(TimeValue.timeValueSeconds(1));
		//将我们的数据放入请求 json
		request.source(JSON.toJSONString(user), XContentType.JSON);
		//客户端发送请求，获取响应的结果
		IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
		System.out.println(indexResponse.toString());
		System.out.println(indexResponse.status());
	}

	//测试获取文档,判断是否存在 对比：GET jiang-index/_doc/1
	@Test
	void testIsExists() throws IOException {
		GetRequest request = new GetRequest("jiang-index", "1");
		//不获取返回的 _source的上下文了
		request.fetchSourceContext(new FetchSourceContext(false));
		boolean exists = client.exists(request, RequestOptions.DEFAULT);
		System.out.println(exists);
	}

	//测试获取文档的信息
	@Test
	void testGetDocument() throws IOException {
		GetRequest request = new GetRequest("jiang-index", "1");
		GetResponse documentFields = client.get(request, RequestOptions.DEFAULT);
		System.out.println(documentFields.getSourceAsString());//打印文档的内容
		System.out.println(documentFields);//返回的全部内容和命令是一样的
	}

	//更新文档的信息
	@Test
	void testUpdateDocument() throws IOException {
		UpdateRequest request = new UpdateRequest("jiang-index", "1");
		request.timeout("1s");
		User user = new User("狂神说JAVA", 18);
		request.doc(JSON.toJSONString(user), XContentType.JSON);
		UpdateResponse update = client.update(request, RequestOptions.DEFAULT);
		System.out.println(update);

	}

	//删除文档记录
	@Test
	void testDeleteDocument() throws IOException {
		DeleteRequest request = new DeleteRequest("jiang-index", "1");
		request.timeout("1s");
		DeleteResponse delete = client.delete(request, RequestOptions.DEFAULT);
		System.out.println(delete.status());
	}

	//真实的项目一般都会批量插入数据
	@Test
	void testBulkRequest() throws IOException {
		BulkRequest request = new BulkRequest();
		request.timeout("10s");
		ArrayList<User> userList = new ArrayList<>();
		userList.add(new User("kuangshen1", 3));
		userList.add(new User("kuangshen2", 3));
		userList.add(new User("kuangshen3", 3));
		userList.add(new User("aha1", 3));
		userList.add(new User("aha2", 3));
		userList.add(new User("aha3", 3));
		userList.add(new User("aha4", 3));
		//批处理请求
		for (int i = 0; i < userList.size(); i++) {
			BulkRequest add = request.add(new IndexRequest("jiang-index")
					.id(""+(i+1))
					.source(JSON.toJSONString(userList.get(i)), XContentType.JSON));
		}
		BulkResponse bulk = client.bulk(request, RequestOptions.DEFAULT);
		System.out.println(bulk.hasFailures());
	}

	//查询
	//SearchRequest 搜索请求
	//SearchSourceBuilder 条件构造
	//QueryBuilders 查询条件工具类
	//HighlightBuilder 构建高亮
	//TermQueryBuilder 精确查询
	//MatchQueryBuilder 模糊匹配查询
	//xxxQueryBuilder 对应我们刚才看到的所有命令
	@Test
	void testSearch() throws IOException {
		SearchRequest request = new SearchRequest("jiang-index");
		//构建搜索条件
		SearchSourceBuilder builder = new SearchSourceBuilder();
		//查询条件，我们可以使用QueryBuilders工具类来实现
		//QueryBuilders.termQuery 精确匹配
		//等等，自己可以.点一下
		TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "kuangshen1");
		builder.query(termQueryBuilder);
		builder.from();
		builder.size();
		builder.timeout(new TimeValue(60, TimeUnit.SECONDS));
		request.source(builder);
		SearchResponse search = client.search(request, RequestOptions.DEFAULT);
		System.out.println(JSON.toJSONString(search.getHits()));
		for (SearchHit hit : search.getHits().getHits()) {
			System.out.println(hit.getSourceAsString());
		}
	}
}
