package util;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import model.Shop;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.bson.Document;

import java.io.IOException;

/**
 * @Author: 张今天
 * @Date: 2020/4/25 11:05
 */

public class MongoOutputFormat implements OutputFormat<Shop> {

    static MongoClient mongoClient = null;
    static MongoDatabase mongoDatabase = null;

    @Override
    public void configure(Configuration parameters) {
        System.out.println("mongoSink 配置中");
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        System.out.println("连接到 mongoDB ");
        // 连接到 mongodb 服务
        mongoClient = new MongoClient("127.0.0.1", 27017);
        // 连接到数据库
        mongoDatabase = mongoClient.getDatabase("tjxt");

        // 创建集合
//        mongoDatabase.createCollection("Shop");

        System.out.println("Connect to database successfully");
    }

    @Override
    public void writeRecord(Shop shop) throws IOException {

        MongoCollection<Document> collection = mongoDatabase.getCollection("Shop");
        //插入文档
        Document document = new Document("id", shop.getId()).
                append("title", shop.getTitle()).
                append("imgUrl", shop.getImgUrl()).
                append("categories", shop.getCategories()).
                append("tags", shop.getTags());
        collection.insertOne(document);
        System.out.println("数据插入成功");
    }

    @Override
    public void close() throws IOException {
        System.out.println("关闭 mongo 连接");
        mongoClient.close();
    }
}

