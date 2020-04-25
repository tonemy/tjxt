package util;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import model.Shop;
import model.ShopScore;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.bson.Document;

import java.io.IOException;

/**
 * @Author: 张今天
 * @Date: 2020/4/25 12:29
 */
public class MongoOutputFormatScore implements OutputFormat<ShopScore> {

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
//         mongoDatabase.createCollection("ShopScore");

        System.out.println("Connect to database successfully");
    }

    @Override
    public void writeRecord(ShopScore shopScore) throws IOException {

        MongoCollection<Document> collection = mongoDatabase.getCollection("ShopScore");
        //插入文档
        Document document = new Document("id", shopScore.getId()).
                append("shopId", shopScore.getShopId()).
                append("score", shopScore.getScore()).
                append("timestamp", shopScore.getTimestamp());
        collection.insertOne(document);
        System.out.println("数据rating插入成功");
    }

    @Override
    public void close() throws IOException {
        System.out.println("关闭 mongo 连接");
//        mongoClient.close();
    }
}
