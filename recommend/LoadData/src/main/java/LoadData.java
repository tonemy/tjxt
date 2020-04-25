
import model.Shop;
import model.ShopScore;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import util.MongoOutputFormat;
import util.MongoOutputFormatScore;


/**
 * @Author: 张今天
 * @Date: 2020/4/24 14:45
 */


public class LoadData {
// 流处理   //mongdb 连接
//    public static MongoClient getConnect() {
//        ServerAddress serverAddress = new ServerAddress("localhost", 27017);
//        List<MongoCredential> credential = new ArrayList<MongoCredential>();
//        MongoCredential mongoCredential1 = MongoCredential.createScramSha1Credential("", "", "".toCharArray());
//        credential.add(mongoCredential1);
//        MongoClient mongoClient = new MongoClient(serverAddress, credential);
//        return mongoClient;
//    }
//
//    public class MongoDBSink extends RichSinkFunction<Tuple5<String, String, String, String, String>> {
//        private static final long serialVersionUID = 1L;
//        MongoClient mongoClient = null;
//
//        public void invoke(Tuple5<String, String, String, String, String> value) {
//            try {
//                if (mongoClient != null) {
//                    mongoClient = getConnect();
//                    MongoDatabase db = mongoClient.getDatabase("tjxt");
//                    MongoCollection collection = db.getCollection("tjxt");
//                    List<Document> list = new ArrayList<Document>();
//                    Document doc = new Document();
//                    doc.append("IP", value.f0);
//                    doc.append("TIME", value.f1);
//                    doc.append("CourseID", value.f2);
//                    doc.append("Status_Code", value.f3);
//                    doc.append("Referer", value.f4);
//                    list.add(doc);
//                    System.out.println("Insert Starting");
//                    collection.insertMany(list);
//                }
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
//
//        public void open(Configuration parms) throws Exception {
//            super.open(parms);
//            mongoClient = getConnect();
//        }
//
//        public void close() throws Exception {
//            if (mongoClient != null) {
//                mongoClient.close();
//            }
//        }
//    }


    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Shop> originData = env.readCsvFile("recommend/LoadData/src/main/resources/products.csv")
                .fieldDelimiter("^")
                .includeFields(true, true, false, false, true, true, true)
                .pojoType(Shop.class, "id", "title", "imgUrl", "categories", "tags");
        originData.print();
        originData.output(new MongoOutputFormat());

        DataSet<ShopScore> originShopScore = env.readCsvFile("recommend/LoadData/src/main/resources/ratings.csv")
                .fieldDelimiter(",")
                .includeFields(true, true, true, true)
                .pojoType(ShopScore.class, "id", "shopId", "score", "timestamp");
        originShopScore.print();
        originShopScore.output(new MongoOutputFormatScore());
         env.execute();
    }
}
