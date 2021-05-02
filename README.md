# BTL
Machine Learning

MLlib là thư viện máy học (ML) của Spark. Mục tiêu của nó là làm cho việc học máy thực tế có thể mở rộng và dễ dàng. Ở cấp độ cao, nó cung cấp các công cụ như:

Thuật toán ML: các thuật toán học tập phổ biến như phân loại, hồi quy, phân cụm và lọc cộng tác
Lông vũ: trích xuất tính năng, biến đổi, giảm kích thước và lựa chọn
Đường ống: công cụ để xây dựng, đánh giá và điều chỉnh Đường ống ML
Tính bền bỉ: lưu và tải các thuật toán, mô hình và đường ống
Các tiện ích: đại số tuyến tính, thống kê, xử lý dữ liệu, v.v.

Spark MLlib:
Apache Spark cung cấp một API Học máy được gọi là MLlib . PySpark cũng có API học máy này bằng Python. Nó hỗ trợ các loại thuật toán khác nhau, được đề cập bên dưới:
![image](https://user-images.githubusercontent.com/77929150/116805240-3aab4100-ab4f-11eb-8af2-98212b0671e7.png)

mllib.classification - Gói spark.mllib hỗ trợ nhiều phương pháp khác nhau để phân loại nhị phân, phân loại đa lớp và phân tích hồi quy. Một số thuật toán phổ biến nhất trong phân loại là Rừng ngẫu nhiên, Vịnh Naive, Cây quyết định , v.v.

mllib.clustering - Clustering là một vấn đề học tập không có giám sát, theo đó bạn nhằm mục đích nhóm các tập con của các thực thể với nhau dựa trên một số khái niệm về sự giống nhau.

mllib.fpm - Đối sánh mẫu thường xuyên là khai thác các mục thường xuyên, tập phổ biến, chuỗi con hoặc các cấu trúc con khác thường nằm trong số các bước đầu tiên để phân tích một tập dữ liệu quy mô lớn. Đây đã là một chủ đề nghiên cứu tích cực trong việc khai thác dữ liệu trong nhiều năm.

mllib.linalg - Tiện ích MLlib cho đại số tuyến tính.

mllib.recommendation - Lọc cộng tác thường được sử dụng cho các hệ thống khuyến nghị. Các kỹ thuật này nhằm mục đích điền vào các mục còn thiếu của ma trận liên kết mục người dùng.

spark.mllib - Nó hiện hỗ trợ lọc cộng tác dựa trên mô hình, trong đó người dùng và sản phẩm được mô tả bằng một tập hợp nhỏ các yếu tố tiềm ẩn có thể được sử dụng để dự đoán các mục nhập bị thiếu. spark.mllib sử dụng thuật toán Bình phương tối thiểu xen kẽ (ALS) để tìm hiểu các yếu tố tiềm ẩn này.

mllib.regression - Hồi quy tuyến tính thuộc họ thuật toán hồi quy. Mục tiêu của hồi quy là tìm mối quan hệ và sự phụ thuộc giữa các biến. Giao diện làm việc với mô hình hồi quy tuyến tính và tóm tắt mô hình tương tự như trường hợp hồi quy logistic.

Spark MLlib được tích hợp chặt chẽ trên Spark giúp giảm bớt sự phát triển của các thuật toán học máy quy mô lớn hiệu quả như thường là lặp đi lặp lại trong tự nhiên.

Cộng đồng mã nguồn mở của Spark đã dẫn đến sự phát triển nhanh chóng và việc áp dụng Spark MLlib. Có hơn 200 cá nhân từ 75 tổ chức cung cấp khoảng hơn 2000 bản vá chỉ riêng cho MLlib.
MLlib dễ triển khai và không yêu cầu cài đặt trước, nếu Hadoop 2 cluster đã được cài đặt và đang chạy.

Khả năng mở rộng, tính đơn giản và khả năng tương thích ngôn ngữ của Spark MLlib (bạn có thể viết ứng dụng bằng Java, Scala và Python) giúp các nhà khoa học dữ liệu giải quyết các vấn đề dữ liệu lặp lại nhanh hơn. Các nhà khoa học dữ liệu có thể tập trung vào các vấn đề dữ liệu quan trọng trong khi tận dụng một cách minh bạch tốc độ, sự dễ dàng và tích hợp chặt chẽ của nền tảng thống nhất của Spark.

MLlib cung cấp hiệu suất cao nhất cho các nhà khoa học dữ liệu và nhanh hơn từ 10 đến 100 lần so với Hadoop và Apache Mahout. Các thuật toán học máy Alternating Least Squares trên Amazon Đánh giá trên tập dữ liệu gồm 660 triệu người dùng, 2,4 triệu mục và xếp hạng 3,5 B chạy trong 40 phút với 50 nút.

Thống kê tóm tắt :

from pyspark.mllib.stat import Statistics

sc = ... # SparkContext

mat = ... # an RDD of Vectors

//# Compute column summary statistics.//
summary = Statistics.colStats(mat)

print(summary.mean())
print(summary.variance())
print(summary.numNonzeros())

Tương quan:
from pyspark.mllib.stat import Statistics

sc = ... # SparkContext

seriesX = ... # a series
seriesY = ... # must have the same number of partitions and cardinality as seriesX


print(Statistics.corr(seriesX, seriesY, method="pearson"))

data = ... # an RDD of Vectors
 
print(Statistics.corr(data, method="pearson"))

Lấy mẫu phân tầng:
sc = ... # SparkContext

data = ... # an RDD of any key value pairs
fractions = ... # specify the exact fraction desired from each key as a dictionary

approxSample = data.sampleByKey(False, fractions);

Tạo dữ liệu ngẫu nhiên:
from pyspark.mllib.random import RandomRDDs

sc = ... # SparkContext


u = RandomRDDs.uniformRDD(sc, 1000000L, 10)

v = u.map(lambda x: 1.0 + 2.0 * x)
Khai thác và chuyển đổi tính năng:
from pyspark.ml.feature import HashingTF, IDF, Tokenizer

sentenceData = spark.createDataFrame([
    (0.0, "Hi I heard about Spark"),
    (0.0, "I wish Java could use case classes"),
    (1.0, "Logistic regression models are neat")
], ["label", "sentence"])

tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
wordsData = tokenizer.transform(sentenceData)

hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=20)
featurizedData = hashingTF.transform(wordsData)

idf = IDF(inputCol="rawFeatures", outputCol="features")
idfModel = idf.fit(featurizedData)
rescaledData = idfModel.transform(featurizedData)

rescaledData.select("label", "features").show()
DataFrames

DataFrame là một kiểu dữ liệu collection phân tán, được tổ chức thành các cột được đặt tên. Về mặt khái niệm, nó tương đương với các bảng quan hệ (relational tables) đi kèm với các kỹ thuật tối ưu tính toán.
DataFrame có thể được xây dựng từ nhiều nguồn dữ liệu khác nhau như Hive table, các file dữ liệu có cấu trúc hay bán cấu trúc (csv, json), các hệ cơ sở dữ liệu phổ biến (MySQL, MongoDB, Cassandra), hoặc RDDs hiện hành. API này được thiết kế cho các ứng dụng Big Data và Data Science hiện đại. Kiểu dữ liệu này được lấy cảm hứng từ DataFrame trong Lập trình R và Pandas trong Python hứa hẹn mang lại hiệu suất tính toán cao hơn.

![image](https://user-images.githubusercontent.com/77929150/116803187-91f5e500-ab40-11eb-99ab-ee8071e4063a.png)

--Thiết lập cho file build.sbt
Ở đây, sẽ quản lý các dependencies thông qua sbt. Bên dưới là các dependencies cần để tiến hành các thực nghiệm truy vấn dữ liệu.

name := "spark-dataframes"
 
version := "1.0"
 
scalaVersion := "2.10.5"

libraryDependencies ++= Seq(

  "org.apache.spark" % "spark-core_2.10" % "1.6.1",
  
  "org.apache.spark" % "spark-sql_2.10" % "1.6.1",
  
  "com.databricks" % "spark-csv_2.10" % "1.4.0"
  
)

--Chạy một truy vấn đơn giản

import org.apache.spark._

import org.apache.spark.SparkContext

import org.apache.spark.sql.SQLContext

object MainExecutor {

  def main(args: Array[String]) {
  
 val conf = new SparkConf()
 
      .setAppName("Spark DataFrames Application")
      
      .setMaster("local[2]")
      
    val sc = new SparkContext(conf)
    
    val sqlContext = new SQLContext(sc)
    
    // Loading customers data
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load("db/csv/customers.csv")
 
    // Let sqlContext know which table you gonna work with
    df.registerTempTable("Customers")
    df.show(5)
  }
} 

Nếu thành công, bạn sẽ nhận được output như bên dưới.

+----------+--------------------+------------------+--------------------+-----------+----------+-------+

|CustomerID|        CustomerName|       ContactName|             Address|       City|PostalCode|Country|

+----------+--------------------+------------------+--------------------+-----------+----------+-------+

|         1| Alfreds Futterkiste|      Maria Anders|       Obere Str. 57|     Berlin|     12209|Germany|

|         2|Ana Trujillo Empa...|      Ana Trujillo|Avda. de la Const...|México D.F.|      5021| Mexico|

|         3|Antonio Moreno Ta...|    Antonio Moreno|      Mataderos 2312|México D.F.|      5023| Mexico|

|         4|     Around the Horn|      Thomas Hardy|     120 Hanover Sq.|     London|   WA1 1DP|     UK|

|         5|  Berglunds snabbköp|Christina Berglund|      Berguvsvägen 8|      Luleå|  S-958 22| Sweden|

+----------+--------------------+------------------+--------------------+-----------+----------+-------+

only showing top 5 rows

Câu lệnh SELECT

Bạn có 2 lựa chọn để có thể truy vấn dữ liệu

Sử dụng sqlContext với hàm sql(): bạn sẽ cảm thấy thân thuộc ngay vì đây hoàn toàn là các câu lệnh SQL bạn từng làm việc.

Sử dụng DataFrames: kiểu dữ liệu DataFrames cung cấp cho các bạn đầy đủ các hàm tương tự như SQL (SQL-like)  để bạn dễ dàng truy vấn.

// SELECT by sqlContext

sqlContext.sql("SELECT CustomerName,City FROM Customers").show(5)

// SELECT by DataFrames

df.select("CustomerName", "City").show(5)

// Output
+--------------------+-----------+

|        CustomerName|       City|

+--------------------+-----------+

| Alfreds Futterkiste|     Berlin|

|Ana Trujillo Empa...|México D.F.|

|Antonio Moreno Ta...|México D.F.|

|     Around the Horn|     London|

|  Berglunds snabbköp|      Luleå|

+--------------------+-----------+

only showing top 5 rows

SQL SELECT DISTINCT Statement

// SELECT DISTINCT by sqlContext
sqlContext.sql("SELECT DISTINCT City FROM Customers").show(5)
// SELECT DISTINCT by DataFrames
df.select("City").distinct().show(5)
 
// Output
+--------+
|    City|
+--------+
|  Aachen|
|    Bern|
|  Nantes|
|Toulouse|
|   Walla|
+--------+
only showing top 5 rows

SQL WHERE Clause
Scala còn cung cấp cho bạn lựa chọn viết code SQL trên nhiều dòng bằng cách sử dụng dấu nháy 3 “””_”””. Thêm vào đó, bạn còn có thể lựa chọn phong cách viết code theo trường phái Scala hay Java như ví dụ minh hoạ bên dưới. Tôi sẽ phân biệt hai trường phái này thông qua hai hàm where (scala) và filter (java).

// WHERE clause

import org.apache.spark.sql.functions._

sqlContext.sql(

  """
  
    SELECT * FROM Customers
    
    WHERE Country='Mexico'
    
  """).show(5)
  
// Scala style

df.where("Country='Mexico'").show(5)

// Java style

df.filter(col("Country") === "Mexico").show(5)

// Output
+----------+--------------------+--------------------+--------------------+-----------+----------+-------+

|CustomerID|        CustomerName|         ContactName|             Address|       City|PostalCode|Country|

+----------+--------------------+--------------------+--------------------+-----------+----------+-------+

|         2|Ana Trujillo Empa...|        Ana Trujillo|Avda. de la Const...|México D.F.|      5021| Mexico|

|         3|Antonio Moreno Ta...|      Antonio Moreno|      Mataderos 2312|México D.F.|      5023| Mexico|

|        13|Centro comercial ...|     Francisco Chang|Sierras de Granad...|México D.F.|      5022| Mexico|

|        58|Pericles Comidas ...| Guillermo Fernández|Calle Dr. Jorge C...|México D.F.|      5033| Mexico|

|        80| Tortuga Restaurante|Miguel Angel Paolino|    Avda. Azteca 123|México D.F.|      5033| Mexico|

+----------+--------------------+--------------------+--------------------+-----------+----------+-------+

