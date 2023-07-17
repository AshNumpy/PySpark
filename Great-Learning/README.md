**PySpark Cheat Sheet - Teorik Bilgiler**

1. **PySpark Nedir?**
   - PySpark, Apache Spark'ın Python API'sini temsil eder.
   - Büyük veri işleme ve analizi için kullanılır.
   - Spark, dağıtık ve hızlı veri işlemeye olanak tanır ve yüksek performans sağlar.

2. **RDD (Resilient Distributed Dataset)**
   - Spark'ın temel veri yapısıdır.
   - Dağıtık ve dayanıklı bir koleksiyon olarak düşünülebilir.
   - Paralel işleme ve hata toleransı sağlar.

3. **SparkSession**
   - Spark uygulamaları başlatmak için kullanılır.
   - SparkSession, DataFrame API'sini kullanmak için gerekli olan giriş noktasıdır.

4. **DataFrame ve Dataset**
   - DataFrame, yapısallaştırılmış verileri temsil eden RDD'lerdir ve Spark SQL ile etkileşimli sorgulamalar ve veri analizleri için kullanılır.
   - Dataset, verileri güçlü tür kontrolleri ve statik yazım ile işleme için tanımlayan bir DataFrame API'sidir.

5. **Transformasyonlar ve Eylemler**
   - Spark, RDD, DataFrame ve Dataset üzerinde transformasyon ve eylem operasyonları sağlar.
   - Transformasyonlar, mevcut veriyi değiştirmeden yeni bir RDD, DataFrame veya Dataset oluşturur.
   - Eylemler, RDD, DataFrame veya Dataset üzerinde bir hesaplama gerçekleştirir ve sonuçları getirir.

6. **Pyspark MLlib**
   - Pyspark MLlib, Spark üzerindeki makine öğrenmesi kütüphanesidir.
   - Temel makine öğrenmesi algoritmalarını içerir: sınıflandırma, regresyon, kümeleme vb.

7. **Persistans**
   - PySpark, verilerin bellekte veya diske kalıcı olarak saklanmasına izin verir.
   - persist() veya cache() metodları kullanılarak RDD'lerin veya DataFrame/Dataset'lerin persiste edilebilir.

8. **Paralel İşleme ve Hata Toleransı**
   - Spark, dağıtık doğası sayesinde verileri paralel olarak işler.
   - Hata durumunda, kaybolan veriler RDD'nin hata toleransı sayesinde otomatik olarak yeniden oluşturulabilir.

9. **Spark Ekosistemi**
   - Spark, birçok veri işleme çerçevesini birleştiren geniş bir ekosisteme sahiptir.
   - HDFS, Hive, HBase, Cassandra gibi diğer büyük veri teknolojileriyle entegrasyon sağlar.

10. **Spark Cluster Yönetimi**
   - Spark, cluster üzerinde çalışmak için çeşitli yönetim modelleri sunar: yerel, YARN, Mesos, Kubernetes vb.
