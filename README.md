# Spark example
    Приложение по базовым функциям spark
    Если получили исключение: Exception in thread main java.lang.IllegalAccessError: class org.apache.spark.storage.StorageUtils$ (in unnamed module @0x67080771) cannot access class sun.nio.ch.DirectBuffer (in module java.base) because module java.base does not export sun.nio.ch to unnamed module @0x6708077 
    ТО  добовляем в JVM option "--add-exports java.base/sun.nio.ch=ALL-UNNAMED"
 
    Если spark установлен на винде запуск мастера будет таким:
    перейти в диркеторию bin и выполнить:
    spark-class org.apache.spark.deploy.master.Master
    после этого на  http://localhost:8080/ запуститься мастер
    запуск воркера на том же узле что и мастер
    перейти  в диркеторию bin и выполнить:
    spark-class org.apache.spark.deploy.worker.Worker <тут должен быть адрес хоста например spark://172.29.176.1:7077>

    Для создания нескольких воркеров используем:
    docker pull spark
    docker run -it spark /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker <тут пишем адрес узла мастера spark://172.29.176.1:7077>