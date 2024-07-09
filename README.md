# Spark example
 This is a test application for word counting

 If an exception occurs: Exception in thread main java.lang.IllegalAccessError: class org.apache.spark.storage.StorageUtils$ (in unnamed module @0x67080771) cannot access class sun.nio.ch.DirectBuffer (in module java.base) because module java.base does not export sun.nio.ch to unnamed module @0x6708077 
 
 Add the JVM option "--add-exports java.base/sun.nio.ch=ALL-UNNAMED"