package t1.datamarts.load

import org.apache.hadoop.fs.{FileSystem, Path}
import java.net.URI
import t1.datamarts.extract.Connect

/**
 *
 * Очистка временных данных Spark
 */
class Clear(tempDir: String) extends Connect {

  // Получаем FileSystem
  val fs = FileSystem.get(new URI(tempDir), spark.sparkContext.hadoopConfiguration)

  // Очистка папки
  if (fs.exists(new Path(tempDir))) {
    fs.delete(new Path(tempDir), true) // true для рекурсивного удаления
    println(s"Папка $tempDir успешно удалена.")
  } else {
    println(s"Папка $tempDir не существует.")
  }

  // Закрываем FileSystem
  fs.close()
}
