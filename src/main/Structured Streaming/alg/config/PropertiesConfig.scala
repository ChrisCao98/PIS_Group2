package alg.config

import java.io.FileInputStream
import java.util.Properties

/**
 * 配置管理类，负责从application.properties加载配置
 * 支持多级配置优先级：
 * 1. 系统属性 (-D参数)
 * 2. 环境变量
 * 3. 配置文件
 * 4. 默认值
 */
object PropertiesConfig {
  private val properties = new Properties()
  private val configPath = "config/application.properties"

  // 初始化时加载配置
  try {
    val input = new FileInputStream(configPath)
    properties.load(input)
    input.close()
  } catch {
    case e: Exception =>
      println(s"警告: 无法加载配置文件 $configPath")
      e.printStackTrace()
  }

  /**
   * 获取配置值，如果不存在则返回默认值
   * @param key 配置键
   * @param defaultValue 默认值
   * @return 配置值或默认值
   */
  def getProperty(key: String, defaultValue: String): String = {
    // 按优先级顺序检查配置:
    // 1. 系统属性
    // 2. 环境变量（将key转换为大写，点替换为下划线）
    // 3. 配置文件
    // 4. 默认值
    Option(System.getProperty(key))
      .orElse(Option(System.getenv(key.toUpperCase.replace('.', '_'))))
      .orElse(Option(properties.getProperty(key)))
      .getOrElse(defaultValue)
  }

  /**
   * 获取必需的配置值，如果不存在则抛出异常
   * @param key 配置键
   * @return 配置值
   * @throws IllegalStateException 如果配置值不存在
   */
  def getRequiredProperty(key: String): String = {
    getProperty(key, null) match {
      case null => throw new IllegalStateException(s"必需的配置项 '$key' 未找到")
      case value => value
    }
  }
}