#!/usr/bin/env python3
"""
MSK Debezium Connector 验证脚本

用于验证 Debezium MySQL Connector 是否正常工作。
功能：创建数据库/表、写入数据、检查 Kafka topics、读取消息。
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime

import pymysql
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient

# 配置 logging
logger = logging.getLogger(__name__)


class ColorFormatter(logging.Formatter):
    """带颜色的日志格式化器"""

    COLORS = {
        "DEBUG": "\033[36m",  # Cyan
        "INFO": "\033[32m",  # Green
        "WARNING": "\033[33m",  # Yellow
        "ERROR": "\033[31m",  # Red
        "CRITICAL": "\033[35m",  # Magenta
    }
    RESET = "\033[0m"
    BOLD = "\033[1m"

    def format(self, record):
        color = self.COLORS.get(record.levelname, self.RESET)
        record.levelname = f"{color}{record.levelname:<7}{self.RESET}"
        record.msg = f"{record.msg}"
        return super().format(record)


def setup_logging(verbose: bool = False):
    """配置日志"""
    level = logging.DEBUG if verbose else logging.INFO

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        ColorFormatter("%(asctime)s │ %(levelname)s │ %(message)s", datefmt="%H:%M:%S")
    )

    # 禁用 kafka 库的冗余日志
    logging.getLogger("kafka").setLevel(logging.WARNING)

    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.addHandler(handler)


def get_mysql_connection(
    host: str, port: int, user: str, password: str, database: str | None = None
):
    """创建 MySQL 连接"""
    return pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )


def log_section(title: str, step: int | None = None):
    """输出分隔段落标题"""
    prefix = f"{step}. " if step else ""
    logger.info("=" * 60)
    logger.info(f"{prefix}{title}")
    logger.info("=" * 60)


def setup_database(
    host: str, port: int, user: str, password: str, database: str, table: str
):
    """创建数据库和表"""
    log_section("创建数据库和表", step=1)

    conn = get_mysql_connection(host, port, user, password)
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
            logger.info(f"数据库 '{database}' 已创建/存在")

            cursor.execute(f"USE {database}")

            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    email VARCHAR(100),
                    status VARCHAR(20) DEFAULT 'active',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                )
            """)
            logger.info(f"表 '{table}' 已创建/存在")

        conn.commit()
    finally:
        conn.close()


def insert_test_data(
    host: str, port: int, user: str, password: str, database: str, table: str
):
    """插入测试数据"""
    log_section("插入测试数据", step=2)

    conn = get_mysql_connection(host, port, user, password, database)
    try:
        with conn.cursor() as cursor:
            timestamp = datetime.now().strftime("%H%M%S")
            test_data = [
                (f"User_A_{timestamp}", f"user_a_{timestamp}@test.com", "active"),
                (f"User_B_{timestamp}", f"user_b_{timestamp}@test.com", "active"),
                (f"User_C_{timestamp}", f"user_c_{timestamp}@test.com", "pending"),
            ]

            for name, email, status in test_data:
                cursor.execute(
                    f"INSERT INTO {table} (name, email, status) VALUES (%s, %s, %s)",
                    (name, email, status),
                )
                logger.info(f"INSERT: {name} - {email}")

        conn.commit()
        logger.info(f"共插入 {len(test_data)} 条数据")
    finally:
        conn.close()


def update_test_data(
    host: str, port: int, user: str, password: str, database: str, table: str
):
    """更新测试数据"""
    log_section("更新测试数据", step=3)

    conn = get_mysql_connection(host, port, user, password, database)
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT id, name FROM {table} ORDER BY id DESC LIMIT 1")
            row = cursor.fetchone()
            if row:
                new_email = f"updated_{row['id']}@test.com"
                cursor.execute(
                    f"UPDATE {table} SET email = %s, status = 'updated' WHERE id = %s",
                    (new_email, row["id"]),
                )
                logger.info(
                    f"UPDATE: id={row['id']}, name={row['name']} -> email={new_email}"
                )
            else:
                logger.warning("没有找到可更新的记录")
        conn.commit()
    finally:
        conn.close()


def delete_test_data(
    host: str, port: int, user: str, password: str, database: str, table: str
):
    """删除测试数据"""
    log_section("删除测试数据", step=4)

    conn = get_mysql_connection(host, port, user, password, database)
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                f"SELECT id, name FROM {table} WHERE status = 'pending' LIMIT 1"
            )
            row = cursor.fetchone()
            if row:
                cursor.execute(f"DELETE FROM {table} WHERE id = %s", (row["id"],))
                logger.info(f"DELETE: id={row['id']}, name={row['name']}")
            else:
                logger.warning("没有找到 status='pending' 的记录")
        conn.commit()
    finally:
        conn.close()


def show_current_data(
    host: str, port: int, user: str, password: str, database: str, table: str
):
    """显示当前表数据"""
    log_section("当前 MySQL 表数据", step=5)

    conn = get_mysql_connection(host, port, user, password, database)
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {table} ORDER BY id DESC LIMIT 10")
            rows = cursor.fetchall()

            if rows:
                header = f"{'ID':<6} {'Name':<20} {'Email':<35} {'Status':<10}"
                separator = f"{'-' * 6} {'-' * 20} {'-' * 35} {'-' * 10}"
                logger.info(header)
                logger.info(separator)
                for row in rows:
                    logger.info(
                        f"{row['id']:<6} {row['name']:<20} {row['email'] or 'N/A':<35} {row['status']:<10}"
                    )
            else:
                logger.warning("表为空")
    finally:
        conn.close()


def list_topics(bootstrap_servers: str, topic_prefix: str) -> list[str]:
    """列出相关的 Kafka topics"""
    log_section("Kafka Topics 列表", step=6)

    try:
        admin = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers, request_timeout_ms=10000
        )
        topics = admin.list_topics()

        relevant_topics = [t for t in sorted(topics) if topic_prefix in t]

        logger.info(f"包含 '{topic_prefix}' 的 Topics:")
        for t in relevant_topics:
            logger.info(f"  • {t}")

        if not relevant_topics:
            logger.warning(f"未找到包含 '{topic_prefix}' 的 topics")

        admin.close()
        return relevant_topics
    except Exception as e:
        logger.error(f"连接 Kafka 失败: {e}")
        return []


def consume_messages(
    bootstrap_servers: str, topic: str, max_messages: int = 20, timeout_ms: int = 10000
):
    """消费 Kafka 消息并显示原始数据"""
    log_section(f"读取 Topic: {topic}", step=7)

    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset="earliest",
            consumer_timeout_ms=timeout_ms,
            request_timeout_ms=15000,
        )

        messages = []
        for msg in consumer:
            messages.append(msg)
            if len(messages) >= max_messages:
                break

        consumer.close()

        if not messages:
            logger.warning(f"Topic '{topic}' 中没有消息")
            return

        logger.info(f"共读取 {len(messages)} 条消息")

        for i, msg in enumerate(messages, 1):
            logger.info("─" * 56)
            logger.info(f"消息 #{i}")
            logger.info("─" * 56)
            logger.info(f"Partition: {msg.partition}, Offset: {msg.offset}")
            logger.info(
                f"Timestamp: {datetime.fromtimestamp(msg.timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')}"
            )

            if msg.key:
                logger.info(f"Key: {msg.key.decode('utf-8')}")

            logger.info("原始 Value (JSON):")
            if msg.value:
                try:
                    value = json.loads(msg.value.decode("utf-8"))
                    # 分行输出 JSON
                    for line in json.dumps(value, indent=2, ensure_ascii=False).split(
                        "\n"
                    ):
                        logger.debug(f"  {line}")

                    # 输出解析摘要
                    logger.info("解析摘要:")
                    op = value.get("op", "N/A")
                    op_name = {
                        "c": "INSERT",
                        "u": "UPDATE",
                        "d": "DELETE",
                        "r": "SNAPSHOT",
                    }.get(op, op)
                    logger.info(f"  操作类型: {op_name} ({op})")

                    if value.get("before"):
                        logger.info(
                            f"  Before: {json.dumps(value['before'], ensure_ascii=False)}"
                        )
                    if value.get("after"):
                        logger.info(
                            f"  After: {json.dumps(value['after'], ensure_ascii=False)}"
                        )

                    source = value.get("source", {})
                    if source:
                        logger.info(
                            f"  Source: db={source.get('db')}, table={source.get('table')}, "
                            f"pos={source.get('file')}:{source.get('pos')}"
                        )
                except json.JSONDecodeError:
                    logger.warning(f"JSON 解析失败: {msg.value[:500]}")

    except Exception as e:
        logger.error(f"读取消息失败: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="MSK Debezium Connector 验证脚本",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 完整验证流程
  python verify_connector.py \\
      --mysql-host my-db.rds.amazonaws.com \\
      --mysql-user admin \\
      --mysql-password 'MyPassword' \\
      --bootstrap-servers broker1:9092,broker2:9092 \\
      --topic-prefix myprefix

  # 只读取 Kafka 消息
  python verify_connector.py \\
      --bootstrap-servers broker1:9092 \\
      --topic-prefix myprefix \\
      --skip-mysql

  # 详细输出 (包含完整 JSON)
  python verify_connector.py -v
        """,
    )

    parser.add_argument(
        "--mysql-host",
        default="common-test.cpwuo9y53vjh.us-east-1.rds.amazonaws.com",
        help="MySQL 主机地址",
    )
    parser.add_argument(
        "--mysql-port", type=int, default=3306, help="MySQL 端口 (默认: 3306)"
    )
    parser.add_argument("--mysql-user", default="admin", help="MySQL 用户名")
    parser.add_argument("--mysql-password", default="Ssa123456#$", help="MySQL 密码")
    parser.add_argument(
        "--database", default="test_db", help="数据库名称 (默认: test_db)"
    )
    parser.add_argument(
        "--table", default="verify_test", help="表名称 (默认: verify_test)"
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="boot-4qw.msklogstream.oee1gg.c16.kafka.us-east-1.amazonaws.com:9092",
        help="Kafka bootstrap servers",
    )
    parser.add_argument(
        "--topic-prefix", default="test_prefix", help="Topic 前缀 (默认: test_prefix)"
    )
    parser.add_argument(
        "--data-topic", default=None, help="数据 Topic 名称 (默认: {prefix}_all_data)"
    )
    parser.add_argument(
        "--max-messages", type=int, default=10, help="最多读取的消息数量 (默认: 10)"
    )
    parser.add_argument(
        "--skip-mysql", action="store_true", help="跳过 MySQL 操作，只检查 Kafka"
    )
    parser.add_argument("--skip-insert", action="store_true", help="跳过插入数据")
    parser.add_argument("--skip-update", action="store_true", help="跳过更新数据")
    parser.add_argument("--skip-delete", action="store_true", help="跳过删除数据")
    parser.add_argument(
        "--wait", type=int, default=3, help="MySQL 操作后等待秒数 (默认: 3)"
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="详细输出 (包含完整 JSON)"
    )

    args = parser.parse_args()

    # 初始化日志
    setup_logging(verbose=args.verbose)

    data_topic = args.data_topic or f"{args.topic_prefix}_all_data"

    logger.info("=" * 60)
    logger.info("MSK Debezium Connector 验证")
    logger.info("=" * 60)
    logger.info(f"MySQL: {args.mysql_host}:{args.mysql_port}")
    logger.info(f"Database: {args.database}, Table: {args.table}")
    logger.info(f"Kafka: {args.bootstrap_servers}")
    logger.info(f"Topic: {data_topic}")

    if not args.skip_mysql:
        setup_database(
            args.mysql_host,
            args.mysql_port,
            args.mysql_user,
            args.mysql_password,
            args.database,
            args.table,
        )

        if not args.skip_insert:
            insert_test_data(
                args.mysql_host,
                args.mysql_port,
                args.mysql_user,
                args.mysql_password,
                args.database,
                args.table,
            )

        if not args.skip_update:
            update_test_data(
                args.mysql_host,
                args.mysql_port,
                args.mysql_user,
                args.mysql_password,
                args.database,
                args.table,
            )

        if not args.skip_delete:
            delete_test_data(
                args.mysql_host,
                args.mysql_port,
                args.mysql_user,
                args.mysql_password,
                args.database,
                args.table,
            )

        show_current_data(
            args.mysql_host,
            args.mysql_port,
            args.mysql_user,
            args.mysql_password,
            args.database,
            args.table,
        )

        if args.wait > 0:
            logger.info(f"等待 {args.wait} 秒让 Debezium 同步数据...")
            time.sleep(args.wait)

    list_topics(args.bootstrap_servers, args.topic_prefix)

    consume_messages(args.bootstrap_servers, data_topic, args.max_messages)

    logger.info("=" * 60)
    logger.info("验证完成")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
