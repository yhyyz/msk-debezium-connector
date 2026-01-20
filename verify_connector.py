#!/usr/bin/env python3
"""
MSK Debezium Connector 验证脚本

用于验证 Debezium MySQL Connector 是否正常工作。
功能：创建数据库/表、写入数据、检查 Kafka topics、读取消息并验证数据一致性。
"""

import argparse
import json
import logging
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime

import pymysql
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient

logger = logging.getLogger(__name__)


@dataclass
class Operation:
    op_type: str  # INSERT, UPDATE, DELETE
    record_id: int
    name: str
    email: str
    status: str
    old_email: str | None = None
    old_status: str | None = None


@dataclass
class VerifyResult:
    inserted: list[Operation] = field(default_factory=list)
    updated: list[Operation] = field(default_factory=list)
    deleted: list[Operation] = field(default_factory=list)
    matched_inserts: list[int] = field(default_factory=list)
    matched_updates: list[int] = field(default_factory=list)
    matched_deletes: list[int] = field(default_factory=list)


class ColorFormatter(logging.Formatter):
    COLORS = {
        "DEBUG": "\033[36m",
        "INFO": "\033[32m",
        "WARNING": "\033[33m",
        "ERROR": "\033[31m",
        "CRITICAL": "\033[35m",
    }
    RESET = "\033[0m"

    def format(self, record):
        color = self.COLORS.get(record.levelname, self.RESET)
        record.levelname = f"{color}{record.levelname:<7}{self.RESET}"
        return super().format(record)


def setup_logging(verbose: bool = False):
    level = logging.DEBUG if verbose else logging.INFO
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        ColorFormatter("%(asctime)s │ %(levelname)s │ %(message)s", datefmt="%H:%M:%S")
    )
    logging.getLogger("kafka").setLevel(logging.WARNING)
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.addHandler(handler)


def get_mysql_connection(
    host: str, port: int, user: str, password: str, database: str | None = None
):
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
    prefix = f"{step}. " if step else ""
    logger.info("=" * 60)
    logger.info(f"{prefix}{title}")
    logger.info("=" * 60)


def setup_database(
    host: str, port: int, user: str, password: str, database: str, table: str
):
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
) -> list[Operation]:
    log_section("插入测试数据", step=2)

    operations = []
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
                record_id = cursor.lastrowid
                operations.append(
                    Operation(
                        op_type="INSERT",
                        record_id=record_id,
                        name=name,
                        email=email,
                        status=status,
                    )
                )
                logger.info(f"INSERT: id={record_id}, {name}, {email}, {status}")

        conn.commit()
        logger.info(f"共插入 {len(operations)} 条数据")
    finally:
        conn.close()

    return operations


def update_test_data(
    host: str, port: int, user: str, password: str, database: str, table: str
) -> list[Operation]:
    log_section("更新测试数据", step=3)

    operations = []
    conn = get_mysql_connection(host, port, user, password, database)
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                f"SELECT id, name, email, status FROM {table} ORDER BY id DESC LIMIT 1"
            )
            row = cursor.fetchone()
            if row:
                new_email = f"updated_{row['id']}@test.com"
                new_status = "updated"
                cursor.execute(
                    f"UPDATE {table} SET email = %s, status = %s WHERE id = %s",
                    (new_email, new_status, row["id"]),
                )
                operations.append(
                    Operation(
                        op_type="UPDATE",
                        record_id=row["id"],
                        name=row["name"],
                        email=new_email,
                        status=new_status,
                        old_email=row["email"],
                        old_status=row["status"],
                    )
                )
                logger.info(
                    f"UPDATE: id={row['id']}, {row['name']}, "
                    f"email: {row['email']} -> {new_email}, "
                    f"status: {row['status']} -> {new_status}"
                )
            else:
                logger.warning("没有找到可更新的记录")
        conn.commit()
    finally:
        conn.close()

    return operations


def delete_test_data(
    host: str, port: int, user: str, password: str, database: str, table: str
) -> list[Operation]:
    log_section("删除测试数据", step=4)

    operations = []
    conn = get_mysql_connection(host, port, user, password, database)
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                f"SELECT id, name, email, status FROM {table} WHERE status = 'pending' LIMIT 1"
            )
            row = cursor.fetchone()
            if row:
                cursor.execute(f"DELETE FROM {table} WHERE id = %s", (row["id"],))
                operations.append(
                    Operation(
                        op_type="DELETE",
                        record_id=row["id"],
                        name=row["name"],
                        email=row["email"],
                        status=row["status"],
                    )
                )
                logger.info(
                    f"DELETE: id={row['id']}, {row['name']}, {row['email']}, {row['status']}"
                )
            else:
                logger.warning("没有找到 status='pending' 的记录")
        conn.commit()
    finally:
        conn.close()

    return operations


def show_current_data(
    host: str, port: int, user: str, password: str, database: str, table: str
):
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


def consume_and_verify(
    bootstrap_servers: str,
    topic: str,
    result: VerifyResult,
    table: str,
    max_messages: int = 100,
    timeout_ms: int = 15000,
    verbose: bool = False,
) -> list[dict]:
    log_section(f"读取并验证 Topic: {topic}", step=7)

    cdc_events = []

    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset="earliest",
            consumer_timeout_ms=timeout_ms,
            request_timeout_ms=20000,
        )

        for msg in consumer:
            if msg.value:
                try:
                    value = json.loads(msg.value.decode("utf-8"))
                    source = value.get("source", {})
                    if source.get("table") == table:
                        cdc_events.append(value)
                except json.JSONDecodeError:
                    pass
            if len(cdc_events) >= max_messages:
                break

        consumer.close()

        if not cdc_events:
            logger.warning(f"Topic '{topic}' 中没有 {table} 表的消息")
            return []

        logger.info(f"找到 {len(cdc_events)} 条 {table} 表的 CDC 事件")

        op_map = {"c": "INSERT", "u": "UPDATE", "d": "DELETE", "r": "SNAPSHOT"}

        for op in result.inserted:
            for event in cdc_events:
                if event.get("op") == "c":
                    after = event.get("after", {})
                    if (
                        after.get("id") == op.record_id
                        and after.get("name") == op.name
                        and after.get("email") == op.email
                    ):
                        result.matched_inserts.append(op.record_id)
                        break

        for op in result.updated:
            for event in cdc_events:
                if event.get("op") == "u":
                    after = event.get("after", {})
                    before = event.get("before", {})
                    if (
                        after.get("id") == op.record_id
                        and after.get("email") == op.email
                        and after.get("status") == op.status
                        and before.get("email") == op.old_email
                    ):
                        result.matched_updates.append(op.record_id)
                        break

        for op in result.deleted:
            for event in cdc_events:
                if event.get("op") == "d":
                    before = event.get("before", {})
                    if (
                        before.get("id") == op.record_id
                        and before.get("name") == op.name
                    ):
                        result.matched_deletes.append(op.record_id)
                        break

        logger.info("─" * 56)
        logger.info("CDC 事件详情")
        logger.info("─" * 56)

        displayed = 0
        for event in cdc_events:
            op_code = event.get("op", "?")
            op_name = op_map.get(op_code, op_code)
            after = event.get("after", {})
            before = event.get("before", {})

            record_id = after.get("id") or before.get("id")

            is_our_record = (
                record_id in [o.record_id for o in result.inserted]
                or record_id in [o.record_id for o in result.updated]
                or record_id in [o.record_id for o in result.deleted]
            )

            if not is_our_record and not verbose:
                continue

            displayed += 1
            marker = "◆" if is_our_record else "○"

            if op_code == "c":
                logger.info(
                    f"{marker} [{op_name}] id={record_id}, "
                    f"name={after.get('name')}, email={after.get('email')}, status={after.get('status')}"
                )
            elif op_code == "u":
                logger.info(
                    f"{marker} [{op_name}] id={record_id}, "
                    f"email: {before.get('email')} -> {after.get('email')}, "
                    f"status: {before.get('status')} -> {after.get('status')}"
                )
            elif op_code == "d":
                logger.info(
                    f"{marker} [{op_name}] id={record_id}, "
                    f"name={before.get('name')}, email={before.get('email')}"
                )
            elif op_code == "r":
                if verbose:
                    logger.debug(
                        f"{marker} [{op_name}] id={record_id}, "
                        f"name={after.get('name')}, email={after.get('email')}"
                    )

        if displayed == 0:
            logger.info("(无本次测试的 CDC 事件，使用 -v 查看所有事件)")

        return cdc_events

    except Exception as e:
        logger.error(f"读取消息失败: {e}")
        return []


def print_verification_summary(result: VerifyResult):
    log_section("验证结果", step=8)

    total_ops = len(result.inserted) + len(result.updated) + len(result.deleted)
    total_matched = (
        len(result.matched_inserts)
        + len(result.matched_updates)
        + len(result.matched_deletes)
    )

    GREEN = "\033[32m"
    RED = "\033[31m"
    YELLOW = "\033[33m"
    RESET = "\033[0m"

    logger.info(f"{'操作类型':<12} {'执行数':<10} {'匹配数':<10} {'状态':<10}")
    logger.info(f"{'-' * 12} {'-' * 10} {'-' * 10} {'-' * 10}")

    insert_ok = len(result.matched_inserts) == len(result.inserted)
    update_ok = len(result.matched_updates) == len(result.updated)
    delete_ok = len(result.matched_deletes) == len(result.deleted)

    insert_status = f"{GREEN}✓ PASS{RESET}" if insert_ok else f"{RED}✗ FAIL{RESET}"
    update_status = f"{GREEN}✓ PASS{RESET}" if update_ok else f"{RED}✗ FAIL{RESET}"
    delete_status = f"{GREEN}✓ PASS{RESET}" if delete_ok else f"{RED}✗ FAIL{RESET}"

    if result.inserted:
        logger.info(
            f"{'INSERT':<12} {len(result.inserted):<10} {len(result.matched_inserts):<10} {insert_status}"
        )
    if result.updated:
        logger.info(
            f"{'UPDATE':<12} {len(result.updated):<10} {len(result.matched_updates):<10} {update_status}"
        )
    if result.deleted:
        logger.info(
            f"{'DELETE':<12} {len(result.deleted):<10} {len(result.matched_deletes):<10} {delete_status}"
        )

    logger.info(f"{'-' * 12} {'-' * 10} {'-' * 10} {'-' * 10}")

    if total_ops == 0:
        logger.warning("没有执行任何操作")
    elif total_matched == total_ops:
        logger.info(f"{GREEN}总计: {total_matched}/{total_ops} 全部匹配 ✓{RESET}")
    else:
        logger.warning(f"{YELLOW}总计: {total_matched}/{total_ops} 部分匹配{RESET}")

        missing_inserts = set(o.record_id for o in result.inserted) - set(
            result.matched_inserts
        )
        missing_updates = set(o.record_id for o in result.updated) - set(
            result.matched_updates
        )
        missing_deletes = set(o.record_id for o in result.deleted) - set(
            result.matched_deletes
        )

        if missing_inserts:
            logger.warning(f"  未匹配的 INSERT: ids={list(missing_inserts)}")
        if missing_updates:
            logger.warning(f"  未匹配的 UPDATE: ids={list(missing_updates)}")
        if missing_deletes:
            logger.warning(f"  未匹配的 DELETE: ids={list(missing_deletes)}")

        logger.info("提示: 可能是 Debezium 同步延迟，尝试增加 --wait 参数")


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

  # 详细输出 (包含所有 CDC 事件)
  python verify_connector.py -v --wait 5
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
        "--max-messages", type=int, default=100, help="最多读取的消息数量 (默认: 100)"
    )
    parser.add_argument(
        "--skip-mysql", action="store_true", help="跳过 MySQL 操作，只检查 Kafka"
    )
    parser.add_argument("--skip-insert", action="store_true", help="跳过插入数据")
    parser.add_argument("--skip-update", action="store_true", help="跳过更新数据")
    parser.add_argument("--skip-delete", action="store_true", help="跳过删除数据")
    parser.add_argument(
        "--wait", type=int, default=5, help="MySQL 操作后等待秒数 (默认: 5)"
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="详细输出 (包含所有 CDC 事件)"
    )

    args = parser.parse_args()
    setup_logging(verbose=args.verbose)

    data_topic = args.data_topic or f"{args.topic_prefix}_all_data"
    result = VerifyResult()

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
            result.inserted = insert_test_data(
                args.mysql_host,
                args.mysql_port,
                args.mysql_user,
                args.mysql_password,
                args.database,
                args.table,
            )

        if not args.skip_update:
            result.updated = update_test_data(
                args.mysql_host,
                args.mysql_port,
                args.mysql_user,
                args.mysql_password,
                args.database,
                args.table,
            )

        if not args.skip_delete:
            result.deleted = delete_test_data(
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

    consume_and_verify(
        args.bootstrap_servers,
        data_topic,
        result,
        args.table,
        args.max_messages,
        verbose=args.verbose,
    )

    if not args.skip_mysql:
        print_verification_summary(result)

    logger.info("=" * 60)
    logger.info("验证完成")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
