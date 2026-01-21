#!/usr/bin/env python3
"""
MSK Debezium Connector 验证脚本
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
    op_type: str
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


class Colors:
    CYAN = "\033[36m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    RED = "\033[31m"
    GRAY = "\033[90m"
    BOLD = "\033[1m"
    RESET = "\033[0m"


class Console:
    @staticmethod
    def header(title: str):
        width = 60
        print(f"\n{Colors.CYAN}╭{'─' * width}╮{Colors.RESET}")
        print(
            f"{Colors.CYAN}│{Colors.RESET}  {Colors.BOLD}{title:<{width - 3}}{Colors.RESET}{Colors.CYAN}│{Colors.RESET}"
        )
        print(f"{Colors.CYAN}╰{'─' * width}╯{Colors.RESET}")
        print()

    @staticmethod
    def config(key: str, value: str):
        print(f"  {Colors.GRAY}{key:<8}{Colors.RESET}{value}")

    @staticmethod
    def step(num: int, total: int, title: str, status: str = ""):
        status_str = f" {Colors.GREEN}✓{Colors.RESET}" if status == "ok" else ""
        print(f"\n{Colors.BOLD}[{num}/{total}]{Colors.RESET} {title}{status_str}")

    @staticmethod
    def item(symbol: str, text: str, color: str = ""):
        color_code = getattr(Colors, color.upper(), "") if color else ""
        reset = Colors.RESET if color_code else ""
        print(f"      {color_code}{symbol}{reset} {text}")

    @staticmethod
    def table_row(cols: list[str], widths: list[int], is_header: bool = False):
        row = "│"
        for col, width in zip(cols, widths):
            row += f" {col:<{width}} │"
        if is_header:
            print(f"      {Colors.GRAY}{row}{Colors.RESET}")
        else:
            print(f"      {row}")

    @staticmethod
    def table_sep(widths: list[int], style: str = "mid"):
        chars = {"top": ("┌", "┬", "┐"), "mid": ("├", "┼", "┤"), "bot": ("└", "┴", "┘")}
        left, mid, right = chars.get(style, chars["mid"])
        line = left + mid.join("─" * (w + 2) for w in widths) + right
        print(f"      {Colors.GRAY}{line}{Colors.RESET}")

    @staticmethod
    def summary(matched: int, total: int):
        if total == 0:
            print(f"\n      {Colors.YELLOW}⚠ 没有执行任何操作{Colors.RESET}")
        elif matched == total:
            print(
                f"\n      {Colors.GREEN}总计: {matched}/{total} 全部匹配 ✓{Colors.RESET}"
            )
        else:
            print(
                f"\n      {Colors.YELLOW}总计: {matched}/{total} 部分匹配{Colors.RESET}"
            )

    @staticmethod
    def footer(start_time: float):
        elapsed = time.time() - start_time
        now = datetime.now().strftime("%H:%M:%S")
        print(f"\n{Colors.GRAY}{'─' * 62}{Colors.RESET}")
        print(f"  {Colors.GRAY}完成于 {now}  耗时 {elapsed:.1f}s{Colors.RESET}\n")

    @staticmethod
    def warning(text: str):
        print(f"      {Colors.YELLOW}⚠ {text}{Colors.RESET}")

    @staticmethod
    def error(text: str):
        print(f"      {Colors.RED}✗ {text}{Colors.RESET}")


def setup_logging(verbose: bool = False):
    level = logging.DEBUG if verbose else logging.WARNING
    logging.basicConfig(level=level, format="%(message)s")
    logging.getLogger("kafka").setLevel(logging.ERROR)


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


def setup_database(
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
    table: str,
    total_steps: int,
) -> bool:
    Console.step(1, total_steps, "创建数据库和表")
    try:
        conn = get_mysql_connection(host, port, user, password)
        with conn.cursor() as cursor:
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
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
        conn.commit()
        conn.close()
        Console.item("✓", f"数据库 '{database}'", "green")
        Console.item("✓", f"表 '{table}'", "green")
        return True
    except Exception as e:
        Console.error(str(e))
        return False


def insert_test_data(
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
    table: str,
    total_steps: int,
) -> list[Operation]:
    Console.step(2, total_steps, "插入测试数据")
    operations = []
    try:
        conn = get_mysql_connection(host, port, user, password, database)
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
                operations.append(Operation("INSERT", record_id, name, email, status))
                Console.item(
                    "+", f"id={record_id:<4} {name:<18} {email:<30} {status}", "green"
                )
        conn.commit()
        conn.close()
    except Exception as e:
        Console.error(str(e))
    return operations


def update_test_data(
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
    table: str,
    total_steps: int,
) -> list[Operation]:
    Console.step(3, total_steps, "更新测试数据")
    operations = []
    try:
        conn = get_mysql_connection(host, port, user, password, database)
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
                        "UPDATE",
                        row["id"],
                        row["name"],
                        new_email,
                        new_status,
                        row["email"],
                        row["status"],
                    )
                )
                Console.item(
                    "~",
                    f"id={row['id']:<4} email: {row['email']} → {new_email}",
                    "yellow",
                )
            else:
                Console.warning("没有找到可更新的记录")
        conn.commit()
        conn.close()
    except Exception as e:
        Console.error(str(e))
    return operations


def delete_test_data(
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
    table: str,
    total_steps: int,
) -> list[Operation]:
    Console.step(4, total_steps, "删除测试数据")
    operations = []
    try:
        conn = get_mysql_connection(host, port, user, password, database)
        with conn.cursor() as cursor:
            cursor.execute(
                f"SELECT id, name, email, status FROM {table} WHERE status = 'pending' LIMIT 1"
            )
            row = cursor.fetchone()
            if row:
                cursor.execute(f"DELETE FROM {table} WHERE id = %s", (row["id"],))
                operations.append(
                    Operation(
                        "DELETE", row["id"], row["name"], row["email"], row["status"]
                    )
                )
                Console.item(
                    "-", f"id={row['id']:<4} {row['name']:<18} {row['email']}", "red"
                )
            else:
                Console.warning("没有符合条件的记录")
        conn.commit()
        conn.close()
    except Exception as e:
        Console.error(str(e))
    return operations


def show_current_data(
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
    table: str,
    total_steps: int,
):
    Console.step(5, total_steps, "当前表数据")
    try:
        conn = get_mysql_connection(host, port, user, password, database)
        with conn.cursor() as cursor:
            cursor.execute(
                f"SELECT id, name, email, status FROM {table} ORDER BY id DESC LIMIT 5"
            )
            rows = cursor.fetchall()
            if rows:
                print(
                    f"      {Colors.GRAY}{'ID':<6} {'Name':<18} {'Email':<30} {'Status':<10}{Colors.RESET}"
                )
                for row in rows:
                    print(
                        f"      {row['id']:<6} {row['name']:<18} {(row['email'] or 'N/A'):<30} {row['status']:<10}"
                    )
            else:
                Console.warning("表为空")
        conn.close()
    except Exception as e:
        Console.error(str(e))


def list_topics(
    bootstrap_servers: str, topic_prefix: str, step_num: int, total_steps: int
) -> list[str]:
    Console.step(step_num, total_steps, "Kafka Topics")
    try:
        admin = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers, request_timeout_ms=10000
        )
        topics = admin.list_topics()
        relevant = [t for t in sorted(topics) if topic_prefix in t]
        for t in relevant:
            Console.item("•", t)
        if not relevant:
            Console.warning(f"未找到包含 '{topic_prefix}' 的 topics")
        admin.close()
        return relevant
    except Exception as e:
        Console.error(f"连接失败: {e}")
        return []


def consume_and_verify(
    bootstrap_servers: str,
    topic: str,
    result: VerifyResult,
    table: str,
    step_num: int,
    total_steps: int,
    max_messages: int = 100,
    timeout_ms: int = 15000,
    verbose: bool = False,
) -> bool:
    Console.step(step_num, total_steps, "CDC 事件验证")

    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset="earliest",
            consumer_timeout_ms=timeout_ms,
            request_timeout_ms=20000,
        )

        cdc_events = []
        for msg in consumer:
            if msg.value:
                try:
                    value = json.loads(msg.value.decode("utf-8"))
                    if value.get("source", {}).get("table") == table:
                        cdc_events.append(value)
                except json.JSONDecodeError:
                    pass
            if len(cdc_events) >= max_messages:
                break
        consumer.close()

        if not cdc_events:
            Console.warning(f"没有找到 {table} 表的 CDC 事件")
            return False

        for op in result.inserted:
            for event in cdc_events:
                if event.get("op") == "c":
                    after = event.get("after", {})
                    if after.get("id") == op.record_id and after.get("name") == op.name:
                        result.matched_inserts.append(op.record_id)
                        break

        for op in result.updated:
            for event in cdc_events:
                if event.get("op") == "u":
                    after = event.get("after", {})
                    if (
                        after.get("id") == op.record_id
                        and after.get("email") == op.email
                    ):
                        result.matched_updates.append(op.record_id)
                        break

        for op in result.deleted:
            for event in cdc_events:
                if event.get("op") == "d":
                    before = event.get("before", {})
                    if before.get("id") == op.record_id:
                        result.matched_deletes.append(op.record_id)
                        break

        our_ids = {
            o.record_id for o in result.inserted + result.updated + result.deleted
        }

        for event in cdc_events:
            op = event.get("op")
            after = event.get("after", {})
            before = event.get("before", {})
            record_id = after.get("id") or before.get("id")

            if record_id not in our_ids and not verbose:
                continue

            marker = "◆" if record_id in our_ids else "○"

            if op == "c":
                Console.item(
                    marker,
                    f"INSERT  id={record_id:<4} {after.get('name', '')}",
                    "green",
                )
            elif op == "u":
                Console.item(
                    marker,
                    f"UPDATE  id={record_id:<4} email → {after.get('email', '')}",
                    "yellow",
                )
            elif op == "d":
                Console.item(
                    marker, f"DELETE  id={record_id:<4} {before.get('name', '')}", "red"
                )
            elif op == "r" and verbose:
                Console.item(
                    marker, f"SNAP    id={record_id:<4} {after.get('name', '')}", "gray"
                )

        return True

    except Exception as e:
        Console.error(f"读取失败: {e}")
        return False


def print_verification_summary(result: VerifyResult, step_num: int, total_steps: int):
    Console.step(step_num, total_steps, "验证结果")

    widths = [8, 6, 6, 8]
    Console.table_sep(widths, "top")
    Console.table_row(["操作", "执行", "匹配", "状态"], widths, is_header=True)
    Console.table_sep(widths, "mid")

    rows = []
    if result.inserted:
        ok = len(result.matched_inserts) == len(result.inserted)
        status = (
            f"{Colors.GREEN}✓ PASS{Colors.RESET}"
            if ok
            else f"{Colors.RED}✗ FAIL{Colors.RESET}"
        )
        rows.append(
            (
                "INSERT",
                str(len(result.inserted)),
                str(len(result.matched_inserts)),
                status,
            )
        )

    if result.updated:
        ok = len(result.matched_updates) == len(result.updated)
        status = (
            f"{Colors.GREEN}✓ PASS{Colors.RESET}"
            if ok
            else f"{Colors.RED}✗ FAIL{Colors.RESET}"
        )
        rows.append(
            (
                "UPDATE",
                str(len(result.updated)),
                str(len(result.matched_updates)),
                status,
            )
        )

    if result.deleted:
        ok = len(result.matched_deletes) == len(result.deleted)
        status = (
            f"{Colors.GREEN}✓ PASS{Colors.RESET}"
            if ok
            else f"{Colors.RED}✗ FAIL{Colors.RESET}"
        )
        rows.append(
            (
                "DELETE",
                str(len(result.deleted)),
                str(len(result.matched_deletes)),
                status,
            )
        )

    for row in rows:
        Console.table_row(list(row), widths)

    Console.table_sep(widths, "bot")

    total = len(result.inserted) + len(result.updated) + len(result.deleted)
    matched = (
        len(result.matched_inserts)
        + len(result.matched_updates)
        + len(result.matched_deletes)
    )
    Console.summary(matched, total)


def main():
    parser = argparse.ArgumentParser(
        description="MSK Debezium Connector 验证脚本",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--mysql-host", default="common-test.cpwuo9y53vjh.us-east-1.rds.amazonaws.com"
    )
    parser.add_argument("--mysql-port", type=int, default=3306)
    parser.add_argument("--mysql-user", default="admin")
    parser.add_argument("--mysql-password", default="Ssa123456#$")
    parser.add_argument("--database", default="test_db")
    parser.add_argument("--table", default="verify_test")
    parser.add_argument(
        "--bootstrap-servers",
        default="boot-4qw.msklogstream.oee1gg.c16.kafka.us-east-1.amazonaws.com:9092",
    )
    parser.add_argument("--topic-prefix", default="test_prefix")
    parser.add_argument("--data-topic", default=None)
    parser.add_argument("--max-messages", type=int, default=100)
    parser.add_argument(
        "--skip-mysql", action="store_true", help="跳过 MySQL 操作，只检查 Kafka"
    )
    parser.add_argument(
        "--mysql-only", action="store_true", help="只执行 MySQL 操作，跳过 Kafka 验证"
    )
    parser.add_argument("--skip-insert", action="store_true")
    parser.add_argument("--skip-update", action="store_true")
    parser.add_argument("--skip-delete", action="store_true")
    parser.add_argument("--wait", type=int, default=5)
    parser.add_argument("-v", "--verbose", action="store_true")

    args = parser.parse_args()
    setup_logging(verbose=args.verbose)

    start_time = time.time()
    data_topic = args.data_topic or f"{args.topic_prefix}_all_data"
    result = VerifyResult()

    total_steps = 5 if args.mysql_only else 8

    Console.header("MSK Debezium Connector 验证")
    Console.config("MySQL", f"{args.mysql_host}:{args.mysql_port}")
    Console.config("DB", f"{args.database} → {args.table}")

    if not args.mysql_only:
        Console.config("Kafka", args.bootstrap_servers.split(",")[0])
        Console.config("Topic", data_topic)
    else:
        Console.config(
            "Mode", f"{Colors.YELLOW}MySQL only (跳过 Kafka 验证){Colors.RESET}"
        )

    if not args.skip_mysql:
        setup_database(
            args.mysql_host,
            args.mysql_port,
            args.mysql_user,
            args.mysql_password,
            args.database,
            args.table,
            total_steps,
        )

        if not args.skip_insert:
            result.inserted = insert_test_data(
                args.mysql_host,
                args.mysql_port,
                args.mysql_user,
                args.mysql_password,
                args.database,
                args.table,
                total_steps,
            )

        if not args.skip_update:
            result.updated = update_test_data(
                args.mysql_host,
                args.mysql_port,
                args.mysql_user,
                args.mysql_password,
                args.database,
                args.table,
                total_steps,
            )

        if not args.skip_delete:
            result.deleted = delete_test_data(
                args.mysql_host,
                args.mysql_port,
                args.mysql_user,
                args.mysql_password,
                args.database,
                args.table,
                total_steps,
            )

        show_current_data(
            args.mysql_host,
            args.mysql_port,
            args.mysql_user,
            args.mysql_password,
            args.database,
            args.table,
            total_steps,
        )

        if not args.mysql_only and args.wait > 0:
            print(
                f"\n      {Colors.GRAY}等待 {args.wait}s 同步...{Colors.RESET}",
                end="",
                flush=True,
            )
            time.sleep(args.wait)
            print(f" {Colors.GREEN}done{Colors.RESET}")

    if not args.mysql_only:
        list_topics(args.bootstrap_servers, args.topic_prefix, 6, total_steps)
        consume_and_verify(
            args.bootstrap_servers,
            data_topic,
            result,
            args.table,
            7,
            total_steps,
            args.max_messages,
            verbose=args.verbose,
        )

        if not args.skip_mysql:
            print_verification_summary(result, 8, total_steps)

    Console.footer(start_time)


if __name__ == "__main__":
    main()
