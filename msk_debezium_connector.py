#!/usr/bin/env python3
"""
MSK Debezium MySQL Connector Management Tool

This tool creates and manages AWS MSK Connect Debezium MySQL connectors.
Supports both authenticated (SecretsManager + IAM) and unauthenticated modes.
"""

import argparse
import base64
import json
import logging
import os
import random
import sys
import tempfile
import time
from typing import Optional

import boto3
import requests
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

DEFAULT_PLUGIN_URL = "https://repo1.maven.org/maven2/io/debezium/debezium-connector-mysql/3.4.0.Final/debezium-connector-mysql-3.4.0.Final-plugin.zip"
DEFAULT_KAFKA_CONNECT_VERSION = "3.7.x"
DEFAULT_MCU_COUNT = 1
DEFAULT_WORKER_COUNT = 1
DEFAULT_DATABASE_PORT = 3306


class MSKDebeziumConnectorManager:
    """Manages MSK Debezium MySQL Connectors."""

    def __init__(self, region: str):
        self.region = region
        self.kafka_client = boto3.client("kafka", region_name=region)
        self.kafkaconnect_client = boto3.client("kafkaconnect", region_name=region)
        self.iam_client = boto3.client("iam", region_name=region)
        self.s3_client = boto3.client("s3", region_name=region)
        self.sts_client = boto3.client("sts", region_name=region)
        self.logs_client = boto3.client("logs", region_name=region)
        self.account_id = self.sts_client.get_caller_identity()["Account"]

    def get_msk_cluster_info(self, cluster_name: str) -> dict:
        """Get MSK cluster VPC, subnets, security groups, and bootstrap servers by cluster name."""
        logger.info(f"Getting MSK cluster info for: {cluster_name}")

        paginator = self.kafka_client.get_paginator("list_clusters_v2")
        cluster_info = None

        for page in paginator.paginate(ClusterNameFilter=cluster_name):
            for cluster in page.get("ClusterInfoList", []):
                if cluster.get("ClusterName") == cluster_name:
                    cluster_info = cluster
                    break
            if cluster_info:
                break

        if not cluster_info:
            raise ValueError(f"MSK cluster '{cluster_name}' not found")

        cluster_arn = cluster_info["ClusterArn"]
        logger.info(f"Found cluster ARN: {cluster_arn}")

        if cluster_info.get("ClusterType") == "PROVISIONED":
            provisioned = cluster_info.get("Provisioned", {})
            broker_info = provisioned.get("BrokerNodeGroupInfo", {})
        else:
            broker_info = cluster_info.get("Serverless", {}).get("VpcConfigs", [{}])[0]

        subnets = broker_info.get("ClientSubnets", [])
        security_groups = broker_info.get("SecurityGroups", [])

        bootstrap_response = self.kafka_client.get_bootstrap_brokers(
            ClusterArn=cluster_arn
        )

        bootstrap_servers_plain = bootstrap_response.get("BootstrapBrokerString", "")
        bootstrap_servers_tls = bootstrap_response.get("BootstrapBrokerStringTls", "")
        bootstrap_servers_sasl_iam = bootstrap_response.get(
            "BootstrapBrokerStringSaslIam", ""
        )

        result = {
            "cluster_arn": cluster_arn,
            "cluster_name": cluster_name,
            "subnets": subnets,
            "security_groups": security_groups,
            "bootstrap_servers_plain": bootstrap_servers_plain,
            "bootstrap_servers_tls": bootstrap_servers_tls,
            "bootstrap_servers_sasl_iam": bootstrap_servers_sasl_iam,
        }

        logger.info(
            f"Cluster info retrieved: subnets={subnets}, security_groups={security_groups}"
        )
        return result

    def create_iam_role(
        self,
        role_name: str,
        msk_cluster_arn: str,
        s3_bucket: str,
        use_iam_auth: bool = False,
        secrets_arn: Optional[str] = None,
    ) -> str:
        """Create IAM role for MSK Connect with required policies for S3, CloudWatch, MSK, and SecretsManager."""
        logger.info(f"Creating IAM role: {role_name}")

        try:
            response = self.iam_client.get_role(RoleName=role_name)
            logger.info(f"IAM role {role_name} already exists")
            return response["Role"]["Arn"]
        except ClientError as e:
            if e.response["Error"]["Code"] != "NoSuchEntity":
                raise

        trust_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "kafkaconnect.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                    "Condition": {
                        "StringEquals": {"aws:SourceAccount": self.account_id}
                    },
                }
            ],
        }

        create_response = self.iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="IAM role for MSK Connect Debezium MySQL Connector",
            Tags=[{"Key": "CreatedBy", "Value": "msk-debezium-connector-tool"}],
        )
        role_arn = create_response["Role"]["Arn"]
        logger.info(f"Created IAM role: {role_arn}")

        s3_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": ["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
                    "Resource": [
                        f"arn:aws:s3:::{s3_bucket}",
                        f"arn:aws:s3:::{s3_bucket}/*",
                    ],
                }
            ],
        }

        self.iam_client.put_role_policy(
            RoleName=role_name,
            PolicyName="S3Access",
            PolicyDocument=json.dumps(s3_policy),
        )
        logger.info("Added S3 access policy")

        logs_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "logs:CreateLogGroup",
                        "logs:CreateLogStream",
                        "logs:PutLogEvents",
                        "logs:DescribeLogGroups",
                        "logs:DescribeLogStreams",
                    ],
                    "Resource": [
                        f"arn:aws:logs:{self.region}:{self.account_id}:log-group:/aws/msk-connect/*",
                        f"arn:aws:logs:{self.region}:{self.account_id}:log-group:/aws/msk-connect/*:*",
                    ],
                }
            ],
        }

        self.iam_client.put_role_policy(
            RoleName=role_name,
            PolicyName="CloudWatchLogs",
            PolicyDocument=json.dumps(logs_policy),
        )
        logger.info("Added CloudWatch Logs policy")

        if use_iam_auth:
            kafka_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": [
                            "kafka-cluster:Connect",
                            "kafka-cluster:DescribeCluster",
                        ],
                        "Resource": [msk_cluster_arn],
                    },
                    {
                        "Effect": "Allow",
                        "Action": [
                            "kafka-cluster:ReadData",
                            "kafka-cluster:WriteData",
                            "kafka-cluster:CreateTopic",
                            "kafka-cluster:DescribeTopic",
                        ],
                        "Resource": [
                            f"arn:aws:kafka:{self.region}:{self.account_id}:topic/*/*/*"
                        ],
                    },
                    {
                        "Effect": "Allow",
                        "Action": [
                            "kafka-cluster:AlterGroup",
                            "kafka-cluster:DescribeGroup",
                        ],
                        "Resource": [
                            f"arn:aws:kafka:{self.region}:{self.account_id}:group/*/*/*"
                        ],
                    },
                ],
            }

            self.iam_client.put_role_policy(
                RoleName=role_name,
                PolicyName="KafkaClusterAccess",
                PolicyDocument=json.dumps(kafka_policy),
            )
            logger.info("Added Kafka cluster IAM access policy")

        if secrets_arn:
            secrets_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": [
                            "secretsmanager:GetSecretValue",
                            "secretsmanager:DescribeSecret",
                        ],
                        "Resource": [secrets_arn],
                    }
                ],
            }

            self.iam_client.put_role_policy(
                RoleName=role_name,
                PolicyName="SecretsManagerAccess",
                PolicyDocument=json.dumps(secrets_policy),
            )
            logger.info("Added SecretsManager access policy")

        logger.info("Waiting for IAM role to propagate...")
        time.sleep(10)

        return role_arn

    def delete_iam_role(self, role_name: str) -> bool:
        """Delete IAM role and all its inline/attached policies."""
        logger.info(f"Deleting IAM role: {role_name}")

        try:
            policies = self.iam_client.list_role_policies(RoleName=role_name)
            for policy_name in policies.get("PolicyNames", []):
                self.iam_client.delete_role_policy(
                    RoleName=role_name, PolicyName=policy_name
                )
                logger.info(f"Deleted inline policy: {policy_name}")

            attached = self.iam_client.list_attached_role_policies(RoleName=role_name)
            for policy in attached.get("AttachedPolicies", []):
                self.iam_client.detach_role_policy(
                    RoleName=role_name, PolicyArn=policy["PolicyArn"]
                )
                logger.info(f"Detached managed policy: {policy['PolicyArn']}")

            self.iam_client.delete_role(RoleName=role_name)
            logger.info(f"Deleted IAM role: {role_name}")
            return True

        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchEntity":
                logger.info(f"IAM role {role_name} does not exist")
                return True
            raise

    def download_and_upload_plugin(
        self, plugin_url: str, s3_bucket: str, s3_key: str
    ) -> str:
        """Download Debezium plugin from URL and upload to S3 if not already present."""
        try:
            self.s3_client.head_object(Bucket=s3_bucket, Key=s3_key)
            logger.info(f"Plugin already exists in S3: s3://{s3_bucket}/{s3_key}")
            return f"s3://{s3_bucket}/{s3_key}"
        except ClientError as e:
            if e.response["Error"]["Code"] != "404":
                raise

        logger.info(f"Downloading plugin from: {plugin_url}")

        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp_file:
            response = requests.get(plugin_url, stream=True)
            response.raise_for_status()

            for chunk in response.iter_content(chunk_size=8192):
                tmp_file.write(chunk)
            tmp_file_path = tmp_file.name

        try:
            logger.info(f"Uploading plugin to s3://{s3_bucket}/{s3_key}")
            self.s3_client.upload_file(tmp_file_path, s3_bucket, s3_key)
            logger.info("Plugin uploaded successfully")
        finally:
            os.unlink(tmp_file_path)

        return f"s3://{s3_bucket}/{s3_key}"

    def create_custom_plugin(
        self, plugin_name: str, s3_bucket: str, s3_key: str
    ) -> tuple:
        """Create MSK Connect custom plugin from S3 location. Returns (plugin_arn, revision)."""
        logger.info(f"Creating custom plugin: {plugin_name}")

        try:
            paginator = self.kafkaconnect_client.get_paginator("list_custom_plugins")
            for page in paginator.paginate():
                for plugin in page.get("customPlugins", []):
                    if plugin.get("name") == plugin_name:
                        plugin_arn = plugin["customPluginArn"]
                        revision = plugin["latestRevision"]["revision"]
                        logger.info(
                            f"Custom plugin {plugin_name} already exists: {plugin_arn}"
                        )
                        return (plugin_arn, revision)
        except ClientError:
            pass

        response = self.kafkaconnect_client.create_custom_plugin(
            name=plugin_name,
            contentType="ZIP",
            location={
                "s3Location": {
                    "bucketArn": f"arn:aws:s3:::{s3_bucket}",
                    "fileKey": s3_key,
                }
            },
            description="Debezium MySQL Connector Plugin",
        )

        plugin_arn = response["customPluginArn"]
        revision = response["revision"]

        logger.info("Waiting for custom plugin to become ACTIVE...")
        while True:
            describe_response = self.kafkaconnect_client.describe_custom_plugin(
                customPluginArn=plugin_arn
            )
            state = describe_response.get("customPluginState")
            if state == "ACTIVE":
                logger.info("Custom plugin is ACTIVE")
                break
            elif state in ["CREATE_FAILED", "DELETING"]:
                raise RuntimeError(f"Custom plugin creation failed with state: {state}")
            logger.info(f"Plugin state: {state}, waiting...")
            time.sleep(10)

        return (plugin_arn, revision)

    def delete_custom_plugin(self, plugin_name: str) -> bool:
        """Delete custom plugin by name."""
        logger.info(f"Deleting custom plugin: {plugin_name}")

        plugin_arn = None
        try:
            paginator = self.kafkaconnect_client.get_paginator("list_custom_plugins")
            for page in paginator.paginate():
                for plugin in page.get("customPlugins", []):
                    if plugin.get("name") == plugin_name:
                        plugin_arn = plugin["customPluginArn"]
                        break
                if plugin_arn:
                    break
        except ClientError:
            pass

        if not plugin_arn:
            logger.info(f"Custom plugin {plugin_name} not found")
            return True

        self.kafkaconnect_client.delete_custom_plugin(customPluginArn=plugin_arn)
        logger.info(f"Deleted custom plugin: {plugin_arn}")

        logger.info("Waiting for plugin deletion...")
        while True:
            try:
                describe_response = self.kafkaconnect_client.describe_custom_plugin(
                    customPluginArn=plugin_arn
                )
                state = describe_response.get("customPluginState")
                if state == "DELETING":
                    logger.info("Plugin still deleting...")
                    time.sleep(5)
                else:
                    break
            except ClientError as e:
                if e.response["Error"]["Code"] == "NotFoundException":
                    logger.info("Plugin deleted")
                    break
                raise

        return True

    def create_worker_configuration(
        self,
        config_name: str,
        use_secrets_manager: bool = False,
        aws_region: Optional[str] = None,
        offset_topic: Optional[str] = None,
    ) -> tuple:
        """Create worker configuration. Returns (worker_config_arn, revision)."""
        logger.info(f"Creating worker configuration: {config_name}")

        try:
            paginator = self.kafkaconnect_client.get_paginator(
                "list_worker_configurations"
            )
            for page in paginator.paginate():
                for config in page.get("workerConfigurations", []):
                    if config.get("name") == config_name:
                        config_arn = config["workerConfigurationArn"]
                        revision = config["latestRevision"]["revision"]
                        logger.info(
                            f"Worker configuration {config_name} already exists: {config_arn}"
                        )
                        return (config_arn, revision)
        except ClientError:
            pass

        if use_secrets_manager:
            if not aws_region:
                aws_region = self.region
            if not offset_topic:
                offset_topic = f"__connect-offsets-{config_name}"

            properties = f"""key.converter.schemas.enable=false
value.converter.schemas.enable=false
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
config.providers.secretManager.class=com.github.jcustenborder.kafka.config.aws.SecretsManagerConfigProvider
config.providers=secretManager
config.providers.secretManager.param.aws.region={aws_region}
offset.storage.topic={offset_topic}"""
        else:
            if not offset_topic:
                offset_topic = f"__connect-offsets-{config_name}"

            properties = f"""key.converter.schemas.enable=false
value.converter.schemas.enable=false
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
offset.storage.topic={offset_topic}"""

        response = self.kafkaconnect_client.create_worker_configuration(
            name=config_name,
            propertiesFileContent=base64.b64encode(properties.encode()).decode(),
            description="Worker configuration for Debezium MySQL Connector",
        )

        config_arn = response["workerConfigurationArn"]
        revision = response["latestRevision"]["revision"]

        logger.info(f"Created worker configuration: {config_arn}")
        return (config_arn, revision)

    def delete_worker_configuration(self, config_name: str) -> bool:
        """Delete worker configuration by name."""
        logger.info(f"Deleting worker configuration: {config_name}")

        config_arn = None
        try:
            paginator = self.kafkaconnect_client.get_paginator(
                "list_worker_configurations"
            )
            for page in paginator.paginate():
                for config in page.get("workerConfigurations", []):
                    if config.get("name") == config_name:
                        config_arn = config["workerConfigurationArn"]
                        break
                if config_arn:
                    break
        except ClientError:
            pass

        if not config_arn:
            logger.info(f"Worker configuration {config_name} not found")
            return True

        self.kafkaconnect_client.delete_worker_configuration(
            workerConfigurationArn=config_arn
        )
        logger.info(f"Deleted worker configuration: {config_arn}")
        return True

    def build_connector_config_no_auth(
        self,
        mysql_host: str,
        mysql_port: int,
        database_list: str,
        topic_prefix: str,
        bootstrap_servers: str,
        mysql_user: Optional[str] = None,
        mysql_password: Optional[str] = None,
        secret_name: Optional[str] = None,
        server_id: Optional[int] = None,
        schema_history_topic: Optional[str] = None,
    ) -> dict:
        """Build connector configuration for non-authenticated MSK (plaintext or SecretsManager password)."""
        if server_id is None:
            server_id = random.randint(100000, 999999)
        if schema_history_topic is None:
            schema_history_topic = f"{topic_prefix}_schema_history"

        if secret_name:
            db_user = f"${{secretManager:{secret_name}:dbusername}}"
            db_password = f"${{secretManager:{secret_name}:dbpassword}}"
        else:
            db_user = mysql_user
            db_password = mysql_password

        return {
            "connector.class": "io.debezium.connector.mysql.MySqlConnector",
            "tasks.max": "1",
            "database.hostname": mysql_host,
            "database.port": str(mysql_port),
            "database.server.id": str(server_id),
            "database.include.list": database_list,
            "database.user": db_user,
            "database.password": db_password,
            "topic.prefix": topic_prefix,
            "decimal.handling.mode": "string",
            "binary.handling.mode": "bytes",
            "bigint.unsigned.handling.mode": "long",
            "time.precision.mode": "adaptive_time_microseconds",
            "snapshot.mode": "initial",
            "snapshot.locking.mode": "minimal",
            "incremental.snapshot.enabled": "true",
            "incremental.snapshot.chunk.size": "1024",
            "schema.history.internal": "io.debezium.storage.kafka.history.KafkaSchemaHistory",
            "schema.history.internal.kafka.topic": schema_history_topic,
            "schema.history.internal.kafka.bootstrap.servers": bootstrap_servers,
            "max.queue.size": "8192",
            "max.batch.size": "2048",
            "poll.interval.ms": "1000",
            "offset.flush.timeout.ms": "120000",
            "heartbeat.interval.ms": "30000",
            "heartbeat.topics.prefix": "__debezium-heartbeat",
            "errors.max.retries": "3",
            "errors.retry.delay.initial.ms": "300",
            "errors.retry.delay.max.ms": "10000",
            "include.schema.changes": "true",
            "include.query": "false",
            "tombstones.on.delete": "true",
            "transforms": "Reroute",
            "transforms.Reroute.type": "io.debezium.transforms.ByLogicalTableRouter",
            "transforms.Reroute.topic.regex": "(.*)\\.(.*)\\.(.*)",
            "transforms.Reroute.topic.replacement": "$1_all_data",
        }

    def build_connector_config_with_auth(
        self,
        mysql_host: str,
        mysql_port: int,
        secret_name: str,
        database_list: str,
        topic_prefix: str,
        bootstrap_servers: str,
        server_id: Optional[int] = None,
        schema_history_topic: Optional[str] = None,
    ) -> dict:
        """Build connector configuration for IAM-authenticated MSK with SecretsManager credentials."""
        if server_id is None:
            server_id = random.randint(100000, 999999)
        if schema_history_topic is None:
            schema_history_topic = f"{topic_prefix}_schema_history"

        return {
            "connector.class": "io.debezium.connector.mysql.MySqlConnector",
            "tasks.max": "1",
            "database.hostname": mysql_host,
            "database.port": str(mysql_port),
            "database.server.id": str(server_id),
            "database.include.list": database_list,
            "database.user": f"${{secretManager:{secret_name}:dbusername}}",
            "database.password": f"${{secretManager:{secret_name}:dbpassword}}",
            "topic.prefix": topic_prefix,
            "decimal.handling.mode": "string",
            "binary.handling.mode": "bytes",
            "bigint.unsigned.handling.mode": "long",
            "time.precision.mode": "adaptive_time_microseconds",
            "snapshot.mode": "initial",
            "snapshot.locking.mode": "minimal",
            "incremental.snapshot.enabled": "true",
            "incremental.snapshot.chunk.size": "1024",
            "schema.history.internal": "io.debezium.storage.kafka.history.KafkaSchemaHistory",
            "schema.history.internal.kafka.topic": schema_history_topic,
            "schema.history.internal.kafka.bootstrap.servers": bootstrap_servers,
            "schema.history.internal.producer.security.protocol": "SASL_SSL",
            "schema.history.internal.producer.sasl.mechanism": "AWS_MSK_IAM",
            "schema.history.internal.producer.sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
            "schema.history.internal.producer.sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
            "schema.history.internal.consumer.security.protocol": "SASL_SSL",
            "schema.history.internal.consumer.sasl.mechanism": "AWS_MSK_IAM",
            "schema.history.internal.consumer.sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
            "schema.history.internal.consumer.sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
            "max.queue.size": "8192",
            "max.batch.size": "2048",
            "poll.interval.ms": "1000",
            "offset.flush.timeout.ms": "120000",
            "heartbeat.interval.ms": "30000",
            "heartbeat.topics.prefix": "__debezium-heartbeat",
            "errors.max.retries": "3",
            "errors.retry.delay.initial.ms": "300",
            "errors.retry.delay.max.ms": "10000",
            "include.schema.changes": "true",
            "include.query": "false",
            "tombstones.on.delete": "true",
            "transforms": "Reroute",
            "transforms.Reroute.type": "io.debezium.transforms.ByLogicalTableRouter",
            "transforms.Reroute.topic.regex": "(.*)\\.(.*)\\.(.*)",
            "transforms.Reroute.topic.replacement": "$1_all_data",
        }

    def create_log_group(self, log_group_name: str) -> str:
        """Create CloudWatch log group if it doesn't exist."""
        try:
            self.logs_client.create_log_group(logGroupName=log_group_name)
            logger.info(f"Created log group: {log_group_name}")
        except ClientError as e:
            if e.response["Error"]["Code"] != "ResourceAlreadyExistsException":
                raise
            logger.info(f"Log group {log_group_name} already exists")

        return log_group_name

    def create_connector(
        self,
        connector_name: str,
        plugin_arn: str,
        plugin_revision: int,
        worker_config_arn: str,
        worker_config_revision: int,
        service_role_arn: str,
        connector_config: dict,
        bootstrap_servers: str,
        subnets: list,
        security_groups: list,
        use_iam_auth: bool = False,
        mcu_count: int = DEFAULT_MCU_COUNT,
        worker_count: int = DEFAULT_WORKER_COUNT,
        kafka_connect_version: str = DEFAULT_KAFKA_CONNECT_VERSION,
        log_group: Optional[str] = None,
    ) -> str:
        """Create MSK Connect connector and wait for RUNNING state."""
        logger.info(f"Creating connector: {connector_name}")

        try:
            paginator = self.kafkaconnect_client.get_paginator("list_connectors")
            for page in paginator.paginate():
                for connector in page.get("connectors", []):
                    if connector.get("connectorName") == connector_name:
                        connector_arn = connector["connectorArn"]
                        logger.info(
                            f"Connector {connector_name} already exists: {connector_arn}"
                        )
                        return connector_arn
        except ClientError:
            pass

        log_delivery = None
        if log_group:
            self.create_log_group(log_group)
            log_delivery = {
                "workerLogDelivery": {
                    "cloudWatchLogs": {"enabled": True, "logGroup": log_group},
                    "firehose": {"enabled": False},
                    "s3": {"enabled": False},
                }
            }

        create_params = {
            "connectorName": connector_name,
            "connectorConfiguration": connector_config,
            "capacity": {
                "provisionedCapacity": {
                    "mcuCount": mcu_count,
                    "workerCount": worker_count,
                }
            },
            "kafkaCluster": {
                "apacheKafkaCluster": {
                    "bootstrapServers": bootstrap_servers,
                    "vpc": {"subnets": subnets, "securityGroups": security_groups},
                }
            },
            "kafkaClusterClientAuthentication": {
                "authenticationType": "IAM" if use_iam_auth else "NONE"
            },
            "kafkaClusterEncryptionInTransit": {
                "encryptionType": "TLS" if use_iam_auth else "PLAINTEXT"
            },
            "kafkaConnectVersion": kafka_connect_version,
            "plugins": [
                {
                    "customPlugin": {
                        "customPluginArn": plugin_arn,
                        "revision": plugin_revision,
                    }
                }
            ],
            "serviceExecutionRoleArn": service_role_arn,
            "workerConfiguration": {
                "workerConfigurationArn": worker_config_arn,
                "revision": worker_config_revision,
            },
        }

        if log_delivery:
            create_params["logDelivery"] = log_delivery

        response = self.kafkaconnect_client.create_connector(**create_params)
        connector_arn = response["connectorArn"]

        logger.info(f"Created connector: {connector_arn}")
        logger.info("Waiting for connector to become RUNNING...")

        while True:
            describe_response = self.kafkaconnect_client.describe_connector(
                connectorArn=connector_arn
            )
            state = describe_response.get("connectorState")
            if state == "RUNNING":
                logger.info("Connector is RUNNING")
                break
            elif state in ["FAILED", "DELETING"]:
                error_msg = describe_response.get("stateDescription", {}).get(
                    "message", "Unknown error"
                )
                raise RuntimeError(f"Connector creation failed: {state} - {error_msg}")
            logger.info(f"Connector state: {state}, waiting...")
            time.sleep(30)

        return connector_arn

    def delete_connector(self, connector_name: str) -> bool:
        """Delete connector by name and wait for deletion to complete."""
        logger.info(f"Deleting connector: {connector_name}")

        connector_arn = None
        try:
            paginator = self.kafkaconnect_client.get_paginator("list_connectors")
            for page in paginator.paginate():
                for connector in page.get("connectors", []):
                    if connector.get("connectorName") == connector_name:
                        connector_arn = connector["connectorArn"]
                        break
                if connector_arn:
                    break
        except ClientError:
            pass

        if not connector_arn:
            logger.info(f"Connector {connector_name} not found")
            return True

        self.kafkaconnect_client.delete_connector(connectorArn=connector_arn)
        logger.info(f"Deletion initiated for connector: {connector_arn}")

        logger.info("Waiting for connector deletion...")
        while True:
            try:
                describe_response = self.kafkaconnect_client.describe_connector(
                    connectorArn=connector_arn
                )
                state = describe_response.get("connectorState")
                if state == "DELETING":
                    logger.info("Connector still deleting...")
                    time.sleep(30)
                else:
                    break
            except ClientError as e:
                if e.response["Error"]["Code"] == "NotFoundException":
                    logger.info("Connector deleted")
                    break
                raise

        return True


def create_command(args):
    """Execute create command."""
    manager = MSKDebeziumConnectorManager(args.region)

    cluster_info = manager.get_msk_cluster_info(args.msk_cluster_name)

    subnets = args.subnets.split(",") if args.subnets else cluster_info["subnets"]
    security_groups = (
        args.security_groups.split(",")
        if args.security_groups
        else cluster_info["security_groups"]
    )

    if args.use_iam_auth:
        bootstrap_servers = cluster_info["bootstrap_servers_sasl_iam"]
        if not bootstrap_servers:
            raise ValueError("MSK cluster does not support IAM authentication")
    else:
        bootstrap_servers = (
            cluster_info["bootstrap_servers_plain"]
            or cluster_info["bootstrap_servers_tls"]
        )
        if not bootstrap_servers:
            raise ValueError("Could not determine bootstrap servers")

    logger.info(f"Using bootstrap servers: {bootstrap_servers}")
    logger.info(f"Using subnets: {subnets}")
    logger.info(f"Using security groups: {security_groups}")

    base_name = args.connector_name
    role_name = f"MSKConnectRole-{base_name}"
    plugin_name = f"debezium-mysql-plugin-{base_name}"
    worker_config_name = f"debezium-worker-config-{base_name}"
    log_group_name = f"/aws/msk-connect/{base_name}"
    s3_key = f"msk-connect/plugins/debezium-mysql/{args.plugin_url.split('/')[-1]}"

    secrets_arn = None
    if args.secrets_name:
        secrets_arn = f"arn:aws:secretsmanager:{args.region}:{manager.account_id}:secret:{args.secrets_name}"

    role_arn = manager.create_iam_role(
        role_name=role_name,
        msk_cluster_arn=cluster_info["cluster_arn"],
        s3_bucket=args.s3_bucket,
        use_iam_auth=args.use_iam_auth,
        secrets_arn=secrets_arn,
    )

    manager.download_and_upload_plugin(
        plugin_url=args.plugin_url, s3_bucket=args.s3_bucket, s3_key=s3_key
    )

    plugin_arn, plugin_revision = manager.create_custom_plugin(
        plugin_name=plugin_name, s3_bucket=args.s3_bucket, s3_key=s3_key
    )

    worker_config_arn, worker_config_revision = manager.create_worker_configuration(
        config_name=worker_config_name,
        use_secrets_manager=args.use_secrets_manager,
        aws_region=args.region,
        offset_topic=f"__connect-offsets-{base_name}",
    )

    if args.use_iam_auth:
        if not args.use_secrets_manager:
            raise ValueError("IAM authentication requires --use-secrets-manager")
        connector_config = manager.build_connector_config_with_auth(
            mysql_host=args.mysql_host,
            mysql_port=args.mysql_port,
            secret_name=args.secrets_name,
            database_list=args.database_list,
            topic_prefix=args.topic_prefix,
            bootstrap_servers=bootstrap_servers,
            server_id=args.server_id,
            schema_history_topic=args.schema_history_topic,
        )
    elif args.use_secrets_manager:
        connector_config = manager.build_connector_config_no_auth(
            mysql_host=args.mysql_host,
            mysql_port=args.mysql_port,
            database_list=args.database_list,
            topic_prefix=args.topic_prefix,
            bootstrap_servers=bootstrap_servers,
            secret_name=args.secrets_name,
            server_id=args.server_id,
            schema_history_topic=args.schema_history_topic,
        )
    else:
        if not args.mysql_user or not args.mysql_password:
            raise ValueError(
                "MySQL username and password are required when not using SecretsManager"
            )
        connector_config = manager.build_connector_config_no_auth(
            mysql_host=args.mysql_host,
            mysql_port=args.mysql_port,
            database_list=args.database_list,
            topic_prefix=args.topic_prefix,
            bootstrap_servers=bootstrap_servers,
            mysql_user=args.mysql_user,
            mysql_password=args.mysql_password,
            server_id=args.server_id,
            schema_history_topic=args.schema_history_topic,
        )

    connector_arn = manager.create_connector(
        connector_name=args.connector_name,
        plugin_arn=plugin_arn,
        plugin_revision=plugin_revision,
        worker_config_arn=worker_config_arn,
        worker_config_revision=worker_config_revision,
        service_role_arn=role_arn,
        connector_config=connector_config,
        bootstrap_servers=bootstrap_servers,
        subnets=subnets,
        security_groups=security_groups,
        use_iam_auth=args.use_iam_auth,
        mcu_count=args.mcu_count,
        worker_count=args.worker_count,
        kafka_connect_version=args.kafka_connect_version,
        log_group=log_group_name if args.enable_logging else None,
    )

    logger.info("=" * 50)
    logger.info("MSK Debezium MySQL Connector created successfully!")
    logger.info(f"Connector ARN: {connector_arn}")
    logger.info(f"IAM Role: {role_arn}")
    logger.info(f"Plugin: {plugin_arn}")
    logger.info(f"Worker Config: {worker_config_arn}")
    logger.info("=" * 50)


def delete_command(args):
    """Execute delete command."""
    manager = MSKDebeziumConnectorManager(args.region)

    base_name = args.connector_name
    role_name = f"MSKConnectRole-{base_name}"
    plugin_name = f"debezium-mysql-plugin-{base_name}"
    worker_config_name = f"debezium-worker-config-{base_name}"

    logger.info("Starting deletion process...")

    manager.delete_connector(args.connector_name)

    if args.delete_worker_config:
        manager.delete_worker_configuration(worker_config_name)

    if args.delete_plugin:
        manager.delete_custom_plugin(plugin_name)

    if args.delete_role:
        manager.delete_iam_role(role_name)

    logger.info("=" * 50)
    logger.info("Deletion completed!")
    logger.info("=" * 50)


def main():
    parser = argparse.ArgumentParser(
        description="MSK Debezium MySQL Connector Management Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Create connector without authentication (plaintext mode)
  python msk_debezium_connector.py create \\
    --connector-name my-connector \\
    --msk-cluster-name msk-log-stream \\
    --s3-bucket clickstream-data-01 \\
    --mysql-host common-test.cpwuo9y53vjh.us-east-1.rds.amazonaws.com \\
    --mysql-user admin \\
    --mysql-password 'Ssa123456#$' \\
    --database-list mydb \\
    --topic-prefix myprefix \\
    --region us-east-1

  # Create connector with IAM authentication and SecretsManager
  python msk_debezium_connector.py create \\
    --connector-name my-connector \\
    --msk-cluster-name msk-log-stream \\
    --s3-bucket clickstream-data-01 \\
    --mysql-host common-test.cpwuo9y53vjh.us-east-1.rds.amazonaws.com \\
    --secrets-name my-db-secret \\
    --database-list mydb \\
    --topic-prefix myprefix \\
    --region us-east-1 \\
    --use-iam-auth \\
    --use-secrets-manager

  # Delete connector and all related resources
  python msk_debezium_connector.py delete \\
    --connector-name my-connector \\
    --region us-east-1 \\
    --delete-all
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Commands")

    create_parser = subparsers.add_parser(
        "create", help="Create MSK Debezium MySQL connector"
    )

    create_parser.add_argument(
        "--connector-name", required=True, help="Name of the connector"
    )
    create_parser.add_argument(
        "--msk-cluster-name",
        required=True,
        help="Name of the MSK cluster to get VPC/subnet/security group from",
    )
    create_parser.add_argument(
        "--s3-bucket", required=True, help="S3 bucket for plugin storage and logs"
    )
    create_parser.add_argument(
        "--mysql-host", required=True, help="MySQL server hostname"
    )
    create_parser.add_argument(
        "--database-list",
        required=True,
        help="Comma-separated list of databases to capture (e.g., db1,db2)",
    )
    create_parser.add_argument(
        "--topic-prefix", required=True, help="Prefix for Kafka topics"
    )
    create_parser.add_argument("--region", required=True, help="AWS region")

    create_parser.add_argument(
        "--mysql-user",
        help="MySQL username (required if not using --use-secrets-manager)",
    )
    create_parser.add_argument(
        "--mysql-password",
        help="MySQL password (required if not using --use-secrets-manager)",
    )

    create_parser.add_argument(
        "--secrets-name",
        help="SecretsManager secret name containing dbusername and dbpassword",
    )

    create_parser.add_argument(
        "--use-iam-auth",
        action="store_true",
        help="Use IAM authentication for MSK (requires MSK cluster with IAM auth enabled)",
    )
    create_parser.add_argument(
        "--use-secrets-manager",
        action="store_true",
        help="Use SecretsManager for MySQL credentials in worker config",
    )

    create_parser.add_argument(
        "--subnets", help="Comma-separated subnet IDs (defaults to MSK cluster subnets)"
    )
    create_parser.add_argument(
        "--security-groups",
        help="Comma-separated security group IDs (defaults to MSK cluster security groups)",
    )

    create_parser.add_argument(
        "--mysql-port",
        type=int,
        default=DEFAULT_DATABASE_PORT,
        help=f"MySQL server port (default: {DEFAULT_DATABASE_PORT})",
    )
    create_parser.add_argument(
        "--server-id",
        type=int,
        help="Unique MySQL server ID for replication (default: random)",
    )
    create_parser.add_argument(
        "--schema-history-topic",
        help="Kafka topic for schema history (default: <topic-prefix>_schema_history)",
    )
    create_parser.add_argument(
        "--plugin-url",
        default=DEFAULT_PLUGIN_URL,
        help=f"URL to download Debezium plugin (default: {DEFAULT_PLUGIN_URL})",
    )
    create_parser.add_argument(
        "--kafka-connect-version",
        default=DEFAULT_KAFKA_CONNECT_VERSION,
        help=f"Kafka Connect version (default: {DEFAULT_KAFKA_CONNECT_VERSION})",
    )
    create_parser.add_argument(
        "--mcu-count",
        type=int,
        default=DEFAULT_MCU_COUNT,
        help=f"MCU count per worker (default: {DEFAULT_MCU_COUNT})",
    )
    create_parser.add_argument(
        "--worker-count",
        type=int,
        default=DEFAULT_WORKER_COUNT,
        help=f"Number of workers (default: {DEFAULT_WORKER_COUNT})",
    )
    create_parser.add_argument(
        "--enable-logging",
        action="store_true",
        default=True,
        help="Enable CloudWatch logging (default: True)",
    )
    create_parser.add_argument(
        "--no-logging",
        action="store_false",
        dest="enable_logging",
        help="Disable CloudWatch logging",
    )

    create_parser.set_defaults(func=create_command)

    delete_parser = subparsers.add_parser(
        "delete", help="Delete MSK Debezium MySQL connector"
    )

    delete_parser.add_argument(
        "--connector-name", required=True, help="Name of the connector to delete"
    )
    delete_parser.add_argument("--region", required=True, help="AWS region")
    delete_parser.add_argument(
        "--delete-all",
        action="store_true",
        help="Delete connector, plugin, worker config, and IAM role",
    )
    delete_parser.add_argument(
        "--delete-plugin", action="store_true", help="Also delete the custom plugin"
    )
    delete_parser.add_argument(
        "--delete-worker-config",
        action="store_true",
        help="Also delete the worker configuration",
    )
    delete_parser.add_argument(
        "--delete-role", action="store_true", help="Also delete the IAM role"
    )

    delete_parser.set_defaults(func=delete_command)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == "delete" and args.delete_all:
        args.delete_plugin = True
        args.delete_worker_config = True
        args.delete_role = True

    if args.command == "create":
        if args.use_secrets_manager:
            if not args.secrets_name:
                parser.error(
                    "--secrets-name is required when using --use-secrets-manager"
                )
        else:
            if not args.mysql_user or not args.mysql_password:
                parser.error(
                    "--mysql-user and --mysql-password are required when not using --use-secrets-manager"
                )

    try:
        args.func(args)
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
