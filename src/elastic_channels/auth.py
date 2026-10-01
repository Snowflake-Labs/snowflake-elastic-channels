# Copyright 2026 Snowflake Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Snowflake key-pair authentication helpers for the ingest SDK."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization


def load_private_key_pem() -> str:
    """Return PKCS#8 PEM private key from env or file path."""
    inline = os.environ.get("SNOWFLAKE_PRIVATE_KEY", "").strip()
    if inline:
        return inline.replace("\\n", "\n")

    path = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH", "").strip()
    if not path:
        raise ValueError(
            "Set SNOWFLAKE_PRIVATE_KEY or SNOWFLAKE_PRIVATE_KEY_PATH for key-pair auth"
        )
    return Path(path).expanduser().read_text(encoding="utf-8")


def load_private_key_der_b64() -> str:
    """Return base64 DER private key (Snowflake connector format)."""
    pem = load_private_key_pem()
    key = serialization.load_pem_private_key(
        pem.encode("utf-8"), password=None, backend=default_backend()
    )
    der = key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    import base64

    return base64.b64encode(der).decode("ascii")


def snowflake_account_url(account: str) -> str:
    host = os.environ.get(
        "SNOWFLAKE_HOST", f"{account.replace('_', '-')}.snowflakecomputing.com"
    )
    if not host.startswith("https://"):
        host = f"https://{host}"
    return host


def ingest_client_properties() -> dict[str, Any]:
    """Build StreamingIngestClient properties dict from environment."""
    account = os.environ["SNOWFLAKE_ACCOUNT"]
    props: dict[str, Any] = {
        "account": account,
        "user": os.environ["SNOWFLAKE_USER"],
        "private_key": load_private_key_pem(),
        "url": snowflake_account_url(account),
    }
    role = os.environ.get("SNOWFLAKE_ROLE")
    if role:
        props["role"] = role
    return props


def connector_config() -> dict[str, Any]:
    """Snowflake connector config using key-pair or password auth."""
    cfg: dict[str, Any] = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "role": os.environ.get("SNOWFLAKE_ROLE"),
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE"),
        "database": os.environ.get("SNOWFLAKE_DATABASE"),
        "schema": os.environ.get("SNOWFLAKE_SCHEMA"),
    }
    password = os.environ.get("SNOWFLAKE_PASSWORD")
    if password:
        cfg["password"] = password
    else:
        cfg["private_key"] = load_private_key_der_b64()
    return {k: v for k, v in cfg.items() if v}
