"""DWS boundaries for the opt-in review scheduler.

The scheduler owns dispatch claiming and reconciliation. This adapter never
claims or retries a send and only accepts a frozen, independently verified plan.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping
from zoneinfo import ZoneInfo

import review_auto_process
import review_dispatch_send
from dws_adapter import CifraCardCredentialSource, DwsClient
from review_inbox_poll import read_dws


@dataclass(frozen=True)
class ReviewCardDeliveryConfig:
    enabled: bool = False
    template_id: str | None = None
    template_version: str | None = None
    identity_map_file: Path | None = None
    field_mapping: dict[str, str] | None = None

    @classmethod
    def from_environment(cls, environment: Mapping[str, str]) -> 'ReviewCardDeliveryConfig':
        enabled = environment.get('DWS_CARD_REVIEW_ENABLED') == 'true'
        raw_mapping = environment.get('DINGTODO_REVIEW_CARD_FIELD_MAP')
        field_mapping = None
        if isinstance(raw_mapping, str) and len(raw_mapping) <= 8192:
            try:
                field_mapping = json.loads(raw_mapping, object_pairs_hook=_unique_mapping)
            except (TypeError, ValueError, json.JSONDecodeError):
                field_mapping = None
        identity_map_file = environment.get('DINGTODO_REVIEW_CARD_IDENTITY_MAP_FILE')
        return cls(
            enabled=enabled,
            template_id=environment.get('DINGTODO_REVIEW_CARD_TEMPLATE_ID'),
            template_version=environment.get('DINGTODO_REVIEW_CARD_TEMPLATE_VERSION'),
            identity_map_file=Path(identity_map_file) if identity_map_file else None,
            field_mapping=field_mapping if isinstance(field_mapping, dict) else None,
        )


def _unique_mapping(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate_review_card_field')
        result[key] = value
    return result


class ReviewScheduleDws:
    def __init__(self, conn, *, profile: str, bot_open_id: str,
                 owner_open_id: str, app_id: str,
                 card_config: ReviewCardDeliveryConfig | None = None):
        if not all(isinstance(value, str) and value.strip()
                   for value in (profile, bot_open_id, owner_open_id, app_id)):
            raise ValueError('review_identity_missing')
        self.conn = conn
        self.profile = profile
        self.bot_open_id = bot_open_id
        self.owner_open_id = owner_open_id
        self.app_id = app_id
        self.card_config = card_config or ReviewCardDeliveryConfig()

    def _args(self, batch: str, mode: str):
        return SimpleNamespace(batch=batch, mode=mode, profile=self.profile,
                               bot_open_id=self.bot_open_id, owner_open_id=self.owner_open_id,
                               app_id=self.app_id, confirm_batch=None)

    def _check_identity(self, context):
        owner, robot = review_auto_process._verified_send_context(
            self.profile, self.bot_open_id, self.app_id, context['conversation'],
            self.owner_open_id, self.conn)
        if owner != context['owner_user'] or robot != context['robot_code']:
            raise ValueError('review_send_identity_mismatch')

    def preflight(self, batch: str) -> dict:
        args = self._args(batch, 'preview')
        context = review_dispatch_send.verify_context(self.conn, args)
        self._check_identity(context)
        preview = review_dispatch_send.run(self.conn, args)
        if preview.get('status') != 'preview' or preview.get('batch') != batch:
            raise ValueError('review_send_preview_failed')
        context = review_dispatch_send.verify_context(self.conn, args)
        if context['status'] != 'prepared' or context['preview_hash'] != context['digest']:
            raise ValueError('review_send_preview_mismatch')
        return {'digest': context['digest']}

    def send(self, batch: str, plan: dict, text: str) -> None:
        args = self._args(batch, 'send')
        context = review_dispatch_send.verify_context(self.conn, args)
        if (not isinstance(plan, dict) or context['status'] != 'sending'
                or context['text'] != text or plan.get('digest') != context['digest']
                or context['preview_hash'] != context['digest']):
            raise ValueError('review_send_plan_changed')
        self._check_identity(context)
        review_dispatch_send.verify_sources(context)
        response = review_dispatch_send.dws([*review_dispatch_send.message_args(args, context), '--yes'])
        if response.get('ok') is not True or (response.get('result') or {}).get('success') is not True:
            raise RuntimeError('review_send_result_unknown')

    def history(self, batch: str, sending_at: str) -> dict:
        args = self._args(batch, 'reconcile')
        context = review_dispatch_send.verify_context(self.conn, args)
        if context['status'] not in ('sending', 'unknown') or context['sending_at'] != sending_at:
            raise ValueError('review_delivery_context_changed')
        self._check_identity(context)
        return read_dws(['chat', '+chat-messages', '--open-dingtalk-id', self.bot_open_id,
                         '--start', sending_at, '--page-all', '--max-items', '200',
                         '--no-reactions', '--profile', self.profile])

    def send_card(self, batch: str) -> dict[str, str]:
        """Optionally deliver one supplementary card after a text batch is proven."""
        config = self.card_config
        if not config.enabled:
            return {'status': 'not_attempted', 'reason': 'configuration_missing'}
        if (not isinstance(config.template_id, str) or not config.template_id.strip()
                or not isinstance(config.template_version, str) or not config.template_version.strip()
                or not isinstance(config.identity_map_file, Path)
                or not isinstance(config.field_mapping, dict) or not config.field_mapping):
            return {'status': 'not_attempted', 'reason': 'configuration_missing'}

        row = self.conn.execute(
            'SELECT b.state,d.status FROM batch AS b JOIN dispatch AS d ON d.id=b.id '
            'WHERE b.id=?', (batch,),
        ).fetchone()
        if row != ('open', 'delivered'):
            return {'status': 'not_attempted', 'reason': 'text_anchor_unverified'}

        try:
            from review_card_adapter import (
                build_review_template_fields,
                card_identity_map_key,
                load_card_identity_map,
            )
            from review_inbox import claim_review_card_delivery, finish_review_card_delivery
        except ImportError:
            return {'status': 'not_attempted', 'reason': 'configuration_missing'}

        try:
            args = self._args(batch, 'reconcile')
            context = review_dispatch_send.verify_context(self.conn, args)
            owner = self.conn.execute(
                'SELECT owner_id FROM dispatch WHERE id=?', (batch,),
            ).fetchone()
            if (context['status'] != 'delivered' or not owner
                    or owner[0] != self.owner_open_id):
                return {'status': 'not_attempted', 'reason': 'text_anchor_unverified'}
            self._check_identity(context)
        except Exception:
            return {'status': 'not_attempted', 'reason': 'preflight_failed'}

        try:
            identity_map = load_card_identity_map(config.identity_map_file)
            identity_key = card_identity_map_key(context['owner_user'], 1)
        except (ValueError, OSError):
            return {'status': 'not_attempted', 'reason': 'identity_unmapped'}
        if identity_map.get(identity_key) != owner[0]:
            return {'status': 'not_attempted', 'reason': 'identity_unmapped'}

        try:
            template_fields = build_review_template_fields(
                batch_id=batch,
                review_text=context['text'],
                items=context['items'],
                field_mapping=config.field_mapping,
            )
        except ValueError:
            return {'status': 'not_attempted', 'reason': 'template_mapping_invalid'}

        try:
            open_space_id = 'dtv1.card//IM_ROBOT.' + context['owner_user']
            now = datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(sep=' ', timespec='seconds')
            claimed, receipt_status = claim_review_card_delivery(
                self.conn, batch, config.template_id, config.template_version, now,
            )
        except (ValueError, OSError):
            return {'status': 'not_attempted', 'reason': 'text_anchor_unverified'}
        if not claimed:
            status = 'delivered' if receipt_status == 'delivered' else (
                'not_delivered' if receipt_status == 'not_delivered' else 'delivery_unknown'
            )
            return {'status': status, 'reason': 'already_attempted'}

        client = DwsClient(confirmed_official_writes=True)
        try:
            response = client.create_and_deliver_review_card(
                batch_id=batch,
                template_fields=template_fields,
                open_space_id=open_space_id,
                credential_source=CifraCardCredentialSource(
                    'DINGTODO_CIFRA_CARD_CLIENT_ID',
                    'DINGTODO_CIFRA_CARD_CLIENT_SECRET',
                ),
                authorization_enabled=True,
                robot_code=context['robot_code'],
                expected_robot_code=context['robot_code'],
                published_template_id=config.template_id,
                published_template_version=config.template_version,
            )
            delivered = (isinstance(response, dict) and response.get('delivered') is True
                         and response.get('out_track_id') == batch)
            if not delivered:
                finish_review_card_delivery(
                    self.conn, batch, 'not_delivered', now,
                    'delivery_response_unverified',
                )
                return {'status': 'not_delivered',
                        'reason': 'delivery_response_unverified'}
            finish_review_card_delivery(self.conn, batch, 'delivered', now)
            return {'status': 'delivered'}
        except Exception:
            try:
                finish_review_card_delivery(
                    self.conn, batch, 'unknown', now, 'delivery_exception',
                )
            except Exception:
                pass
            return {'status': 'delivery_unknown', 'reason': 'delivery_exception'}
