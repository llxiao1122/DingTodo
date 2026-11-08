import tempfile
from pathlib import Path

import pytest

from review_inbox import (
    claim_review_card_delivery,
    connect,
    finish_review_card_delivery,
    prepare_dispatch,
    register_batch,
)


BATCH_ID = "card_batch_1234"
ITEMS = [{
    "action_id": "701",
    "source_date": "2026-11-07",
    "source": {"text": "synthetic source", "text_truncated": False},
}]


@pytest.fixture
def review_db():
    with tempfile.TemporaryDirectory() as temporary_directory:
        connection = connect(str(Path(temporary_directory) / "review.sqlite3"))
        prepare_dispatch(connection, BATCH_ID, "owner-conversation", "owner", ITEMS)
        connection.execute(
            "UPDATE dispatch SET status='delivered',prompt_message_id=?,prompt_time=? WHERE id=?",
            ("text-prompt", "2026-11-07 10:00:00", BATCH_ID),
        )
        register_batch(
            connection, BATCH_ID, "owner-conversation", "owner", "text-prompt",
            "2026-11-07 10:00:00", {1: ["701"]}, enable_automation=True,
        )
        yield connection
        connection.close()


def test_card_receipt_claim_is_committed_before_the_single_attempt(review_db):
    claimed, status = claim_review_card_delivery(
        review_db, BATCH_ID, "template-id", "version-3", "2026-11-07 10:01:00",
    )

    assert claimed is True
    assert status == "attempting"
    assert review_db.execute(
        "SELECT status FROM review_card_delivery WHERE batch_id=?", (BATCH_ID,),
    ).fetchone() == ("attempting",)

    finish_review_card_delivery(
        review_db, BATCH_ID, "unknown", "2026-11-07 10:01:05", "delivery_exception",
    )
    assert claim_review_card_delivery(
        review_db, BATCH_ID, "template-id", "version-3", "2026-11-07 10:02:00",
    ) == (False, "unknown")
    assert review_db.execute(
        "SELECT status,error_code FROM review_card_delivery WHERE batch_id=?", (BATCH_ID,),
    ).fetchone() == ("unknown", "delivery_exception")


def test_card_receipt_requires_a_proven_text_batch(review_db):
    review_db.execute("UPDATE dispatch SET status='unknown' WHERE id=?", (BATCH_ID,))
    with pytest.raises(ValueError, match="text_anchor_unverified"):
        claim_review_card_delivery(
            review_db, BATCH_ID, "template-id", "version-3", "2026-11-07 10:01:00",
        )
    assert review_db.execute("SELECT COUNT(*) FROM review_card_delivery").fetchone()[0] == 0


def test_card_receipt_claim_is_idempotent_after_verified_delivery(review_db):
    assert claim_review_card_delivery(
        review_db, BATCH_ID, "template-id", "version-3", "2026-11-07 10:01:00",
    ) == (True, "attempting")
    finish_review_card_delivery(review_db, BATCH_ID, "delivered", "2026-11-07 10:01:05")

    assert claim_review_card_delivery(
        review_db, BATCH_ID, "template-id", "version-3", "2026-11-07 10:02:00",
    ) == (False, "delivered")
    assert review_db.execute("SELECT COUNT(*) FROM review_card_delivery").fetchone()[0] == 1
