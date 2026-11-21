import argparse
import json
from pathlib import Path

import pytest

from seven_tv_events import (
    ChangeMap,
    CloseCode,
    Dispatch,
    EndOfStream,
    Error,
    Hello,
    Message,
    ObjectKind,
    Opcode,
    ReconnectPolicy,
    Subscription,
    User,
)
from seven_tv_events._cli import parse_subscription


def test_message_envelope() -> None:
    msg = Message.from_json({"op": 1, "t": 1_700_000_000_000, "d": {"x": 1}})
    assert msg.op is Opcode.HELLO
    assert msg.t is not None and msg.t.year == 2023
    assert msg.d == {"x": 1}


def test_unknown_opcode_is_kept_as_int() -> None:
    assert Message.from_json({"op": 99, "d": None}).op == 99


def test_hello() -> None:
    hello = Hello.from_json({"heartbeat_interval": 45000, "session_id": "abc", "subscription_limit": 500})
    assert (hello.heartbeat_interval, hello.session_id, hello.subscription_limit) == (45000, "abc", 500)


def test_dispatch_doc_example() -> None:
    # The "Result Dispatch (Op 0)" example from the docs.
    d = Dispatch.from_json({
        "type": "emote_set.update",
        "body": {
            "id": "01HKFR1QNG000DQX3MV0V8XFFP",
            "updated": [{"key": "name", "old_value": "Old Name", "value": "New Name"}],
        },
    })
    assert d.type == "emote_set.update"
    assert d.body.id == "01HKFR1QNG000DQX3MV0V8XFFP"
    [(kind, change)] = list(d.body.changes())
    assert kind == "updated"
    assert (change.key, change.old_value, change.value) == ("name", "Old Name", "New Name")


def test_change_map_nested_fields_and_kind() -> None:
    body = ChangeMap.from_json({
        "id": "x",
        "kind": 3,
        "pushed": [{"key": "emotes", "index": 4, "value": {"id": "e1", "name": "Pog"}}],
        "updated": [
            {"key": "owner", "nested": True, "value": [{"key": "display_name", "old_value": "a", "value": "b"}]}
        ],
        "actor": {"id": "u1"},
    })
    assert body.kind is ObjectKind.EMOTE_SET
    assert body.pushed[0].index == 4
    nested = body.updated[0]
    assert nested.nested
    assert nested.value[0].key == "display_name"
    assert body.actor is not None and body.actor.id == "u1"
    assert ChangeMap.from_json({}).actor is None
    assert ChangeMap.from_json({"kind": 1234}).kind == 1234


def test_end_of_stream() -> None:
    eos = EndOfStream.from_json({"code": 4006, "message": "restarting"})
    assert eos.code is CloseCode.RESTART
    assert eos.code.reconnect is ReconnectPolicy.YES


def test_close_code_policies_cover_every_code() -> None:
    for code in CloseCode:
        assert isinstance(code.reconnect, ReconnectPolicy)
    assert CloseCode.ALREADY_SUBSCRIBED.reconnect is ReconnectPolicy.NO
    assert CloseCode.MAINTENANCE.reconnect is ReconnectPolicy.LATER


def test_subscription_equality_ignores_condition_order() -> None:
    a = Subscription("cosmetic.*", host_id="h", connection_id="1")
    b = Subscription("cosmetic.*", {"connection_id": "1"}, host_id="h")
    assert a == b and hash(a) == hash(b)
    assert a != Subscription("cosmetic.*", host_id="h")
    assert a.to_json() == {"type": "cosmetic.*", "condition": {"connection_id": "1", "host_id": "h"}}


def test_cli_parse_subscription() -> None:
    assert parse_subscription("emote_set.update:object_id=abc") == Subscription("emote_set.update", object_id="abc")
    assert parse_subscription("system.announcement") == Subscription("system.announcement")
    assert parse_subscription("cosmetic.*:host_id=h, connection_id=1") == Subscription(
        "cosmetic.*", host_id="h", connection_id="1"
    )
    with pytest.raises(argparse.ArgumentTypeError):
        parse_subscription("emote.update:oops")


def test_live_emote_set_events() -> None:
    """Real dispatches captured from the live EventAPI while editing an emote set."""
    path = Path(__file__).parent / "data" / "live_emote_set_events.jsonl"
    events = [Dispatch.from_json(json.loads(line)) for line in path.read_text(encoding="utf-8").splitlines()]
    summary = [
        (
            kind,
            f.key,
            f.index,
            (f.old_value or {}).get("name") if f.key == "emotes" else f.old_value,
            (f.value or {}).get("name") if f.key == "emotes" else f.value,
        )
        for e in events
        for kind, f in e.body.changes()
    ]
    assert summary == [
        ("pushed", "emotes", 921, None, "apuBand"),
        ("pulled", "emotes", 171, "veiThighs", None),
        ("pulled", "emotes", 285, "peepoSpace", None),
        ("updated", "emotes", 291, "bocchiDefeated", "bocchiDx"),
        ("pulled", "emotes", 291, "bocchiDx", None),
        ("updated", "name", None, "Default", "Default Emote Set"),
    ]
    for e in events:
        assert e.type == "emote_set.update"
        assert e.body.kind is ObjectKind.EMOTE_SET
        assert e.body.actor is not None and e.body.actor.username == "irenesbot"
        assert all(f.type == "object" for _, f in e.body.changes() if f.key == "emotes")


def test_user_from_live_event() -> None:
    path = Path(__file__).parent / "data" / "live_emote_set_events.jsonl"
    event = Dispatch.from_json(json.loads(path.read_text(encoding="utf-8").splitlines()[0]))
    pushed_emote_owner = User.from_json(event.body.pushed[0].value["data"]["owner"])

    actor = event.body.actor
    assert actor is not None
    assert (actor.id, actor.username, actor.display_name) == ("01KFF67D46PJPD1S6DPFFT06E3", "irenesbot", "IrenesBot")
    assert actor.role_ids == ["01G68MMQFR0007J6GNM9E2M0TM"]
    assert actor.style.paint_id is None and actor.style.color is None
    twitch = actor.connection("twitch")
    assert twitch is not None
    assert (twitch.id, twitch.emote_set_id, twitch.emote_capacity) == ("1277023540", "01FAQVCS500002EV4FV330P46A", 1000)
    assert twitch.linked_at is not None and twitch.linked_at.year == 2026

    assert pushed_emote_owner.username == "laden"
    assert pushed_emote_owner.style.paint_id == "01JWHCGA1E7K0ZZGETCV3A6KF4"
    assert pushed_emote_owner.style.color == -5635841
    assert [c.platform for c in pushed_emote_owner.connections] == ["TWITCH", "YOUTUBE"]
    assert pushed_emote_owner.connection("discord") is None


def test_error_fields() -> None:
    assert Error.from_json({"message": "bad", "fields": {"type": "x"}}).fields == {"type": "x"}
    assert Error.from_json({"message": "bad"}).fields == {}
