import json
from collections.abc import AsyncIterator
from http import HTTPStatus
from pathlib import Path

import aiohttp
import pytest
from aiohttp import web

from seven_tv_events import ChangeMap, Emote, NotFoundError, RESTError
from seven_tv_events.rest import fetch_emote_set, fetch_user_connection

DATA = Path(__file__).parent / "data"
EMOTE_SET_ID = "01FAQVCS500002EV4FV330P46A"
TWITCH_ID = "1277023540"


def load(name: str) -> dict:
    return json.loads((DATA / name).read_text(encoding="utf-8"))


@pytest.fixture
async def base_url() -> AsyncIterator[str]:
    """A fake 7TV REST API serving the recorded responses, with the live server's error bodies."""

    def error(status: int, message: str, code: int) -> web.Response:
        body = {"status": HTTPStatus(status).phrase, "error_code": code, "error": message}
        return web.json_response(body, status=status)

    async def emote_set(request: web.Request) -> web.Response:
        set_id = request.match_info["id"]
        if len(set_id) != 26:
            return error(400, f"invalid id length: {len(set_id)}", 10000)
        if set_id != EMOTE_SET_ID:
            return error(404, "emote set not found", 12000)
        return web.json_response(load("rest_emote_set.json"))

    async def user(request: web.Request) -> web.Response:
        if request.match_info["platform"] != "twitch" or request.match_info["id"] != TWITCH_ID:
            return error(404, "user not found", 12000)
        return web.json_response(load("rest_user_connection.json"))

    async def broken(request: web.Request) -> web.Response:
        return web.Response(status=502, text="<html>Bad Gateway</html>", content_type="text/html")

    app = web.Application()
    app.router.add_get("/v3/emote-sets/{id}", emote_set)
    app.router.add_get("/v3/users/{platform}/{id}", user)
    app.router.add_get("/broken/emote-sets/{id}", broken)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    yield f"http://127.0.0.1:{runner.addresses[0][1]}/v3"
    await runner.cleanup()


async def test_fetch_emote_set(base_url: str) -> None:
    emote_set = await fetch_emote_set(EMOTE_SET_ID, base_url=base_url)
    assert emote_set.id == EMOTE_SET_ID
    assert emote_set.name == "Default Emote Set"
    assert (emote_set.emote_count, emote_set.capacity) == (3, 1000)
    assert emote_set.owner is not None and emote_set.owner.username == "irene_adler__"
    assert [e.name for e in emote_set.emotes] == ["FeelsDonkMan", "dankHug", "DANKPOGGIES"]

    donk = emote_set.emotes[0]
    assert donk.id == "01EZPJ9AFR000C438200A44F30"
    assert not donk.aliased
    assert donk.host_url == "https://cdn.7tv.app/emote/01EZPJ9AFR000C438200A44F30"
    assert donk.added_at is not None and donk.added_at.year == 2021

    aliased = emote_set.get("DANKPOGGIES")
    assert aliased is not None and aliased.aliased and aliased.original_name == "DANKIES"
    assert emote_set.get("DANKIES") is None


async def test_fetch_user_connection(base_url: str) -> None:
    conn = await fetch_user_connection("Twitch", int(TWITCH_ID), base_url=base_url)
    assert (conn.platform, conn.id, conn.username) == ("TWITCH", TWITCH_ID, "irenesbot")
    assert conn.emote_set_id == EMOTE_SET_ID
    assert conn.emote_set is not None and conn.emote_set.id == EMOTE_SET_ID and len(conn.emote_set.emotes) == 3
    assert conn.user is not None and conn.user.id == "01KFF67D46PJPD1S6DPFFT06E3"
    assert conn.user.connection("twitch") is not None


async def test_not_found(base_url: str) -> None:
    with pytest.raises(NotFoundError) as exc:
        await fetch_user_connection("twitch", 1, base_url=base_url)
    assert (exc.value.status, exc.value.error_code, exc.value.message) == (404, 12000, "user not found")

    with pytest.raises(NotFoundError, match="emote set not found"):
        await fetch_emote_set("01FAQVCS500002EV4FV330P46B", base_url=base_url)


async def test_other_errors(base_url: str) -> None:
    with pytest.raises(RESTError) as exc:
        await fetch_emote_set("nope", base_url=base_url)
    assert type(exc.value) is RESTError
    assert (exc.value.status, exc.value.message) == (400, "invalid id length: 4")

    # A non-JSON error page falls back to the HTTP reason phrase.
    broken_url = base_url.removesuffix("/v3") + "/broken"
    with pytest.raises(RESTError) as exc:
        await fetch_emote_set(EMOTE_SET_ID, base_url=broken_url)
    assert (exc.value.status, exc.value.message, exc.value.error_code) == (502, "Bad Gateway", None)


async def test_shared_session_is_left_open(base_url: str) -> None:
    async with aiohttp.ClientSession() as session:
        await fetch_emote_set(EMOTE_SET_ID, session=session, base_url=base_url)
        await fetch_user_connection("twitch", TWITCH_ID, session=session, base_url=base_url)
        assert not session.closed


def test_emote_parses_dispatch_values() -> None:
    # The first recorded live event pushes apuBand into a set.
    line = (DATA / "live_emote_set_events.jsonl").read_text(encoding="utf-8").splitlines()[0]
    body = ChangeMap.from_json(json.loads(line)["body"])
    emote = Emote.from_json(body.pushed[0].value)
    assert (emote.name, emote.original_name, emote.animated) == ("apuBand", "apuBand", True)
    assert emote.owner is not None and emote.owner.username == "laden"
    assert emote.actor_id == "01KFF67D46PJPD1S6DPFFT06E3"
