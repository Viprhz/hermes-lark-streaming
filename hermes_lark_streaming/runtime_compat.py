"""Hermes 0.21.3 runtime bridge. No core-file writes or native text streaming.

The stream observer hooks are lossy, asynchronous and observer-only. They cannot
acknowledge final delivery. This bridge uses the existing consumer confirmation
contract, with a small, reversible runtime binding at TurnRunner's factory.
"""
from __future__ import annotations

import asyncio
import contextvars
import functools
import hashlib
import importlib
import importlib.util
import inspect
import json
import logging
import os
import queue
import threading
import time
import uuid
from pathlib import Path
from typing import Any

from .config import Config, hermes_home
from .controller import StreamCardController
from .streaming.segments import SegmentType
from .streaming.session import SessionState

LOG = logging.getLogger("hermes_lark_streaming.compat")
_LOCK = threading.RLock()
_RUNTIME = None
_BG = contextvars.ContextVar("lark_compat_background", default=None)


class CompatibilityError(RuntimeError):
    pass


def hermes_install_root() -> Path:
    """Locate the Hermes source tree WITHOUT importing any pinned module.

    ``find_spec("plugins.platforms.feishu.adapter")`` executes the package
    ``__init__`` chain, and feishu's re-exports ``adapter``, which imports
    top-level Hermes modules (``pm``). Under a stale editable install those are
    unmapped, so a read-only hash check would die with ModuleNotFoundError.
    ``gateway``'s ``__init__`` pulls in no core module, so resolve that and
    derive the sibling paths from the tree instead.
    """
    spec = importlib.util.find_spec("gateway")
    origin = getattr(spec, "origin", None)
    if not origin:
        raise CompatibilityError("Cannot locate the Hermes install root")
    return Path(origin).resolve().parent.parent


def compatibility() -> dict:
    """Read-only, secret-free check. Pin implementation files, not version strings."""
    manifest = json.loads(Path(__file__).with_name("compat_contract.json").read_text())
    root = hermes_install_root()
    checks = {}
    for rel, digest in manifest["modules"].items():
        path = root / rel
        checks[rel] = bool(path.is_file()
                           and hashlib.sha256(path.read_bytes()).hexdigest() == digest)
    return {"compatible": all(checks.values()), "checks": checks,
            "baseline": manifest["baseline"], "mode": "runtime-consumer", "core_writes": False}


def status() -> dict:
    result = compatibility()
    result.update(installed=_RUNTIME is not None, native_feishu_streaming=False)
    if _RUNTIME:
        result["bindings_intact"] = all(getattr(owner, key) is replacement
                                        for owner, key, _, replacement in _RUNTIME.bindings)
        result["active_consumers"] = len(_RUNTIME.active)
    return result


class CardConsumer:
    """Sync callbacks -> gateway loop. The loop alone mutates CardSession.

    Confirmation flags become true ONLY after the controller's final update
    succeeds; merely creating a card never suppresses Hermes' text fallback.
    """
    accepts_tool_progress = False
    _turn_split_delivery = True  # never let Hermes edit a CardKit id as text
    message_id = None
    adapter = None

    def __init__(self, runtime, ctx):
        self.runtime, self.ctx = runtime, ctx
        self.key = str(ctx.inbound_message_id or ctx.event_message_id or "")
        self.token = uuid.uuid4().hex
        self.runtime.pending[self.token] = self
        self.events = queue.Queue()
        self.closed = False
        self.final_response_sent = False
        self.final_content_delivered = False
        self.final_text = None
        self.started_at = time.monotonic()
        self._finish_enqueued = False
        self._lock = threading.Lock()

    def _put(self, kind, value=None):
        if not self.closed:
            self.events.put((kind, value))

    def on_delta(self, text):
        # User selected final-only answers (A). Never expose raw pre-sanitize
        # tokens, nor seal provisional answer text into previous tool cards.
        pass

    def on_commentary(self, text):
        # Scratch commentary is not the authoritative final answer.
        self._put("commentary", text)

    def on_segment_break(self):
        pass

    def finish(self, final_text=None):
        # Hermes calls this BEFORE media augmentation and result normalization.
        # run_sync's wrapper publishes the actual returned result instead.
        pass

    def finish_result(self, result):
        with self._lock:
            if self._finish_enqueued:
                return
            self._finish_enqueued = True
            self._put("finish", dict(result or {}))

    def abort(self):
        self._put("abort")

    def delivered_final_matches(self, text):
        return self.final_response_sent and text == self.final_text

    def has_delivered_text(self, text):
        return self.delivered_final_matches(text)

    async def _abort(self):
        ctrl = self.runtime.controller
        session = ctrl._sessions.get(self.key)
        if session:
            # Do not abort by session_key: a queued successor can already own it.
            session.state = SessionState.ABORTED
            session.flush.mark_completed()
            await ctrl._complete_session_after_creation(session)
        self.runtime.release(self)

    async def _complete(self, result):
        ctrl = self.runtime.controller
        if result.get("interrupted") or result.get("failed") or not self.ctx._run_still_current():
            await self._abort()
            return
        answer = result.get("final_response") or ""
        from gateway.response_filters import is_intentional_silence_response
        if not answer or answer == "(empty)" or is_intentional_silence_response(answer):
            await self._abort()
            return
        session = ctrl._sessions.get(self.key)
        if not session or not await ctrl._wait_for_card_creation(session):
            return
        # Quiesce pending flushes before final authoritative reconciliation.
        await session.flush.wait_for_flush()
        session.flush.mark_completed()
        segments = session.segment_state
        if segments:
            # Preserve tool/reasoning history, replace only the active answer. A
            # stale token accumulator must not be acknowledged as the final.
            active = segments.segments[session.split_index:]
            active[:] = [s for s in active if s.type != SegmentType.ANSWER]
            segments.segments[session.split_index:] = active
            segments._new_answer(answer)
        sent = await ctrl.on_completed_wait(
            message_id=self.key, answer=answer, duration=time.monotonic()-self.started_at,
            model=result.get("model") or "", tokens=result,
            context={"used_tokens": result.get("last_prompt_tokens", 0),
                     "max_tokens": result.get("context_length", 0)},
        )
        if sent:
            self.final_text = answer
            self.final_response_sent = self.final_content_delivered = True
        else:
            ctrl.consume_text_fallback(self.key)

    async def run(self):
        ctrl = self.runtime.controller
        self.runtime.active.add(self)
        try:
            ctrl.on_message_started(message_id=self.key, chat_id=self.ctx.source.chat_id,
                                    anchor_id=self.ctx.event_message_id, session_key=self.ctx.session_key)
            while True:
                if not self.ctx._run_still_current():
                    await self._abort()
                    return
                try:
                    kind, value = self.events.get_nowait()
                except queue.Empty:
                    await asyncio.sleep(0.01)
                    continue
                if kind == "abort":
                    await self._abort()
                    return
                if kind == "finish":
                    # The factory task must exit so Hermes can shape/sanitize
                    # its result. Finalize only at the actual delivery boundary.
                    if value.get("interrupted") or value.get("failed"):
                        await self._abort()
                    return
                if kind == "delta":
                    ctrl.on_answer(message_id=self.key, text=value)
                elif kind == "tool":
                    ctrl.on_tool_update(message_id=self.key, **value)
                # Interim commentary deliberately stays out of final answer text.
        except asyncio.CancelledError:
            # A timed-out stream is NOT a confirmed final delivery.
            await self._abort()
            raise
        except Exception:
            LOG.exception("Card consumer failed; final delivery remains unconfirmed")
            await self._abort()
        finally:
            self.closed = True
            # Pending is retained for the post-sanitization delivery boundary.
            ctrl.consume_text_fallback(self.key)


class Runtime:
    def __init__(self, home, controller=None):
        self.home = Path(home).resolve()
        self.controller = controller or StreamCardController(self.home)
        self.bindings = []
        self.active = set()
        self.pending = {}

    def eligible(self, source=None, adapter=None, config=None):
        if hermes_home().resolve() != self.home:
            return False
        if source is not None:
            if getattr(getattr(source, "platform", None), "value", None) != "feishu":
                return False
            if getattr(source, "profile", None) not in (None, "", "default"):
                return False
        if adapter is not None and getattr(adapter, "_owner_profile", None) not in (None, "", "default"):
            return False
        cfg = config if config is not None else Config(self.home)._load()
        platform = ((cfg.get("display") or {}).get("platforms") or {}).get("feishu") or {}
        return ((cfg.get("streaming") or {}).get("enabled") is True
                and platform.get("streaming") is False and self.controller.enabled)

    @staticmethod
    def redact(text):
        from gateway.run import _redact_gateway_user_facing_secrets
        return _redact_gateway_user_facing_secrets(str(text))

    def release(self, sc):
        self.pending.pop(sc.token, None)
        self.active.discard(sc)

    async def complete_at_delivery(self, sc, result, text):
        try:
            if not sc.final_response_sent:
                await sc._complete({**result, "final_response": text})
        finally:
            self.release(sc)

    def bind(self, owner, name, factory):
        original = getattr(owner, name)
        replacement = functools.wraps(original)(factory(original))
        setattr(owner, name, replacement)
        self.bindings.append((owner, name, original, replacement))

    def uninstall(self):
        if self.active:
            raise RuntimeError("Cannot uninstall while card consumers are active")
        for owner, name, original, replacement in reversed(self.bindings):
            if getattr(owner, name) is not replacement:
                raise RuntimeError(f"Runtime binding changed externally: {name}")
            setattr(owner, name, original)
        self.bindings.clear()

    def install(self):
        from gateway.run_turn_runner import TurnRunner
        from gateway.run_turn import GatewayTurnMixin
        from gateway.platform_registry import platform_registry
        entry = platform_registry.get("feishu")
        if entry is None or not inspect.isclass(entry.adapter_factory):
            raise CompatibilityError("Feishu platform factory is not registered")
        # The production loader uses hermes_plugins.feishu_platform.adapter,
        # not the source-tree import namespace (those are different classes).
        FeishuAdapter = entry.adapter_factory
        expected = json.loads(Path(__file__).with_name("compat_contract.json").read_text())["modules"]["plugins/platforms/feishu/adapter.py"]
        if hashlib.sha256(Path(inspect.getfile(FeishuAdapter)).read_bytes()).hexdigest() != expected:
            raise CompatibilityError("Registered Feishu adapter differs from tested contract")

        def setup(original):
            def wrapped(tr, platform_key):
                ctx = tr._ctx
                if not self.eligible(ctx.source, config=ctx.user_config) or not (ctx.inbound_message_id or ctx.event_message_id):
                    return original(tr, platform_key)
                consumer = CardConsumer(self, ctx)
                ctx.stream_consumer_holder[0] = consumer
                # Independent CardKit delta sink; never instantiate the native
                # text consumer, even if interim_assistant_messages is enabled.
                stts = ctx.streaming_tts_consumer_holder[0]
                def delta(text):
                    if ctx._run_still_current():
                        consumer.on_delta(text)
                        if stts is not None:
                            stts.on_delta(text)
                def interim(text, *, already_streamed=False):
                    if not already_streamed:
                        consumer.on_commentary(text)
                return consumer, delta, interim, True
            return wrapped

        def run_sync(original):
            def wrapped(tr):
                try:
                    result = original(tr)
                except BaseException:
                    sc = tr._ctx.stream_consumer_holder[0]
                    if isinstance(sc, CardConsumer):
                        sc.abort()
                    raise
                sc = tr._ctx.stream_consumer_holder[0]
                if isinstance(sc, CardConsumer):
                    if isinstance(result, dict):
                        result["_lark_card_token"] = sc.token
                    sc.finish_result(result)
                return result
            return wrapped

        def progress(original):
            def wrapped(tr, event_type, tool_name=None, preview=None, args=None, **kwargs):
                sc = tr._ctx.stream_consumer_holder[0]
                if isinstance(sc, CardConsumer) and event_type in {"tool.started", "tool.completed"}:
                    if tr._ctx._run_still_current() and not tr._agent_interrupted():
                        sc._put("tool", {"tool_name": tool_name or "tool", "status": event_type.split(".")[-1],
                                         "detail": self.redact(preview or "")})
                    return
                return original(tr, event_type, tool_name, preview, args, **kwargs)
            return wrapped

        def background(original):
            async def wrapped(runner, prompt, source, task_id, event_message_id=None, media_urls=None, media_types=None):
                if not self.eligible(source):
                    return await original(runner, prompt, source, task_id, event_message_id, media_urls, media_types)
                token = _BG.set({"preview": prompt[:60], "anchor": event_message_id, "chat_id": source.chat_id})
                try:
                    return await original(runner, prompt, source, task_id, event_message_id, media_urls, media_types)
                finally:
                    _BG.reset(token)
            return wrapped

        def send(original):
            async def wrapped(adapter, chat_id, content, reply_to=None, metadata=None):
                bg = _BG.get()
                meta = metadata or {}
                is_cron = bool(meta.get("job_id"))
                # A thread-only route cannot safely be translated to a reply
                # anchor by this client. Preserve native routing instead.
                if (not self.eligible(adapter=adapter) or not content
                    or (not is_cron and not (bg and bg["chat_id"] == chat_id))
                    or (meta.get("thread_id") and not reply_to and not (bg and bg["anchor"]))):
                    return await original(adapter, chat_id, content, reply_to, metadata)
                from .cardkit.builder import build_cron_card, build_background_card
                from gateway.platforms.base import SendResult
                ctrl = self.controller
                try:
                    await ctrl._ensure_init()
                    card = (build_cron_card(content, task_name=str(meta["job_id"])) if is_cron
                            else build_background_card(bg["preview"], content))
                    mid = await ctrl._client.send_card_to_chat(
                        chat_id, card, reply_to_message_id=reply_to or (bg["anchor"] if bg else None))
                    if not mid:
                        raise RuntimeError("Card transport returned no message id")
                    return SendResult(success=True, message_id=mid)
                except Exception:
                    LOG.exception("Static card failed; invoking original text delivery once")
                    return await original(adapter, chat_id, content, reply_to, metadata)
            return wrapped

        def mark_delivery(original):
            async def wrapped(runner, response, turn_ctx):
                sc = turn_ctx.stream_consumer_holder[0]
                if isinstance(sc, CardConsumer) and isinstance(response, dict):
                    response["_lark_card_token"] = sc.token
                    # No confirmation before outer response shaping/redaction.
                    return
                return await original(runner, response, turn_ctx)
            return wrapped

        def deliver_response(original):
            async def wrapped(runner, event, source, session_entry, session_key, run_generation,
                              agent_result, agent_messages, response, _footer_line, _intentional_silence):
                sc = self.pending.get(agent_result.get("_lark_card_token"))
                if sc:
                    if _intentional_silence:
                        await sc._abort()
                        self.release(sc)
                    else:
                        # This is the real post-shaping payload, not run_sync's
                        # raw answer. Redact again after reasoning/footer joins.
                        from gateway.run import _sanitize_gateway_final_response
                        response = _sanitize_gateway_final_response(source.platform, response)
                        await self.complete_at_delivery(sc, agent_result, response)
                    if sc.final_response_sent:
                        agent_result["already_sent"] = True
                        agent_result["_lark_card_confirmed"] = True
                        _footer_line = ""
                return await original(runner, event, source, session_entry, session_key, run_generation,
                                      agent_result, agent_messages, response, _footer_line, _intentional_silence)
            return wrapped

        def queued_delivery(original):
            async def wrapped(runner, turn_ctx, adapter, response, result, stream_task):
                sc = turn_ctx.stream_consumer_holder[0]
                if isinstance(sc, CardConsumer):
                    from gateway.run import (_sanitize_gateway_final_response,
                        _normalize_empty_agent_response, _is_gateway_hidden_reasoning_incomplete_turn)
                    text = result.get("final_response") or ""
                    if _is_gateway_hidden_reasoning_incomplete_turn(result):
                        text = ""
                    text = _normalize_empty_agent_response(result, text, history_len=0)
                    text = _sanitize_gateway_final_response(turn_ctx.source.platform, text)
                    result["final_response"] = text
                    if isinstance(response, dict):
                        response["final_response"] = text
                    await self.complete_at_delivery(sc, result, text)
                return await original(runner, turn_ctx, adapter, response, result, stream_task)
            return wrapped

        # 0.22.2 moved two seams onto mixins in their own modules. Bind the
        # DEFINING class, not the subclass: binding a name that the subclass
        # only inherits would shadow it with a new attribute, and uninstall's
        # identity check would then restore nothing.
        from gateway.run_turn_progress import GatewayTurnProgressMixin
        from gateway.run_turn_persistence import GatewayTurnPersistenceMixin

        self.bind(TurnRunner, "_setup_stream_consumer", setup)
        self.bind(TurnRunner, "run_sync", run_sync)
        self.bind(GatewayTurnProgressMixin, "progress_callback", progress)
        self.bind(GatewayTurnMixin, "_run_agent_mark_streamed_delivery", mark_delivery)
        self.bind(GatewayTurnPersistenceMixin, "_hmwa_deliver_turn_response", deliver_response)
        self.bind(GatewayTurnMixin, "_run_agent_deliver_first_response", queued_delivery)
        self.bind(GatewayTurnMixin, "_run_background_task_inner", background)
        self.bind(FeishuAdapter, "send", send)


def install(*, home=None, controller=None):
    """Explicit startup entry point. Call before gateway startup, not AST install.

    home is the default profile root (a temp default root in tests). Repeated
    calls are no-ops. Drift rejects BEFORE any runtime binding is changed.
    """
    global _RUNTIME
    with _LOCK:
        home = Path(home or hermes_home()).resolve()
        if _RUNTIME:
            if _RUNTIME.home != home:
                raise CompatibilityError("Runtime already bound to another profile")
            return status()
        if "profiles" in home.parts:
            raise CompatibilityError("Compatibility bridge is default-profile only")
        report = compatibility()
        if not report["compatible"]:
            raise CompatibilityError(f"Hermes runtime contract drift: {report['checks']}")
        runtime = Runtime(home, controller)
        try:
            runtime.install()
        except BaseException:
            runtime.uninstall()
            raise
        _RUNTIME = runtime
        report = status()
        report.update(pid=os.getpid(), installed_at=time.time())
        receipt = home / "cache" / "lark-runtime" / f"{os.getpid()}.json"
        receipt.parent.mkdir(parents=True, exist_ok=True)
        receipt.write_text(json.dumps(report, ensure_ascii=False, indent=2))
        LOG.info("Installed Hermes CardKit runtime bridge (no core writes), baseline=%s pid=%s",
                 report.get("baseline"), os.getpid())
        return report


def stop_session(*, session_key, platform=None, **kwargs):
    """Native /stop observer: thread-safe, generation-local queue cancellation."""
    if _RUNTIME is None or platform not in ("feishu", None):
        return
    if hermes_home().resolve() != _RUNTIME.home:
        return
    for consumer in tuple(_RUNTIME.active):
        if consumer.ctx.session_key == session_key:
            consumer.abort()


def uninstall():
    global _RUNTIME
    with _LOCK:
        if _RUNTIME:
            _RUNTIME.uninstall()
            _RUNTIME = None
