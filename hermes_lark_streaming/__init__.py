import logging
import threading
import time

__version__ = "0.12.0"

_DEFERRED_LOCK = threading.Lock()
_DEFERRED_DONE = False


def install_deferred(*, home=None, controller=None):
    """Bind after the plugin-discovery sweep has released its lock.

    ``register()`` MUST NOT reach ``platform_registry.get("feishu")``: feishu is
    a deferred-load platform whose loader re-acquires PluginManager's
    ``_discovery_lock``. During discovery that lock is already held by the sweep
    calling us, on a different thread, so the loader blocks forever and the
    10s plugin-load deadline kills the bridge (silently -- the abandoned thread
    logs nothing). Resolving it once discovery is done is safe and cheap.

    Idempotent and never fatal: a failure here must leave Hermes' native text
    delivery untouched rather than take the gateway down.
    """
    global _DEFERRED_DONE
    with _DEFERRED_LOCK:
        if _DEFERRED_DONE:
            return None
        try:
            from .runtime_compat import install
            report = install(home=home, controller=controller)
        except Exception:
            logging.getLogger("hermes_lark_streaming").warning(
                "Deferred CardKit bridge install failed; native text delivery stays active",
                exc_info=True)
            _DEFERRED_DONE = True
            return None
        _DEFERRED_DONE = True
        return report


def _schedule_deferred_install():
    """Run install_deferred() off the loader thread, once discovery has finished.

    A plain thread (not the loader's) means we never inherit its deadline, and
    the retry loop rides out the sweep still holding the lock while sibling
    plugins load.
    """
    def _run():
        from .runtime_compat import CompatibilityError
        deadline = time.monotonic() + 120.0
        while time.monotonic() < deadline:
            try:
                from gateway.platform_registry import platform_registry
                if platform_registry.get("feishu") is not None:
                    install_deferred()
                    return
            except CompatibilityError:
                return  # contract drift: refuse permanently, keep text fallback
            except Exception:
                pass
            time.sleep(1.0)
        logging.getLogger("hermes_lark_streaming").warning(
            "Feishu platform never registered within 120s; CardKit bridge not installed")

    threading.Thread(target=_run, name="lark-bridge-deferred-install", daemon=True).start()


def register(ctx):
    """Native entry-point discovery; explicit startup install is also supported."""
    from .runtime_compat import stop_session
    from .config import hermes_home
    if "profiles" in hermes_home().resolve().parts:
        return
    # Binding is deferred on purpose -- see install_deferred().
    _schedule_deferred_install()
    ctx.register_hook("agent_loop_stopped", stop_session)

