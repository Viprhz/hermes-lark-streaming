"""Read-only bridge health; legacy AST mutation commands are intentionally blocked."""
import json,socket,sys
from .runtime_compat import compatibility,hermes_install_root
from .config import hermes_home

def _ensure_hermes_importable():
    """Put the Hermes source tree on sys.path, like the official launcher does.

    hermes_cli imports top-level modules (hermes_yaml, pm). An editable install
    whose finder MAPPING predates them cannot resolve those, so importing
    hermes_cli from outside the tree dies with ModuleNotFoundError.
    """
    root=str(hermes_install_root())
    if root not in sys.path:
        sys.path.insert(0,root)

def check_status():
    report=compatibility()
    _ensure_hermes_importable()
    from hermes_cli.config_effective import load_user_config_effective
    cfg=load_user_config_effective()
    report['plugin_enabled']='hermes-lark-streaming' in (cfg.get('plugins',{}).get('enabled') or [])
    report['native_feishu_streaming']=cfg.get('display',{}).get('platforms',{}).get('feishu',{}).get('streaming')
    report['gateway_loaded']=False
    try:
        with socket.socket(socket.AF_UNIX,socket.SOCK_STREAM) as s:
            s.settimeout(3);s.connect(str(hermes_home()/'gateway.sock'));s.sendall(b'{"verb":"status"}\n')
            raw=b''
            while not raw.endswith(b'\n'):
                part=s.recv(65536)
                if not part:break
                raw+=part
        gateway=json.loads(raw)['result'];pid=gateway['answering_pid']
        receipt=json.loads((hermes_home()/'cache/lark-runtime'/f'{pid}.json').read_text())
        report.update(gateway_pid=pid,gateway_loaded=receipt.get('installed') is True and receipt.get('bindings_intact') is True,
                      gateway_installed_at=receipt.get('installed_at'))
    except (OSError,ValueError,KeyError) as exc:
        report['gateway_check']=str(exc)
    print(json.dumps(report,ensure_ascii=False,indent=2))
    return 0 if report['compatible'] and report['plugin_enabled'] and report['gateway_loaded'] else 1

def verify():
    report=compatibility();print(json.dumps(report,ensure_ascii=False,indent=2))
    return 0 if report['compatible'] else 1

def refuse_legacy():
    print('Runtime bridge mode: AST install/restore disabled. Enable/disable via hermes plugins enable/disable hermes-lark-streaming, then safely restart the gateway. No source files changed.')
    return 1
