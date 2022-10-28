from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from parsl.channels.local.local import LocalChannel
    from parsl.channels.ssh.ssh import SSHChannel

lazys = {
        'SSHChannel': 'parsl.channels.ssh.ssh',
        'LocalChannel': 'parsl.channels.local.local',
        'SSHInteractiveLoginChannel': 'parsl.channels.ssh_il.ssh_il',
        'OAuthSSHChannel': 'parsl.channels.oauth_ssh.oauth_ssh',
}

import parsl.channels as px


def lazy_loader(name):
    if name in lazys:
        import importlib
        m = lazys[name]
        print(f"lazy load {name} from module {m}")
        v = importlib.import_module(m)
        print(f"imported module: {v}")
        a = v.__getattribute__(name)
        px.__setattr__(name, a)
        return a
    raise AttributeError(f"No (lazy loadable) attribute in {__name__} for {name}")


px.__getattr__ = lazy_loader  # type: ignore

__all__ = ['SSHChannel', 'LocalChannel', 'SSHInteractiveLoginChannel', 'OAuthSSHChannel']
